from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from core.config_loader import StrategyConfig
from core.live.execution_plan import build_dry_run_execution_plan
from core.live.execution_runner import execute_live_execution_plan, load_live_execution_config
from core.live.live_state import load_live_state
from core.live.market_data_hub import load_finalized_candidate_inputs_from_hub
from strategies.snapback.current_ledger import collect_consumer_exchange_activity_snapshot
from strategies.spring.live_execution import build_spring_live_execution_intent
from strategies.spring.logic import SpringSABCStrategy

BJ = timezone(timedelta(hours=8))
DEFAULT_OUTPUT_DIR = "output/live_projection"


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def _now_utc_ms() -> int:
    return int(time.time() * 1000)


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ).strftime("%Y-%m-%d %H:%M:%S")


def _json_default(value: Any) -> Any:
    if hasattr(value, "item"):
        return value.item()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, set):
        return sorted(value)
    if isinstance(value, tuple):
        return list(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _json_safe_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=_json_default)


def _build_run_id(account: str) -> str:
    account_key = str(account).upper().strip()
    if not account_key:
        raise ValueError("account must not be empty")
    ts_utc = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"SPRINGOBS_{account_key}_{ts_utc}"


def _projection_path(output_dir: str, run_id: str) -> Path:
    return Path(output_dir) / f"spring_observer.{run_id}.jsonl"


def _heartbeat_path(output_dir: str, run_id: str) -> Path:
    return Path(output_dir) / f"spring_observer_heartbeat.{run_id}.json"


def _append_projection_row(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(_json_safe_dumps(row) + "\n")
        f.flush()
        os.fsync(f.fileno())


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    tmp_path.write_text(_json_safe_dumps(payload) + "\n", encoding="utf-8")
    os.replace(tmp_path, path)


def _write_heartbeat(
    *,
    output_dir: str,
    run_id: str,
    account: str,
    mode: str,
    iteration: int | None,
    status: str,
    payload: dict[str, Any] | None = None,
) -> None:
    now_utc_ms = _now_utc_ms()
    heartbeat = {
        "schema_version": 1,
        "run_mode": "live_observer",
        "strategy_name": "spring-sabc",
        "account": str(account).strip(),
        "run_id": run_id,
        "mode": mode,
        "iteration": iteration,
        "status": status,
        "updated_utc_ms": now_utc_ms,
        "updated_bj": _fmt_bj_from_ms(now_utc_ms),
    }
    if payload:
        heartbeat.update(payload)
    _atomic_write_json(_heartbeat_path(output_dir, run_id), heartbeat)


def _require_payload_field(payload: Mapping[str, Any], field: str) -> Any:
    if field not in payload:
        raise KeyError(f"hub finalized candidate payload missing required field: {field}")
    value = payload[field]
    if value is None:
        raise ValueError(f"hub finalized candidate payload field is null: {field}")
    return value


def _load_hub_payload(account: str, max_age_secs: int) -> dict[str, Any]:
    payload = load_finalized_candidate_inputs_from_hub(account, max_age_secs=max_age_secs)
    if not isinstance(payload, dict):
        raise TypeError(f"hub finalized candidate payload must be dict, got {type(payload).__name__}")
    return payload


def _validate_hub_payload(payload: Mapping[str, Any]) -> tuple[int, str, int, str, pd.DataFrame, dict[str, Any]]:
    c_bar_ts = int(_require_payload_field(payload, "latest_closed_bar_ts"))
    c_bar_bj = str(_require_payload_field(payload, "latest_closed_bar_bj"))
    signal_time_ts = int(_require_payload_field(payload, "signal_time_ts"))
    signal_time_bj = str(_require_payload_field(payload, "signal_time_bj"))
    symbol_count = int(payload.get("symbol_count") or 0)
    if symbol_count == 0 and ("cross_section" not in payload or "full_df" not in payload):
        cross_section = pd.DataFrame()
        full_df: dict[str, Any] = {}
    else:
        cross_section = _require_payload_field(payload, "cross_section")
        full_df = _require_payload_field(payload, "full_df")

    if not isinstance(cross_section, pd.DataFrame):
        raise TypeError(f"hub cross_section must be DataFrame, got {type(cross_section).__name__}")
    if not isinstance(full_df, dict):
        raise TypeError(f"hub full_df must be dict, got {type(full_df).__name__}")
    if c_bar_ts <= 0:
        raise ValueError(f"latest_closed_bar_ts must be > 0, got {c_bar_ts}")
    if signal_time_ts <= c_bar_ts:
        raise ValueError(f"signal_time_ts must be after latest_closed_bar_ts, got {signal_time_ts} <= {c_bar_ts}")
    return c_bar_ts, c_bar_bj, signal_time_ts, signal_time_bj, cross_section, dict(full_df)


def _active_symbols_from_args(values: list[str] | None) -> set[str]:
    result = {str(value).upper().strip() for value in (values or []) if str(value).strip()}
    return result


def _fail_reason_counts(audits: Mapping[str, Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for rec in audits.values():
        reason = str(rec.get("signal_fail_reason") or rec.get("fail_reason") or "UNKNOWN")
        counts[reason] = counts.get(reason, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _audit_preview(audits: Mapping[str, Mapping[str, Any]], limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for symbol in sorted(audits.keys()):
        rec = audits[symbol]
        rows.append({
            "symbol": str(symbol).upper().strip(),
            "universe_pass": bool(rec.get("universe_pass", False)),
            "structure_pass": bool(rec.get("structure_pass", False)),
            "signal_emit": bool(rec.get("signal_emit", False)),
            "fail_reason": rec.get("signal_fail_reason") or rec.get("fail_reason"),
            "score": rec.get("score"),
            "score_order": rec.get("score_order"),
            "chg_24h": rec.get("chg_24h"),
            "vol_24h": rec.get("vol_24h"),
            "a_time_ms": rec.get("a_time_ms"),
            "b_time_ms": rec.get("b_time_ms"),
            "c_time_ms": rec.get("c_time_ms"),
            "risk_pct": rec.get("risk_pct"),
            "position_notional_usdt": rec.get("position_notional_usdt"),
        })
        if len(rows) >= limit:
            break
    return rows


def run_once(
    *,
    config_path: str,
    account: str,
    output_dir: str,
    hub_max_age_secs: int,
    active_symbols: set[str],
    audit_preview_limit: int,
    run_id: str,
    loop_iteration: int | None = None,
    verify_exchange: bool = False,
    execute_live: bool = False,
    live_execution_config_path: str | None = None,
) -> dict[str, Any]:
    started_utc_ms = _now_utc_ms()
    strategy_cfg = StrategyConfig.load(config_path)
    if str(strategy_cfg.get("strategy_name") or "") != "spring-sabc":
        raise ValueError(f"config strategy_name must be spring-sabc, got {strategy_cfg.get('strategy_name')!r}")

    payload = _load_hub_payload(account, hub_max_age_secs)
    c_bar_ts, c_bar_bj, signal_time_ts, signal_time_bj, cross_section, full_df = _validate_hub_payload(payload)

    strategy = SpringSABCStrategy(strategy_cfg)
    signal = strategy.on_kline_close(
        signal_time_ts,
        cross_section,
        active_symbols,
        full_df=full_df,
    )
    intent = build_spring_live_execution_intent(signal, account=account).to_dict() if signal else None
    local_state_snapshot = load_live_state(account) if intent else None
    exchange_snapshot = collect_consumer_exchange_activity_snapshot(account) if intent and (verify_exchange or execute_live) else None
    dry_run_execution_plan = (
        build_dry_run_execution_plan(
            intent,
            exchange_snapshot=exchange_snapshot,
            local_state_snapshot=local_state_snapshot,
        )
        if intent
        else None
    )
    live_execution_result = None
    if execute_live:
        if not live_execution_config_path or not str(live_execution_config_path).strip():
            raise ValueError("live execution config path is required when execute_live is enabled")
        if intent and dry_run_execution_plan:
            live_execution_config = load_live_execution_config(live_execution_config_path)
            live_execution_result = execute_live_execution_plan(
                intent,
                execution_plan=dry_run_execution_plan,
                execution_config=live_execution_config,
                exchange_snapshot=exchange_snapshot,
                source="spring_live_observer",
            )
    finished_utc_ms = _now_utc_ms()

    audits = dict(getattr(strategy, "_last_signal_audits", {}) or {})
    row = {
        "schema_version": 1,
        "run_mode": "live_observer",
        "projection_type": "spring_observer",
        "strategy_name": "spring-sabc",
        "account": str(account).strip(),
        "run_id": run_id,
        "loop_iteration": loop_iteration,
        "config_path": config_path,
        "hub_max_age_secs": int(hub_max_age_secs),
        "started_utc_ms": started_utc_ms,
        "started_bj": _fmt_bj_from_ms(started_utc_ms),
        "finished_utc_ms": finished_utc_ms,
        "finished_bj": _fmt_bj_from_ms(finished_utc_ms),
        "elapsed_ms": int(finished_utc_ms - started_utc_ms),
        "signal_time_ts": signal_time_ts,
        "signal_time_bj": signal_time_bj,
        "c_bar_ts": c_bar_ts,
        "c_bar_bj": c_bar_bj,
        "hub_reason": payload.get("reason"),
        "hub_symbol_count": int(payload.get("symbol_count") or len(full_df)),
        "cross_section_symbol_count": int(len(cross_section)),
        "full_df_symbol_count": int(len(full_df)),
        "active_symbols": sorted(active_symbols),
        "universe_candidate_count": int(len(getattr(strategy, "_last_universe_candidates", []) or [])),
        "structure_candidate_count": int(len(getattr(strategy, "_last_structure_candidates", []) or [])),
        "audited_symbol_count": int(len(audits)),
        "fail_reason_counts": _fail_reason_counts(audits),
        "audit_preview_limit": int(audit_preview_limit),
        "audit_preview": _audit_preview(audits, audit_preview_limit),
        "signal_present": bool(signal),
        "signal_symbol": str((signal or {}).get("symbol") or "").upper().strip() or None,
        "signal": signal,
        "execution_intent": intent,
        "dry_run_verify_exchange": bool(verify_exchange),
        "dry_run_execution_plan": dry_run_execution_plan,
        "execute_live": bool(execute_live),
        "live_execution_config_path": live_execution_config_path,
        "live_execution_result": live_execution_result,
    }
    path = _projection_path(output_dir, run_id)
    _append_projection_row(path, row)
    logging.info(
        "[Spring-LiveObserver] wrote projection | account=%s | signal=%s | symbol=%s | path=%s",
        account,
        bool(signal),
        row["signal_symbol"],
        path,
    )
    return {"ok": True, "path": str(path), "row": row}


def _next_signal_check_epoch(now_epoch: float | None = None, *, second: int = 2) -> float:
    if not 0 <= int(second) <= 59:
        raise ValueError(f"signal check second must be in [0, 59], got {second}")
    if now_epoch is None:
        now_epoch = time.time()
    now = datetime.fromtimestamp(now_epoch, tz=timezone.utc)
    target = now.replace(second=int(second), microsecond=0)
    if now < target:
        return target.timestamp()
    return (target + timedelta(minutes=1)).timestamp()


def _sleep_until_epoch(target_epoch: float) -> None:
    while True:
        remaining = float(target_epoch) - time.time()
        if remaining <= 0:
            return
        if remaining > 1.0:
            time.sleep(min(remaining - 0.2, 10.0))
        elif remaining > 0.2:
            time.sleep(max(0.05, remaining - 0.05))
        else:
            time.sleep(min(remaining, 0.02))


def run_loop(
    *,
    config_path: str,
    account: str,
    output_dir: str,
    hub_max_age_secs: int,
    active_symbols: set[str],
    audit_preview_limit: int,
    run_id: str,
    max_iterations: int,
    signal_check_second: int,
    verify_exchange: bool,
    execute_live: bool,
    live_execution_config_path: str | None,
) -> None:
    iteration = 0
    _write_heartbeat(
        output_dir=output_dir,
        run_id=run_id,
        account=account,
        mode="loop",
        iteration=None,
        status="started",
        payload={
            "config_path": config_path,
            "hub_max_age_secs": int(hub_max_age_secs),
            "max_iterations": int(max_iterations),
            "signal_check_second": int(signal_check_second),
            "verify_exchange": bool(verify_exchange),
            "execute_live": bool(execute_live),
            "live_execution_config_path": live_execution_config_path,
        },
    )
    while True:
        if max_iterations > 0 and iteration >= max_iterations:
            _write_heartbeat(
                output_dir=output_dir,
                run_id=run_id,
                account=account,
                mode="loop",
                iteration=iteration,
                status="completed",
            )
            logging.info("[Spring-LiveObserver] loop completed | run_id=%s | iterations=%s", run_id, iteration)
            return

        next_epoch = _next_signal_check_epoch(second=signal_check_second)
        _write_heartbeat(
            output_dir=output_dir,
            run_id=run_id,
            account=account,
            mode="loop",
            iteration=iteration + 1,
            status="sleeping",
            payload={
                "next_run_epoch": next_epoch,
                "next_run_bj": datetime.fromtimestamp(next_epoch, tz=timezone.utc).astimezone(BJ).strftime("%Y-%m-%d %H:%M:%S"),
            },
        )
        _sleep_until_epoch(next_epoch)
        iteration += 1
        try:
            result = run_once(
                config_path=config_path,
                account=account,
                output_dir=output_dir,
                hub_max_age_secs=hub_max_age_secs,
                active_symbols=active_symbols,
                audit_preview_limit=audit_preview_limit,
                run_id=run_id,
                loop_iteration=iteration,
                verify_exchange=verify_exchange,
                execute_live=execute_live,
                live_execution_config_path=live_execution_config_path,
            )
        except Exception as exc:
            _write_heartbeat(
                output_dir=output_dir,
                run_id=run_id,
                account=account,
                mode="loop",
                iteration=iteration,
                status="error",
                payload={"error": str(exc)},
            )
            raise
        row = dict(result.get("row") or {})
        _write_heartbeat(
            output_dir=output_dir,
            run_id=run_id,
            account=account,
            mode="loop",
            iteration=iteration,
            status="ok",
            payload={
                "projection_path": result.get("path"),
                "signal_present": bool(row.get("signal_present")),
                "signal_symbol": row.get("signal_symbol"),
                "c_bar_bj": row.get("c_bar_bj"),
                "signal_time_bj": row.get("signal_time_bj"),
                "elapsed_ms": row.get("elapsed_ms"),
                "execute_live": bool(row.get("execute_live")),
                "live_execution_outcome": (row.get("live_execution_result") or {}).get("outcome") if isinstance(row.get("live_execution_result"), dict) else None,
            },
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Spring-SABC live runner")
    parser.add_argument("--config", default="strategies/spring/config.json")
    parser.add_argument("--account", required=True)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--hub-max-age-secs", type=int, default=120)
    parser.add_argument("--active-symbol", action="append", default=[])
    parser.add_argument("--audit-preview-limit", type=int, default=30)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--max-iterations", type=int, default=0)
    parser.add_argument("--signal-check-second", type=int, default=2)
    parser.add_argument("--dry-run-verify-exchange", action="store_true")
    parser.add_argument("--execute-live", action="store_true")
    parser.add_argument("--live-execution-config", default="")
    args = parser.parse_args()

    setup_logging()
    if args.hub_max_age_secs <= 0:
        raise SystemExit("--hub-max-age-secs must be > 0")
    if args.audit_preview_limit <= 0:
        raise SystemExit("--audit-preview-limit must be > 0")
    if args.max_iterations < 0:
        raise SystemExit("--max-iterations must be >= 0")
    if not 0 <= args.signal_check_second <= 59:
        raise SystemExit("--signal-check-second must be in [0, 59]")
    if args.execute_live and args.loop:
        raise SystemExit("--execute-live currently supports once mode only")
    if args.execute_live and not str(args.live_execution_config).strip():
        raise SystemExit("--live-execution-config is required with --execute-live")
    run_id = str(args.run_id).strip() or _build_run_id(args.account)
    active_symbols = _active_symbols_from_args(args.active_symbol)
    if args.loop:
        run_loop(
            config_path=args.config,
            account=args.account,
            output_dir=args.output_dir,
            hub_max_age_secs=int(args.hub_max_age_secs),
            active_symbols=active_symbols,
            audit_preview_limit=int(args.audit_preview_limit),
            run_id=run_id,
            max_iterations=int(args.max_iterations),
            signal_check_second=int(args.signal_check_second),
            verify_exchange=bool(args.dry_run_verify_exchange),
            execute_live=bool(args.execute_live),
            live_execution_config_path=str(args.live_execution_config).strip() or None,
        )
    else:
        result = run_once(
            config_path=args.config,
            account=args.account,
            output_dir=args.output_dir,
            hub_max_age_secs=int(args.hub_max_age_secs),
            active_symbols=active_symbols,
            audit_preview_limit=int(args.audit_preview_limit),
            run_id=run_id,
            loop_iteration=None,
            verify_exchange=bool(args.dry_run_verify_exchange),
            execute_live=bool(args.execute_live),
            live_execution_config_path=str(args.live_execution_config).strip() or None,
        )
        _write_heartbeat(
            output_dir=args.output_dir,
            run_id=run_id,
            account=args.account,
            mode="once",
            iteration=None,
            status="ok",
            payload={
                "projection_path": result.get("path"),
                "signal_present": bool((result.get("row") or {}).get("signal_present")),
                "signal_symbol": (result.get("row") or {}).get("signal_symbol"),
                "execute_live": bool((result.get("row") or {}).get("execute_live")),
                "live_execution_outcome": ((result.get("row") or {}).get("live_execution_result") or {}).get("outcome") if isinstance((result.get("row") or {}).get("live_execution_result"), dict) else None,
            },
        )


if __name__ == "__main__":
    main()
