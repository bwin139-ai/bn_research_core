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
from core.live.execution_runner import (
    account_local_activity_precheck,
    execute_live_execution_plan,
    load_live_execution_config,
    reconcile_strategy_open_trades,
)
from core.live.live_data_gate import expected_snapshot_from_signal_check_epoch
from core.live.live_data_gate import record_finalized_payload_not_ready_event
from core.live.live_data_gate import wait_finalized_candidate_inputs_for_snapshot
from core.live.live_state import load_live_state
from core.live.signal_gate import build_live_signal_gate
from core.live.market_data_hub import load_finalized_candidate_inputs_from_hub
from core.message_bridge import send_to_bot
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
    return f"SPRINGLIVE_{account_key}_{ts_utc}"


def _projection_path(output_dir: str, run_id: str) -> Path:
    return Path(output_dir) / f"spring_live.{run_id}.jsonl"


def _heartbeat_path(output_dir: str, run_id: str) -> Path:
    return Path(output_dir) / f"spring_live_heartbeat.{run_id}.json"


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
        "run_mode": "live",
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


def _runner_notify_enabled(*, execute_live: bool, live_execution_config_path: str | None) -> bool:
    if not execute_live:
        return False
    if not live_execution_config_path or not str(live_execution_config_path).strip():
        raise ValueError("live execution config path is required when execute_live is enabled")
    live_execution_config = load_live_execution_config(live_execution_config_path)
    return bool(live_execution_config.get("notify_enabled", False))


def _strategy_concurrency_scope(live_execution_config: Mapping[str, Any] | None) -> str:
    if not isinstance(live_execution_config, Mapping):
        return "symbol"
    scope = str(live_execution_config.get("strategy_concurrency_scope") or "").strip()
    if scope not in {"symbol", "account"}:
        raise ValueError("live execution config strategy_concurrency_scope must be symbol or account")
    return scope


def _notify_runner_started(*, notify_enabled: bool, account: str, run_id: str, mode: str) -> None:
    message = f"[Spring-Live] runner started | account={account} | run_id={run_id} | mode={mode}"
    logging.info(message)
    if notify_enabled:
        send_to_bot(message, label="spring")


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
    expected_signal_time_ts = c_bar_ts + 60_000
    if signal_time_ts != expected_signal_time_ts:
        raise ValueError(
            "signal_time_ts must equal latest_closed_bar_ts + 60000 for Spring CB=C+1m semantics, "
            f"got {signal_time_ts} vs expected {expected_signal_time_ts}"
        )
    return c_bar_ts, c_bar_bj, signal_time_ts, signal_time_bj, cross_section, dict(full_df)


def _latest_closed_closes(full_df: Mapping[str, Any], c_bar_ts: int) -> dict[str, float]:
    result: dict[str, float] = {}
    for raw_symbol, value in dict(full_df or {}).items():
        symbol = str(raw_symbol).upper().strip()
        if not symbol or not isinstance(value, pd.DataFrame) or value.empty or "close" not in value.columns:
            continue
        df = value.copy()
        try:
            df = df.sort_index()
            df.index = pd.Index([int(x) for x in df.index])
        except Exception:
            continue
        rows = df[df.index <= int(c_bar_ts)]
        if rows.empty:
            continue
        try:
            close = float(pd.to_numeric(rows["close"], errors="coerce").iloc[-1])
        except Exception:
            continue
        if close > 0:
            result[symbol] = close
    return result


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
            "rank_chg_24h": rec.get("rank_chg_24h"),
            "rank_vol_24h": rec.get("rank_vol_24h"),
            "score": rec.get("score"),
            "score_rank_all": rec.get("score_rank_all"),
            "score_order": rec.get("score_order"),
            "selected_score_order": rec.get("selected_score_order"),
            "score_top_n": rec.get("score_top_n"),
            "selected_for_structure": rec.get("selected_for_structure"),
            "universe_hard_gate_pass": rec.get("universe_hard_gate_pass"),
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


def _decision_audit(audits: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for symbol in sorted(audits.keys()):
        rec = audits[symbol]
        rows.append({
            "symbol": str(symbol).upper().strip(),
            "universe_pass": bool(rec.get("universe_pass", False)),
            "universe_hard_gate_pass": rec.get("universe_hard_gate_pass"),
            "structure_pass": bool(rec.get("structure_pass", False)),
            "signal_emit": bool(rec.get("signal_emit", False)),
            "fail_reason": rec.get("signal_fail_reason") or rec.get("fail_reason"),
            "rank_chg_24h": rec.get("rank_chg_24h"),
            "rank_vol_24h": rec.get("rank_vol_24h"),
            "score": rec.get("score"),
            "score_rank_all": rec.get("score_rank_all"),
            "score_order": rec.get("score_order"),
            "selected_score_order": rec.get("selected_score_order"),
            "score_top_n": rec.get("score_top_n"),
            "selected_for_structure": rec.get("selected_for_structure"),
            "chg_24h": rec.get("chg_24h"),
            "vol_24h": rec.get("vol_24h"),
            "a_time_ms": rec.get("a_time_ms"),
            "b_time_ms": rec.get("b_time_ms"),
            "c_time_ms": rec.get("c_time_ms"),
            "risk_pct": rec.get("risk_pct"),
            "position_notional_usdt": rec.get("position_notional_usdt"),
        })
    return sorted(
        rows,
        key=lambda row: (
            row["score_rank_all"] is None,
            int(row["score_rank_all"] or 10**9),
            str(row["symbol"]),
        ),
    )


def _decision_scoreboard(audits: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in _decision_audit(audits)
        if row.get("score") is not None
    ]


def _blocked_execution_result(
    *,
    dry_run_execution_plan: Mapping[str, Any] | None,
    account_local_precheck: Mapping[str, Any] | None,
) -> dict[str, Any]:
    executable_blockers = []
    if isinstance(dry_run_execution_plan, Mapping):
        executable_blockers.extend(list(dry_run_execution_plan.get("executable_blockers") or []))
    if isinstance(account_local_precheck, Mapping):
        executable_blockers.extend(list(account_local_precheck.get("blockers") or []))
    return {
        "ok": False,
        "outcome": "execution_blocked_by_precheck",
        "reason": ",".join(str(x) for x in executable_blockers),
        "executable_blockers": executable_blockers,
        "account_local_precheck": dict(account_local_precheck or {}),
    }


def _not_ready_projection_row(
    *,
    config_path: str,
    account: str,
    run_id: str,
    loop_iteration: int | None,
    output_dir: str,
    hub_max_age_secs: int,
    audit_preview_limit: int,
    active_symbols: set[str],
    started_utc_ms: int,
    expected_latest_closed_bar_ts: int | None,
    expected_signal_time_ts: int | None,
    candidate_payload_wait: Mapping[str, Any],
    execute_live: bool,
    verify_exchange: bool,
    live_execution_config_path: str | None,
) -> dict[str, Any]:
    finished_utc_ms = _now_utc_ms()
    signal_time_ts = int(expected_signal_time_ts or 0) or None
    c_bar_ts = int(expected_latest_closed_bar_ts or 0) or None
    row = {
        "schema_version": 1,
        "run_mode": "live",
        "projection_type": "spring_live",
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
        "signal_time_bj": _fmt_bj_from_ms(signal_time_ts),
        "c_bar_ts": c_bar_ts,
        "c_bar_bj": _fmt_bj_from_ms(c_bar_ts),
        "hub_reason": "finalized_candidate_payload_not_ready_for_current_snapshot",
        "hub_symbol_count": 0,
        "cross_section_symbol_count": 0,
        "full_df_symbol_count": 0,
        "latest_close_symbol_count": 0,
        "active_symbols": sorted(active_symbols),
        "universe_candidate_count": 0,
        "structure_candidate_count": 0,
        "audited_symbol_count": 0,
        "fail_reason_counts": {},
        "audit_preview_limit": int(audit_preview_limit),
        "audit_preview": [],
        "decision_scoreboard": [],
        "decision_audit": [],
        "signal_present": False,
        "signal_symbol": None,
        "signal": None,
        "execution_intent": None,
        "dry_run_verify_exchange": bool(verify_exchange),
        "dry_run_execution_plan": None,
        "execute_live": bool(execute_live),
        "live_execution_config_path": live_execution_config_path,
        "lifecycle_reconcile": None,
        "account_local_precheck": None,
        "live_execution_result": {
            "ok": False,
            "outcome": "finalized_candidate_payload_not_ready",
            "reason": str(candidate_payload_wait.get("last_reason") or "not_ready"),
        },
        "candidate_payload_wait": dict(candidate_payload_wait),
    }
    path = _projection_path(output_dir, run_id)
    _append_projection_row(path, row)
    record_finalized_payload_not_ready_event(
        strategy_name="spring-sabc",
        strategy_label="Spring-Live",
        account=account,
        run_id=run_id,
        loop_iteration=loop_iteration,
        expected_latest_closed_bar_ts=expected_latest_closed_bar_ts,
        expected_signal_time_ts=expected_signal_time_ts,
        candidate_payload_wait=candidate_payload_wait,
        projection_path=path,
        logger=logging,
    )
    return {"ok": False, "path": str(path), "row": row}


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
    expected_latest_closed_bar_ts: int | None = None,
    expected_signal_time_ts: int | None = None,
) -> dict[str, Any]:
    started_utc_ms = _now_utc_ms()
    strategy_cfg = StrategyConfig.load(config_path)
    if str(strategy_cfg.get("strategy_name") or "") != "spring-sabc":
        raise ValueError(f"config strategy_name must be spring-sabc, got {strategy_cfg.get('strategy_name')!r}")

    live_execution_config = None
    lifecycle_reconcile = None
    account_local_precheck = None
    if execute_live:
        if not live_execution_config_path or not str(live_execution_config_path).strip():
            raise ValueError("live execution config path is required when execute_live is enabled")
        live_execution_config = load_live_execution_config(live_execution_config_path)
        live_execution_config["_projection_output_dir"] = output_dir
        live_execution_config["_projection_run_id"] = run_id
        live_execution_config["_projection_schema_version"] = 1

    candidate_payload_wait = None
    if expected_latest_closed_bar_ts is not None or expected_signal_time_ts is not None:
        if expected_latest_closed_bar_ts is None or expected_signal_time_ts is None:
            raise ValueError("expected latest_closed_bar_ts and signal_time_ts must be provided together")
        payload, candidate_payload_wait = wait_finalized_candidate_inputs_for_snapshot(
            account,
            expected_latest_closed_bar_ts=int(expected_latest_closed_bar_ts),
            expected_signal_time_ts=int(expected_signal_time_ts),
        )
        if payload is None:
            return _not_ready_projection_row(
                config_path=config_path,
                account=account,
                run_id=run_id,
                loop_iteration=loop_iteration,
                output_dir=output_dir,
                hub_max_age_secs=hub_max_age_secs,
                audit_preview_limit=audit_preview_limit,
                active_symbols=active_symbols,
                started_utc_ms=started_utc_ms,
                expected_latest_closed_bar_ts=expected_latest_closed_bar_ts,
                expected_signal_time_ts=expected_signal_time_ts,
                candidate_payload_wait=candidate_payload_wait,
                execute_live=execute_live,
                verify_exchange=verify_exchange,
                live_execution_config_path=live_execution_config_path,
            )
    else:
        payload = _load_hub_payload(account, hub_max_age_secs)
    c_bar_ts, c_bar_bj, signal_time_ts, signal_time_bj, cross_section, full_df = _validate_hub_payload(payload)
    if expected_latest_closed_bar_ts is not None and c_bar_ts != int(expected_latest_closed_bar_ts):
        raise ValueError(f"hub finalized payload latest_closed_bar_ts mismatch: got {c_bar_ts}, expected {expected_latest_closed_bar_ts}")
    if expected_signal_time_ts is not None and signal_time_ts != int(expected_signal_time_ts):
        raise ValueError(f"hub finalized payload signal_time_ts mismatch: got {signal_time_ts}, expected {expected_signal_time_ts}")
    latest_closes = _latest_closed_closes(full_df, c_bar_ts)

    if execute_live:
        lifecycle_reconcile = reconcile_strategy_open_trades(
            account,
            execution_config=live_execution_config,
            current_time_ms=signal_time_ts,
            current_time_bj=signal_time_bj,
            latest_closes=latest_closes,
            source="spring_live_pre_scan",
        )
        account_local_precheck = account_local_activity_precheck(account, strategy_name="spring-sabc")

    strategy_concurrency_scope = _strategy_concurrency_scope(live_execution_config if execute_live else None)
    signal_gate = build_live_signal_gate(
        account=account,
        strategy_name="spring-sabc",
        current_time_ms=signal_time_ts,
        configured_active_symbols=active_symbols,
        strategy_concurrency_scope=strategy_concurrency_scope,
        account_local_precheck=account_local_precheck,
        cooldown_map={} if not execute_live else None,
    )
    strategy = SpringSABCStrategy(strategy_cfg)
    strategy.cooldown_until = dict(signal_gate.cooldown_map)
    signal = None
    if not signal_gate.blocks_new_signals:
        signal = strategy.on_kline_close(
            signal_time_ts,
            cross_section,
            signal_gate.active_symbols_for_strategy,
            full_df=full_df,
        )
    intent = build_spring_live_execution_intent(signal, account=account).to_dict() if signal else None
    local_state_snapshot = load_live_state(account, strategy_name="spring-sabc") if intent else None
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
        if intent and dry_run_execution_plan:
            if (not bool(dry_run_execution_plan.get("ok_to_execute"))) or (
                strategy_concurrency_scope == "account" and
                isinstance(account_local_precheck, Mapping)
                and bool(account_local_precheck.get("blockers"))
            ):
                live_execution_result = _blocked_execution_result(
                    dry_run_execution_plan=dry_run_execution_plan,
                    account_local_precheck=account_local_precheck,
                )
            else:
                live_execution_result = execute_live_execution_plan(
                    intent,
                    execution_plan=dry_run_execution_plan,
                    execution_config=live_execution_config or load_live_execution_config(live_execution_config_path),
                    exchange_snapshot=exchange_snapshot,
                    source="spring_live",
                )
    finished_utc_ms = _now_utc_ms()

    audits = dict(getattr(strategy, "_last_signal_audits", {}) or {})
    row = {
        "schema_version": 1,
        "run_mode": "live",
        "projection_type": "spring_live",
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
        "latest_close_symbol_count": int(len(latest_closes)),
        "configured_active_symbols": sorted(active_symbols),
        "strategy_concurrency_scope": strategy_concurrency_scope,
        "active_symbols": sorted(signal_gate.active_symbols_for_strategy),
        "live_state_active_symbols": sorted(signal_gate.live_state_active_symbols),
        "cooldown_symbols": sorted(signal_gate.cooldown_symbols),
        "signal_gate": signal_gate.to_projection(),
        "universe_candidate_count": int(len(getattr(strategy, "_last_universe_candidates", []) or [])),
        "structure_candidate_count": int(len(getattr(strategy, "_last_structure_candidates", []) or [])),
        "audited_symbol_count": int(len(audits)),
        "fail_reason_counts": _fail_reason_counts(audits),
        "audit_preview_limit": int(audit_preview_limit),
        "audit_preview": _audit_preview(audits, audit_preview_limit),
        "decision_scoreboard": _decision_scoreboard(audits),
        "decision_audit": _decision_audit(audits),
        "signal_present": bool(signal),
        "signal_symbol": str((signal or {}).get("symbol") or "").upper().strip() or None,
        "signal": signal,
        "execution_intent": intent,
        "dry_run_verify_exchange": bool(verify_exchange),
        "dry_run_execution_plan": dry_run_execution_plan,
        "execute_live": bool(execute_live),
        "live_execution_config_path": live_execution_config_path,
        "lifecycle_reconcile": lifecycle_reconcile,
        "account_local_precheck": account_local_precheck,
        "live_execution_result": live_execution_result,
        "candidate_payload_wait": candidate_payload_wait,
    }
    path = _projection_path(output_dir, run_id)
    _append_projection_row(path, row)
    return {"ok": True, "path": str(path), "row": row}


def _next_signal_check_epoch(now_epoch: float | None = None, *, second: int = 5) -> float:
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
            logging.info("[Spring-Live] loop completed | run_id=%s | iterations=%s", run_id, iteration)
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
        expected_latest_closed_bar_ts, expected_signal_time_ts = expected_snapshot_from_signal_check_epoch(next_epoch)
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
                expected_latest_closed_bar_ts=expected_latest_closed_bar_ts,
                expected_signal_time_ts=expected_signal_time_ts,
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
        heartbeat_status = "ok" if bool(result.get("ok")) else "not_ready"
        _write_heartbeat(
            output_dir=output_dir,
            run_id=run_id,
            account=account,
            mode="loop",
            iteration=iteration,
            status=heartbeat_status,
            payload={
                "projection_path": result.get("path"),
                "signal_present": bool(row.get("signal_present")),
                "signal_symbol": row.get("signal_symbol"),
                "c_bar_bj": row.get("c_bar_bj"),
                "signal_time_bj": row.get("signal_time_bj"),
                "elapsed_ms": row.get("elapsed_ms"),
                "execute_live": bool(row.get("execute_live")),
                "live_execution_outcome": (row.get("live_execution_result") or {}).get("outcome") if isinstance(row.get("live_execution_result"), dict) else None,
                "candidate_payload_wait_ok": (row.get("candidate_payload_wait") or {}).get("ok") if isinstance(row.get("candidate_payload_wait"), dict) else None,
                "candidate_payload_wait_attempts": (row.get("candidate_payload_wait") or {}).get("attempts") if isinstance(row.get("candidate_payload_wait"), dict) else None,
                "candidate_payload_wait_last_reason": (row.get("candidate_payload_wait") or {}).get("last_reason") if isinstance(row.get("candidate_payload_wait"), dict) else None,
                "candidate_payload_wait_deadline_bj": (row.get("candidate_payload_wait") or {}).get("deadline_bj") if isinstance(row.get("candidate_payload_wait"), dict) else None,
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
    parser.add_argument("--signal-check-second", type=int, default=5)
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
    if args.execute_live and not str(args.live_execution_config).strip():
        raise SystemExit("--live-execution-config is required with --execute-live")
    run_id = str(args.run_id).strip() or _build_run_id(args.account)
    active_symbols = _active_symbols_from_args(args.active_symbol)
    live_execution_config_path = str(args.live_execution_config).strip() or None
    runner_notify_enabled = _runner_notify_enabled(
        execute_live=bool(args.execute_live),
        live_execution_config_path=live_execution_config_path,
    )
    _notify_runner_started(
        notify_enabled=runner_notify_enabled,
        account=args.account,
        run_id=run_id,
        mode="loop" if args.loop else "once",
    )
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
            live_execution_config_path=live_execution_config_path,
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
            live_execution_config_path=live_execution_config_path,
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
