#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/proxy_common.sh"

LOOPS="${LOOPS:-3}"
MODES="${MODES:-direct mono-http mono-socks aws-socks}"

clean_env=(
  env
  -u http_proxy -u https_proxy -u all_proxy
  -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY
  -u no_proxy -u NO_PROXY
)

curl_base=(
  curl
  --connect-timeout 8
  --max-time 25
  -sS
)

run_curl() {
  local mode="$1"
  shift
  local -a cmd
  cmd=("${curl_base[@]}")
  case "$mode" in
    direct)
      ;;
    mono-http)
      cmd+=(-x "$(mono_http_url)")
      ;;
    mono-socks)
      cmd+=(--socks5-hostname "${MONO_SOCKS_HOST}:${MONO_SOCKS_PORT}")
      ;;
    aws-socks)
      cmd+=(--socks5-hostname "${AWS_PROXY_SOCKS_HOST}:${AWS_PROXY_SOCKS_PORT}")
      ;;
    *)
      echo "unknown mode: $mode" >&2
      return 2
      ;;
  esac
  "${clean_env[@]}" "${cmd[@]}" "$@"
}

probe_mode_once() {
  local mode="$1"
  local tmp_trace tmp_endpoint
  tmp_trace="$(mktemp /tmp/codex_net_trace.XXXXXX)"
  tmp_endpoint="$(mktemp /tmp/codex_net_endpoint.XXXXXX)"

  printf '[%s] trace: ' "$mode"
  if run_curl "$mode" -o "$tmp_trace" \
    -w 'http=%{http_code} version=%{http_version} time=%{time_total} remote=%{remote_ip}\n' \
    https://chatgpt.com/cdn-cgi/trace; then
    awk -F= '/^(ip|colo|loc|http|tls|warp|gateway)=/ {printf "%s ", $0} END {print ""}' "$tmp_trace"
  else
    echo "trace_failed"
  fi

  printf '[%s] codex endpoint GET: ' "$mode"
  run_curl "$mode" -o "$tmp_endpoint" \
    -w 'http=%{http_code} version=%{http_version} time=%{time_total} remote=%{remote_ip}\n' \
    https://chatgpt.com/backend-api/codex/responses || true

  rm -f "$tmp_trace" "$tmp_endpoint"
}

echo "timestamp=$(date '+%Y-%m-%dT%H:%M:%S%z')"
echo "loops=$LOOPS"
echo "modes=$MODES"
echo

echo "== inherited proxy env =="
env | sort | grep -E '^(all_proxy|http_proxy|https_proxy|ALL_PROXY|HTTP_PROXY|HTTPS_PROXY)=' || true
echo

for i in $(seq 1 "$LOOPS"); do
  echo "== loop $i/$LOOPS =="
  for mode in $MODES; do
    probe_mode_once "$mode"
  done
  echo
  if [[ "$i" != "$LOOPS" ]]; then
    sleep 2
  fi
done
