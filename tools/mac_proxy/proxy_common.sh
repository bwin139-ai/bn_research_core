#!/usr/bin/env bash
set -euo pipefail

MAC_PROXY_SERVICE="${MAC_PROXY_SERVICE:-Wi-Fi}"
AWS_PROXY_HOST="${AWS_PROXY_HOST:-13.230.97.189}"
AWS_WIREGUARD_PUBLIC_IP="${AWS_WIREGUARD_PUBLIC_IP:-13.230.97.189}"
AWS_PROXY_USER="${AWS_PROXY_USER:-ubuntu}"
AWS_PROXY_SSH_KEY="${AWS_PROXY_SSH_KEY:-$HOME/Downloads/LightsailDefaultKey-ap-northeast-1.pem}"
AWS_PROXY_SOCKS_HOST="${AWS_PROXY_SOCKS_HOST:-127.0.0.1}"
AWS_PROXY_SOCKS_PORT="${AWS_PROXY_SOCKS_PORT:-18080}"
AWS_OUTLINE_SS_CONFIG="${AWS_OUTLINE_SS_CONFIG:-$HOME/.config/bn_research_core/aws_outline_e_macbook.json}"
AWS_OUTLINE_SOCKS_HOST="${AWS_OUTLINE_SOCKS_HOST:-127.0.0.1}"
AWS_OUTLINE_SOCKS_PORT="${AWS_OUTLINE_SOCKS_PORT:-18081}"
MONO_HTTP_HOST="${MONO_HTTP_HOST:-127.0.0.1}"
MONO_HTTP_PORT="${MONO_HTTP_PORT:-8118}"
MONO_SOCKS_HOST="${MONO_SOCKS_HOST:-127.0.0.1}"
MONO_SOCKS_PORT="${MONO_SOCKS_PORT:-8119}"
ZSHRC_PATH="${ZSHRC_PATH:-$HOME/.zshrc}"

PROXY_BLOCK_BEGIN="# >>> bn_research_core proxy mode >>>"
PROXY_BLOCK_END="# <<< bn_research_core proxy mode <<<"

require_macos_proxy_tools() {
  if ! command -v networksetup >/dev/null 2>&1; then
    echo "networksetup not found; these scripts are for macOS." >&2
    exit 1
  fi
}

require_git() {
  if ! command -v git >/dev/null 2>&1; then
    echo "git not found." >&2
    exit 1
  fi
}

aws_socks_url() {
  printf 'socks5h://%s:%s' "$AWS_PROXY_SOCKS_HOST" "$AWS_PROXY_SOCKS_PORT"
}

aws_outline_socks_url() {
  printf 'socks5h://%s:%s' "$AWS_OUTLINE_SOCKS_HOST" "$AWS_OUTLINE_SOCKS_PORT"
}

mono_http_url() {
  printf 'http://%s:%s' "$MONO_HTTP_HOST" "$MONO_HTTP_PORT"
}

mono_socks_url() {
  printf 'socks5h://%s:%s' "$MONO_SOCKS_HOST" "$MONO_SOCKS_PORT"
}

unset_git_proxy() {
  require_git
  git config --global --unset-all http.proxy >/dev/null 2>&1 || true
  git config --global --unset-all https.proxy >/dev/null 2>&1 || true
}

clean_curl_env() {
  env \
    -u http_proxy -u https_proxy -u all_proxy \
    -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u no_proxy -u NO_PROXY \
    "$@"
}

test_aws_socks() {
  clean_curl_env curl --socks5-hostname "${AWS_PROXY_SOCKS_HOST}:${AWS_PROXY_SOCKS_PORT}" \
    --connect-timeout 5 --max-time 12 -fsS https://ifconfig.me/ip >/dev/null
}

test_aws_outline_socks() {
  clean_curl_env curl --socks5-hostname "${AWS_OUTLINE_SOCKS_HOST}:${AWS_OUTLINE_SOCKS_PORT}" \
    --connect-timeout 5 --max-time 12 -fsS https://ifconfig.me/ip >/dev/null
}

tcp_listener_pids() {
  local port="$1"
  lsof -nP -iTCP:"$port" -sTCP:LISTEN -t 2>/dev/null || true
}

mono_http_listener_pids() {
  tcp_listener_pids "$MONO_HTTP_PORT"
}

mono_socks_listener_pids() {
  tcp_listener_pids "$MONO_SOCKS_PORT"
}

mono_http_listener_active() {
  [[ -n "$(mono_http_listener_pids | tr -d '\n')" ]]
}

mono_socks_listener_active() {
  [[ -n "$(mono_socks_listener_pids | tr -d '\n')" ]]
}

aws_tunnel_listener_pids() {
  tcp_listener_pids "$AWS_PROXY_SOCKS_PORT"
}

aws_tunnel_ssh_pids() {
  lsof -nP -iTCP:"$AWS_PROXY_SOCKS_PORT" -sTCP:LISTEN -a -c ssh -t 2>/dev/null || true
}

aws_outline_listener_pids() {
  tcp_listener_pids "$AWS_OUTLINE_SOCKS_PORT"
}

aws_outline_ss_local_pids() {
  lsof -nP -iTCP:"$AWS_OUTLINE_SOCKS_PORT" -sTCP:LISTEN -a -c ss-local -t 2>/dev/null || true
}

ss_local_bin() {
  if command -v ss-local >/dev/null 2>&1; then
    command -v ss-local
  elif [[ -x /usr/local/opt/shadowsocks-libev/bin/ss-local ]]; then
    printf '%s\n' /usr/local/opt/shadowsocks-libev/bin/ss-local
  elif [[ -x /opt/homebrew/opt/shadowsocks-libev/bin/ss-local ]]; then
    printf '%s\n' /opt/homebrew/opt/shadowsocks-libev/bin/ss-local
  else
    echo "ss-local not found. Install shadowsocks-libev first." >&2
    exit 1
  fi
}

ensure_aws_tunnel() {
  if test_aws_socks; then
    echo "AWS SOCKS tunnel already healthy on ${AWS_PROXY_SOCKS_HOST}:${AWS_PROXY_SOCKS_PORT}."
    return
  fi

  local listeners ssh_listeners
  listeners="$(aws_tunnel_listener_pids | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
  ssh_listeners="$(aws_tunnel_ssh_pids | tr '\n' ' ' | sed 's/[[:space:]]*$//')"

  if [[ -n "$listeners" ]]; then
    if [[ "$listeners" == "$ssh_listeners" ]]; then
      echo "Restarting stale SSH listener on ${AWS_PROXY_SOCKS_HOST}:${AWS_PROXY_SOCKS_PORT}: ${ssh_listeners}"
      kill $ssh_listeners
      sleep 2
    else
      echo "Port ${AWS_PROXY_SOCKS_PORT} is occupied by a non-SSH process:" >&2
      lsof -nP -iTCP:"$AWS_PROXY_SOCKS_PORT" -sTCP:LISTEN >&2 || true
      echo "Stop that process or override AWS_PROXY_SOCKS_PORT." >&2
      exit 2
    fi
  fi

  if [[ ! -f "$AWS_PROXY_SSH_KEY" ]]; then
    echo "SSH key not found: $AWS_PROXY_SSH_KEY" >&2
    exit 1
  fi
  chmod 600 "$AWS_PROXY_SSH_KEY"

  ssh -i "$AWS_PROXY_SSH_KEY" \
    -o StrictHostKeyChecking=accept-new \
    -o ExitOnForwardFailure=yes \
    -o ServerAliveInterval=20 \
    -o ServerAliveCountMax=3 \
    -f -N -D "${AWS_PROXY_SOCKS_HOST}:${AWS_PROXY_SOCKS_PORT}" \
    "${AWS_PROXY_USER}@${AWS_PROXY_HOST}"

  sleep 2
  test_aws_socks
  echo "AWS SOCKS tunnel started on ${AWS_PROXY_SOCKS_HOST}:${AWS_PROXY_SOCKS_PORT}."
}

prepare_aws_outline_config() {
  local config_dir tmp_file
  config_dir="$(dirname "$AWS_OUTLINE_SS_CONFIG")"
  tmp_file="${AWS_OUTLINE_SS_CONFIG}.tmp"

  mkdir -p "$config_dir"
  chmod 700 "$config_dir"

  ssh -i "$AWS_PROXY_SSH_KEY" \
    -o StrictHostKeyChecking=accept-new \
    "${AWS_PROXY_USER}@${AWS_PROXY_HOST}" \
    "sudo python3 -c 'import json; p=\"/etc/shadowsocks-libev/config.json\"; d=json.load(open(p)); d[\"server\"]=\"${AWS_PROXY_HOST}\"; d[\"local_address\"]=\"${AWS_OUTLINE_SOCKS_HOST}\"; d[\"local_port\"]=${AWS_OUTLINE_SOCKS_PORT}; d[\"mode\"]=\"tcp_only\"; print(json.dumps(d, indent=2))'" \
    > "$tmp_file"
  chmod 600 "$tmp_file"
  mv "$tmp_file" "$AWS_OUTLINE_SS_CONFIG"
}

ensure_aws_outline_tunnel() {
  if test_aws_outline_socks; then
    echo "AWS Outline/Shadowsocks tunnel already healthy on ${AWS_OUTLINE_SOCKS_HOST}:${AWS_OUTLINE_SOCKS_PORT}."
    return
  fi

  local listeners ss_local_listeners ss_bin pid_file
  listeners="$(aws_outline_listener_pids | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
  ss_local_listeners="$(aws_outline_ss_local_pids | tr '\n' ' ' | sed 's/[[:space:]]*$//')"

  if [[ -n "$listeners" ]]; then
    if [[ "$listeners" == "$ss_local_listeners" ]]; then
      echo "Restarting stale ss-local listener on ${AWS_OUTLINE_SOCKS_HOST}:${AWS_OUTLINE_SOCKS_PORT}: ${ss_local_listeners}"
      kill $ss_local_listeners
      sleep 2
    else
      echo "Port ${AWS_OUTLINE_SOCKS_PORT} is occupied by a non-ss-local process:" >&2
      lsof -nP -iTCP:"$AWS_OUTLINE_SOCKS_PORT" -sTCP:LISTEN >&2 || true
      echo "Stop that process or override AWS_OUTLINE_SOCKS_PORT." >&2
      exit 2
    fi
  fi

  if [[ ! -f "$AWS_PROXY_SSH_KEY" ]]; then
    echo "SSH key not found: $AWS_PROXY_SSH_KEY" >&2
    exit 1
  fi
  chmod 600 "$AWS_PROXY_SSH_KEY"
  prepare_aws_outline_config

  ss_bin="$(ss_local_bin)"
  pid_file="${AWS_OUTLINE_SS_CONFIG}.pid"
  "$ss_bin" -c "$AWS_OUTLINE_SS_CONFIG" -f "$pid_file"
  sleep 2
  test_aws_outline_socks
  echo "AWS Outline/Shadowsocks tunnel started on ${AWS_OUTLINE_SOCKS_HOST}:${AWS_OUTLINE_SOCKS_PORT}."
}

stop_aws_tunnel_if_owned() {
  local ssh_listeners
  ssh_listeners="$(aws_tunnel_ssh_pids | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
  if [[ -z "$ssh_listeners" ]]; then
    return
  fi
  echo "Stopping AWS SSH SOCKS listener on ${AWS_PROXY_SOCKS_HOST}:${AWS_PROXY_SOCKS_PORT}: ${ssh_listeners}"
  kill $ssh_listeners
}

stop_aws_outline_if_owned() {
  local ss_local_listeners
  ss_local_listeners="$(aws_outline_ss_local_pids | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
  if [[ -z "$ss_local_listeners" ]]; then
    return
  fi
  echo "Stopping AWS Outline/Shadowsocks listener on ${AWS_OUTLINE_SOCKS_HOST}:${AWS_OUTLINE_SOCKS_PORT}: ${ss_local_listeners}"
  kill $ss_local_listeners
}

set_system_proxy_aws() {
  networksetup -setwebproxystate "$MAC_PROXY_SERVICE" off
  networksetup -setsecurewebproxystate "$MAC_PROXY_SERVICE" off
  networksetup -setsocksfirewallproxy "$MAC_PROXY_SERVICE" "$AWS_PROXY_SOCKS_HOST" "$AWS_PROXY_SOCKS_PORT"
  networksetup -setsocksfirewallproxystate "$MAC_PROXY_SERVICE" on
}

set_system_proxy_aws_outline() {
  networksetup -setwebproxystate "$MAC_PROXY_SERVICE" off
  networksetup -setsecurewebproxystate "$MAC_PROXY_SERVICE" off
  networksetup -setsocksfirewallproxy "$MAC_PROXY_SERVICE" "$AWS_OUTLINE_SOCKS_HOST" "$AWS_OUTLINE_SOCKS_PORT"
  networksetup -setsocksfirewallproxystate "$MAC_PROXY_SERVICE" on
}

set_system_proxy_direct() {
  networksetup -setwebproxystate "$MAC_PROXY_SERVICE" off
  networksetup -setsecurewebproxystate "$MAC_PROXY_SERVICE" off
  networksetup -setsocksfirewallproxystate "$MAC_PROXY_SERVICE" off
}

set_system_proxy_mono() {
  networksetup -setwebproxy "$MAC_PROXY_SERVICE" "$MONO_HTTP_HOST" "$MONO_HTTP_PORT"
  networksetup -setsecurewebproxy "$MAC_PROXY_SERVICE" "$MONO_HTTP_HOST" "$MONO_HTTP_PORT"
  networksetup -setsocksfirewallproxy "$MAC_PROXY_SERVICE" "$MONO_SOCKS_HOST" "$MONO_SOCKS_PORT"
  networksetup -setwebproxystate "$MAC_PROXY_SERVICE" on
  networksetup -setsecurewebproxystate "$MAC_PROXY_SERVICE" on
  networksetup -setsocksfirewallproxystate "$MAC_PROXY_SERVICE" on
}

set_git_proxy_aws() {
  require_git
  git config --global http.proxy "$(aws_socks_url)"
  git config --global https.proxy "$(aws_socks_url)"
}

set_git_proxy_aws_outline() {
  require_git
  git config --global http.proxy "$(aws_outline_socks_url)"
  git config --global https.proxy "$(aws_outline_socks_url)"
}

set_git_proxy_mono() {
  require_git
  git config --global http.proxy "$(mono_http_url)"
  git config --global https.proxy "$(mono_http_url)"
}

wireguard_ipv4_lines() {
  ifconfig 2>/dev/null | grep '10\.89\.0\.' || true
}

wireguard_active() {
  [[ -n "$(wireguard_ipv4_lines | tr -d '\n')" ]]
}

require_mono_listeners() {
  if ! mono_http_listener_active || ! mono_socks_listener_active; then
    echo "MonoProxy is not fully listening on ${MONO_HTTP_HOST}:${MONO_HTTP_PORT} and ${MONO_SOCKS_HOST}:${MONO_SOCKS_PORT}." >&2
    echo "Start MonoProxy and click Set As System Proxy, then rerun this script." >&2
    exit 2
  fi
}

require_no_mono_listeners() {
  if mono_http_listener_active || mono_socks_listener_active; then
    echo "MonoProxy still appears to be running on ${MONO_HTTP_PORT}/${MONO_SOCKS_PORT}." >&2
    echo "Quit MonoProxy from the menu bar, then rerun this script." >&2
    exit 2
  fi
}

require_wireguard_active() {
  if ! wireguard_active; then
    echo "AWS WireGuard is not active; no 10.89.0.x address was found." >&2
    echo "Open WireGuard and click Start for personal-proxy-tokyo-test-macbook, then rerun this script." >&2
    exit 2
  fi
}

require_wireguard_inactive() {
  if wireguard_active; then
    echo "WireGuard still appears active:" >&2
    wireguard_ipv4_lines >&2
    echo "Stop the WireGuard tunnel, then rerun this script." >&2
    exit 2
  fi
}

public_ipv4_direct() {
  clean_curl_env curl -4 --connect-timeout 8 --max-time 20 -fsS https://ifconfig.me/ip
}

public_ipv4_mono() {
  clean_curl_env curl -x "$(mono_http_url)" --connect-timeout 8 --max-time 20 -fsS https://ifconfig.me/ip
}

test_codex_endpoint_direct() {
  local code
  code="$(clean_curl_env curl --connect-timeout 8 --max-time 20 -sS -o /tmp/mac_proxy_codex_direct.out -w '%{http_code}' https://chatgpt.com/backend-api/codex/responses || true)"
  rm -f /tmp/mac_proxy_codex_direct.out
  [[ "$code" == "405" ]]
}

test_codex_endpoint_mono() {
  local code
  code="$(clean_curl_env curl -x "$(mono_http_url)" --connect-timeout 8 --max-time 20 -sS -o /tmp/mac_proxy_codex_mono.out -w '%{http_code}' https://chatgpt.com/backend-api/codex/responses || true)"
  rm -f /tmp/mac_proxy_codex_mono.out
  [[ "$code" == "405" ]]
}

test_codex_endpoint_aws_socks() {
  local code
  code="$(clean_curl_env curl --socks5-hostname "${AWS_PROXY_SOCKS_HOST}:${AWS_PROXY_SOCKS_PORT}" --connect-timeout 8 --max-time 20 -sS -o /tmp/mac_proxy_codex_aws_socks.out -w '%{http_code}' https://chatgpt.com/backend-api/codex/responses || true)"
  rm -f /tmp/mac_proxy_codex_aws_socks.out
  [[ "$code" == "405" ]]
}

test_codex_endpoint_aws_outline() {
  local code
  code="$(clean_curl_env curl --socks5-hostname "${AWS_OUTLINE_SOCKS_HOST}:${AWS_OUTLINE_SOCKS_PORT}" --connect-timeout 8 --max-time 20 -sS -o /tmp/mac_proxy_codex_aws_outline.out -w '%{http_code}' https://chatgpt.com/backend-api/codex/responses || true)"
  rm -f /tmp/mac_proxy_codex_aws_outline.out
  [[ "$code" == "405" ]]
}

test_trace_mono() {
  clean_curl_env curl -x "$(mono_http_url)" --connect-timeout 8 --max-time 20 -fsS https://chatgpt.com/cdn-cgi/trace >/dev/null
}

test_trace_aws_socks() {
  clean_curl_env curl --socks5-hostname "${AWS_PROXY_SOCKS_HOST}:${AWS_PROXY_SOCKS_PORT}" --connect-timeout 8 --max-time 20 -fsS https://chatgpt.com/cdn-cgi/trace >/dev/null
}

test_trace_aws_outline() {
  clean_curl_env curl --socks5-hostname "${AWS_OUTLINE_SOCKS_HOST}:${AWS_OUTLINE_SOCKS_PORT}" --connect-timeout 8 --max-time 20 -fsS https://chatgpt.com/cdn-cgi/trace >/dev/null
}

verify_mode_a() {
  test_trace_mono
  test_codex_endpoint_mono
}

verify_mode_b() {
  local ip
  ip="$(public_ipv4_direct)"
  if [[ "$ip" != "$AWS_WIREGUARD_PUBLIC_IP" ]]; then
    echo "Unexpected WireGuard public IPv4: ${ip}; expected ${AWS_WIREGUARD_PUBLIC_IP}." >&2
    return 1
  fi
  test_codex_endpoint_direct
}

verify_mode_c() {
  public_ipv4_direct >/dev/null
}

verify_mode_d() {
  test_aws_socks
  test_trace_aws_socks
  test_codex_endpoint_aws_socks
}

verify_mode_e() {
  test_aws_outline_socks
  test_trace_aws_outline
  test_codex_endpoint_aws_outline
}

update_zshrc_proxy_block() {
  local mode="$1"
  python3 - "$ZSHRC_PATH" "$mode" "$PROXY_BLOCK_BEGIN" "$PROXY_BLOCK_END" \
    "$(aws_socks_url)" "$(aws_outline_socks_url)" "$(mono_http_url)" "$(mono_socks_url)" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1]).expanduser()
mode = sys.argv[2]
begin = sys.argv[3]
end = sys.argv[4]
aws_socks = sys.argv[5]
aws_outline_socks = sys.argv[6]
mono_http = sys.argv[7]
mono_socks = sys.argv[8]

if mode == "aws":
    block_lines = [
        begin,
        "# mode: aws-ssh-socks",
        "unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY",
        f"export all_proxy={aws_socks}",
        f"export ALL_PROXY={aws_socks}",
        end,
    ]
elif mode == "aws-wireguard-direct":
    block_lines = [
        begin,
        "# mode: aws-wireguard-direct",
        "unset http_proxy https_proxy all_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY",
        "unset no_proxy NO_PROXY",
        end,
    ]
elif mode == "direct":
    block_lines = [
        begin,
        "# mode: direct",
        "unset http_proxy https_proxy all_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY",
        "unset no_proxy NO_PROXY",
        end,
    ]
elif mode == "mono":
    block_lines = [
        begin,
        "# mode: monoproxy",
        f"export http_proxy={mono_http}",
        f"export https_proxy={mono_http}",
        f"export HTTP_PROXY={mono_http}",
        f"export HTTPS_PROXY={mono_http}",
        f"export all_proxy={mono_socks}",
        f"export ALL_PROXY={mono_socks}",
        end,
    ]
elif mode == "aws-outline":
    block_lines = [
        begin,
        "# mode: aws-outline-shadowsocks",
        "unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY",
        f"export all_proxy={aws_outline_socks}",
        f"export ALL_PROXY={aws_outline_socks}",
        end,
    ]
else:
    raise SystemExit(f"unknown mode: {mode}")

text = path.read_text() if path.exists() else ""
lines = text.splitlines()
out = []
inside = False
managed_proxy_names = {
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "no_proxy",
    "NO_PROXY",
}
for line in lines:
    if line == begin:
        inside = True
        continue
    if line == end:
        inside = False
        continue
    if not inside:
        stripped = line.strip()
        if stripped.startswith("export "):
            exported = stripped[len("export "):].split("=", 1)[0].strip()
            if exported in managed_proxy_names:
                continue
        if stripped.startswith("unset "):
            unset_names = set(stripped[len("unset "):].split())
            if unset_names and unset_names.issubset(managed_proxy_names):
                continue
        out.append(line)

while out and out[-1] == "":
    out.pop()

out.extend(["", *block_lines])
path.write_text("\n".join(out) + "\n")
PY
}

print_next_shell_note() {
  echo "Open a new terminal, or run: source \"$ZSHRC_PATH\""
}
