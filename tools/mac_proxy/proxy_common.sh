#!/usr/bin/env bash
set -euo pipefail

MAC_PROXY_SERVICE="${MAC_PROXY_SERVICE:-Wi-Fi}"
AWS_PROXY_HOST="${AWS_PROXY_HOST:-13.230.97.189}"
AWS_PROXY_USER="${AWS_PROXY_USER:-ubuntu}"
AWS_PROXY_SSH_KEY="${AWS_PROXY_SSH_KEY:-$HOME/Downloads/LightsailDefaultKey-ap-northeast-1.pem}"
AWS_PROXY_SOCKS_HOST="${AWS_PROXY_SOCKS_HOST:-127.0.0.1}"
AWS_PROXY_SOCKS_PORT="${AWS_PROXY_SOCKS_PORT:-18080}"
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

mono_http_url() {
  printf 'http://%s:%s' "$MONO_HTTP_HOST" "$MONO_HTTP_PORT"
}

mono_socks_url() {
  printf 'socks5h://%s:%s' "$MONO_SOCKS_HOST" "$MONO_SOCKS_PORT"
}

test_aws_socks() {
  curl --socks5-hostname "${AWS_PROXY_SOCKS_HOST}:${AWS_PROXY_SOCKS_PORT}" \
    --connect-timeout 5 --max-time 12 -fsS https://ifconfig.me/ip >/dev/null
}

aws_tunnel_listener_pids() {
  lsof -nP -iTCP:"$AWS_PROXY_SOCKS_PORT" -sTCP:LISTEN -t 2>/dev/null || true
}

aws_tunnel_ssh_pids() {
  lsof -nP -iTCP:"$AWS_PROXY_SOCKS_PORT" -sTCP:LISTEN -a -c ssh -t 2>/dev/null || true
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

set_system_proxy_aws() {
  networksetup -setwebproxystate "$MAC_PROXY_SERVICE" off
  networksetup -setsecurewebproxystate "$MAC_PROXY_SERVICE" off
  networksetup -setsocksfirewallproxy "$MAC_PROXY_SERVICE" "$AWS_PROXY_SOCKS_HOST" "$AWS_PROXY_SOCKS_PORT"
  networksetup -setsocksfirewallproxystate "$MAC_PROXY_SERVICE" on
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

set_git_proxy_mono() {
  require_git
  git config --global http.proxy "$(mono_http_url)"
  git config --global https.proxy "$(mono_http_url)"
}

update_zshrc_proxy_block() {
  local mode="$1"
  python3 - "$ZSHRC_PATH" "$mode" "$PROXY_BLOCK_BEGIN" "$PROXY_BLOCK_END" \
    "$(aws_socks_url)" "$(mono_http_url)" "$(mono_socks_url)" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1]).expanduser()
mode = sys.argv[2]
begin = sys.argv[3]
end = sys.argv[4]
aws_socks = sys.argv[5]
mono_http = sys.argv[6]
mono_socks = sys.argv[7]

if mode == "aws":
    block_lines = [
        begin,
        "# mode: aws-ssh-socks",
        "unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY",
        f"export all_proxy={aws_socks}",
        f"export ALL_PROXY={aws_socks}",
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
else:
    raise SystemExit(f"unknown mode: {mode}")

text = path.read_text() if path.exists() else ""
lines = text.splitlines()
out = []
inside = False
for line in lines:
    if line == begin:
        inside = True
        continue
    if line == end:
        inside = False
        continue
    if not inside:
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

