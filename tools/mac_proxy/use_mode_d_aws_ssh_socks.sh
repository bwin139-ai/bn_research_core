#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/proxy_common.sh"

require_macos_proxy_tools
require_no_mono_listeners
require_wireguard_inactive
ensure_aws_tunnel
set_system_proxy_aws
set_git_proxy_aws
update_zshrc_proxy_block aws
stop_aws_outline_http_if_owned
stop_aws_outline_if_owned

echo "Mode D AWS SSH HTTP settings applied."
echo "System HTTP/HTTPS: ${AWS_PROXY_HTTP_HOST}:${AWS_PROXY_HTTP_PORT}"
echo "System SOCKS: off"
echo "Local SOCKS test listener: ${AWS_PROXY_SOCKS_HOST}:${AWS_PROXY_SOCKS_PORT}"
echo "Git proxy: $(aws_http_url)"

verify_mode_d
echo "Mode D: PASS"
print_next_shell_note
