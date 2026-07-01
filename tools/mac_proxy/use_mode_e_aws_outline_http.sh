#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/proxy_common.sh"

require_macos_proxy_tools
require_no_mono_listeners
require_wireguard_inactive
ensure_aws_outline_http_proxy
set_system_proxy_aws_outline_http
set_git_proxy_aws_outline_http
update_zshrc_proxy_block aws-outline-http
stop_aws_tunnel_if_owned

echo "Mode E+ AWS Outline/Shadowsocks HTTP settings applied."
echo "System HTTP/HTTPS: ${AWS_OUTLINE_HTTP_HOST}:${AWS_OUTLINE_HTTP_PORT}"
echo "System SOCKS: ${AWS_OUTLINE_SOCKS_HOST}:${AWS_OUTLINE_SOCKS_PORT}"
echo "Local SOCKS listener: ${AWS_OUTLINE_SOCKS_HOST}:${AWS_OUTLINE_SOCKS_PORT}"
echo "Git proxy: $(aws_outline_http_url)"
echo "Local Shadowsocks config: ${AWS_OUTLINE_SS_CONFIG}"
echo "Local HTTP proxy config: ${AWS_OUTLINE_HTTP_CONFIG}"

verify_mode_e_http
echo "Mode E+: PASS"
print_next_shell_note
