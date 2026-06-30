#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/proxy_common.sh"

require_macos_proxy_tools
require_no_mono_listeners
require_wireguard_inactive
ensure_aws_outline_tunnel
set_system_proxy_aws_outline
set_git_proxy_aws_outline
update_zshrc_proxy_block aws-outline
stop_aws_tunnel_if_owned

echo "Mode E AWS Outline/Shadowsocks settings applied."
echo "System SOCKS: ${AWS_OUTLINE_SOCKS_HOST}:${AWS_OUTLINE_SOCKS_PORT}"
echo "System HTTP/HTTPS proxies: off"
echo "Git proxy: $(aws_outline_socks_url)"
echo "Local config: ${AWS_OUTLINE_SS_CONFIG}"

verify_mode_e
echo "Mode E: PASS"
print_next_shell_note
