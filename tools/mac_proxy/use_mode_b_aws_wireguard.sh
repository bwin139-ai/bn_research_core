#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/proxy_common.sh"

require_macos_proxy_tools
require_wireguard_active
require_no_mono_listeners
set_system_proxy_direct
unset_git_proxy
update_zshrc_proxy_block aws-wireguard-direct
stop_aws_tunnel_if_owned
stop_aws_outline_http_if_owned
stop_aws_outline_if_owned

echo "Mode B AWS WireGuard settings applied."
echo "System HTTP/HTTPS/SOCKS proxies: off"
echo "Git global proxy: unset"
echo "Expected public IPv4: ${AWS_WIREGUARD_PUBLIC_IP}"

verify_mode_b
echo "Mode B: PASS"
print_next_shell_note
