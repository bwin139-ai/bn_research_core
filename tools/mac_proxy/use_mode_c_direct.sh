#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/proxy_common.sh"

require_macos_proxy_tools
require_no_mono_listeners
require_wireguard_inactive
set_system_proxy_direct
unset_git_proxy
update_zshrc_proxy_block direct
stop_aws_tunnel_if_owned
stop_aws_outline_if_owned

echo "Mode C direct settings applied."
echo "System HTTP/HTTPS/SOCKS proxies: off"
echo "Git global proxy: unset"

verify_mode_c
echo "Mode C: PASS"
print_next_shell_note
