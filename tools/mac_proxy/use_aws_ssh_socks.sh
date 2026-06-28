#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/proxy_common.sh"

require_macos_proxy_tools
ensure_aws_tunnel
set_system_proxy_aws
set_git_proxy_aws
update_zshrc_proxy_block aws

echo "Mac proxy mode switched to AWS SSH SOCKS."
echo "System SOCKS: ${AWS_PROXY_SOCKS_HOST}:${AWS_PROXY_SOCKS_PORT}"
echo "Git proxy: $(aws_socks_url)"
print_next_shell_note
