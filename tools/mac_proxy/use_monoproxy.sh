#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/proxy_common.sh"

require_macos_proxy_tools
set_system_proxy_mono
set_git_proxy_mono
update_zshrc_proxy_block mono

echo "Mac proxy mode switched to MonoProxy."
echo "System HTTP/HTTPS: ${MONO_HTTP_HOST}:${MONO_HTTP_PORT}"
echo "System SOCKS: ${MONO_SOCKS_HOST}:${MONO_SOCKS_PORT}"
echo "Git proxy: $(mono_http_url)"
print_next_shell_note

