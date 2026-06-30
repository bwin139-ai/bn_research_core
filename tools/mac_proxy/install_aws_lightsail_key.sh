#!/usr/bin/env bash
set -euo pipefail

SOURCE_KEY="${AWS_PROXY_DOWNLOADS_SSH_KEY:-$HOME/Downloads/LightsailDefaultKey-ap-northeast-1.pem}"
DEST_KEY="${AWS_PROXY_DEFAULT_SSH_KEY:-$HOME/.ssh/aws_lightsail_tokyo.pem}"
DEST_DIR="$(dirname "$DEST_KEY")"

if [[ ! -f "$SOURCE_KEY" ]]; then
  echo "Source Lightsail key not found: $SOURCE_KEY" >&2
  echo "Set AWS_PROXY_DOWNLOADS_SSH_KEY=/path/to/key if it is stored elsewhere." >&2
  exit 1
fi

mkdir -p "$DEST_DIR"
chmod 700 "$DEST_DIR"
cp "$SOURCE_KEY" "$DEST_KEY"
chmod 600 "$DEST_KEY"

if command -v xattr >/dev/null 2>&1; then
  xattr -d com.apple.quarantine "$DEST_KEY" >/dev/null 2>&1 || true
  xattr -d com.apple.macl "$DEST_KEY" >/dev/null 2>&1 || true
  xattr -d com.apple.provenance "$DEST_KEY" >/dev/null 2>&1 || true
fi

if ! ssh-keygen -y -f "$DEST_KEY" >/dev/null 2>&1; then
  echo "Installed key still cannot be read by ssh: $DEST_KEY" >&2
  echo "Grant Terminal/Codex access to the source key, or move the key with Finder and rerun this script." >&2
  exit 1
fi

echo "AWS Lightsail key installed for D/E mode:"
echo "$DEST_KEY"
echo
echo "Next:"
echo "  tools/mac_proxy/use_mode_d_aws_ssh_socks.sh"
