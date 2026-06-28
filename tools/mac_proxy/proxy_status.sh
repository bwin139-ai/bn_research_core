#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/proxy_common.sh"

echo "== macOS network proxy: ${MAC_PROXY_SERVICE} =="
if command -v networksetup >/dev/null 2>&1; then
  networksetup -getwebproxy "$MAC_PROXY_SERVICE" || true
  echo
  networksetup -getsecurewebproxy "$MAC_PROXY_SERVICE" || true
  echo
  networksetup -getsocksfirewallproxy "$MAC_PROXY_SERVICE" || true
else
  echo "networksetup not found"
fi

echo
echo "== scutil proxy =="
if command -v scutil >/dev/null 2>&1; then
  scutil --proxy
else
  echo "scutil not found"
fi

echo
echo "== git global proxy =="
if command -v git >/dev/null 2>&1; then
  git config --global --get-regexp '^(http|https)\.proxy$' || true
else
  echo "git not found"
fi

echo
echo "== current shell proxy env =="
env | sort | grep -Ei '^(all|http|https)_proxy=' || true
env | sort | grep -Ei '^(ALL|HTTP|HTTPS)_PROXY=' || true

echo
echo "== AWS local SOCKS listener =="
lsof -nP -iTCP:"$AWS_PROXY_SOCKS_PORT" -sTCP:LISTEN || true

echo
echo "== quick network checks =="
if command -v curl >/dev/null 2>&1; then
  printf 'system ifconfig.me/ip: '
  curl --connect-timeout 5 --max-time 12 -fsS https://ifconfig.me/ip || true
  echo
  printf 'aws socks ifconfig.me/ip: '
  curl --socks5-hostname "${AWS_PROXY_SOCKS_HOST}:${AWS_PROXY_SOCKS_PORT}" \
    --connect-timeout 5 --max-time 12 -fsS https://ifconfig.me/ip || true
  echo
  printf 'aws socks openai models http: '
  curl --socks5-hostname "${AWS_PROXY_SOCKS_HOST}:${AWS_PROXY_SOCKS_PORT}" \
    --connect-timeout 5 --max-time 12 -sS -o /tmp/aws_proxy_openai_models.out \
    -w '%{http_code} time=%{time_total}' https://api.openai.com/v1/models || true
  rm -f /tmp/aws_proxy_openai_models.out
  echo
else
  echo "curl not found"
fi

