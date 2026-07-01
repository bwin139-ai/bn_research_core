#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/proxy_common.sh"

pass_fail() {
  local label="$1"
  shift
  if "$@"; then
    printf '%s: PASS\n' "$label"
  else
    printf '%s: FAIL\n' "$label"
  fi
}

networksetup_proxy_block() {
  local kind="$1"
  case "$kind" in
    web) networksetup -getwebproxy "$MAC_PROXY_SERVICE" 2>/dev/null ;;
    secure) networksetup -getsecurewebproxy "$MAC_PROXY_SERVICE" 2>/dev/null ;;
    socks) networksetup -getsocksfirewallproxy "$MAC_PROXY_SERVICE" 2>/dev/null ;;
    *) return 2 ;;
  esac
}

proxy_enabled() {
  local kind="$1"
  networksetup_proxy_block "$kind" | awk -F': ' '/^Enabled:/ {print $2}' | grep -qx 'Yes'
}

proxy_disabled() {
  local kind="$1"
  networksetup_proxy_block "$kind" | awk -F': ' '/^Enabled:/ {print $2}' | grep -qx 'No'
}

proxy_host_port_is() {
  local kind="$1" host="$2" port="$3"
  local block
  block="$(networksetup_proxy_block "$kind")"
  grep -qx "Server: ${host}" <<<"$block" && grep -qx "Port: ${port}" <<<"$block"
}

system_proxy_mono() {
  proxy_enabled web &&
    proxy_enabled secure &&
    proxy_enabled socks &&
    proxy_host_port_is web "$MONO_HTTP_HOST" "$MONO_HTTP_PORT" &&
    proxy_host_port_is secure "$MONO_HTTP_HOST" "$MONO_HTTP_PORT" &&
    proxy_host_port_is socks "$MONO_SOCKS_HOST" "$MONO_SOCKS_PORT"
}

system_proxy_direct() {
  proxy_disabled web && proxy_disabled secure && proxy_disabled socks
}

system_proxy_aws_socks() {
  proxy_enabled web &&
    proxy_enabled secure &&
    proxy_disabled socks &&
    proxy_host_port_is web "$AWS_PROXY_HTTP_HOST" "$AWS_PROXY_HTTP_PORT" &&
    proxy_host_port_is secure "$AWS_PROXY_HTTP_HOST" "$AWS_PROXY_HTTP_PORT"
}

system_proxy_aws_outline() {
  proxy_disabled web &&
    proxy_disabled secure &&
    proxy_enabled socks &&
    proxy_host_port_is socks "$AWS_OUTLINE_SOCKS_HOST" "$AWS_OUTLINE_SOCKS_PORT"
}

system_proxy_aws_outline_http() {
  proxy_enabled web &&
    proxy_enabled secure &&
    proxy_disabled socks &&
    proxy_host_port_is web "$AWS_OUTLINE_HTTP_HOST" "$AWS_OUTLINE_HTTP_PORT" &&
    proxy_host_port_is secure "$AWS_OUTLINE_HTTP_HOST" "$AWS_OUTLINE_HTTP_PORT"
}

git_proxy_mono() {
  [[ "$(git config --global --get http.proxy 2>/dev/null || true)" == "$(mono_http_url)" ]] &&
    [[ "$(git config --global --get https.proxy 2>/dev/null || true)" == "$(mono_http_url)" ]]
}

git_proxy_empty() {
  [[ -z "$(git config --global --get-regexp '^(http|https)\.proxy$' 2>/dev/null || true)" ]]
}

git_proxy_aws_socks() {
  [[ "$(git config --global --get http.proxy 2>/dev/null || true)" == "$(aws_http_url)" ]] &&
    [[ "$(git config --global --get https.proxy 2>/dev/null || true)" == "$(aws_http_url)" ]]
}

git_proxy_aws_outline() {
  [[ "$(git config --global --get http.proxy 2>/dev/null || true)" == "$(aws_outline_socks_url)" ]] &&
    [[ "$(git config --global --get https.proxy 2>/dev/null || true)" == "$(aws_outline_socks_url)" ]]
}

git_proxy_aws_outline_http() {
  [[ "$(git config --global --get http.proxy 2>/dev/null || true)" == "$(aws_outline_http_url)" ]] &&
    [[ "$(git config --global --get https.proxy 2>/dev/null || true)" == "$(aws_outline_http_url)" ]]
}

shell_proxy_mono() {
  [[ "${http_proxy:-}" == "$(mono_http_url)" ]] &&
    [[ "${https_proxy:-}" == "$(mono_http_url)" ]] &&
    [[ "${HTTP_PROXY:-}" == "$(mono_http_url)" ]] &&
    [[ "${HTTPS_PROXY:-}" == "$(mono_http_url)" ]] &&
    [[ "${all_proxy:-}" == "$(mono_socks_url)" ]] &&
    [[ "${ALL_PROXY:-}" == "$(mono_socks_url)" ]]
}

shell_proxy_empty() {
  [[ -z "${http_proxy:-}${https_proxy:-}${all_proxy:-}${HTTP_PROXY:-}${HTTPS_PROXY:-}${ALL_PROXY:-}" ]]
}

shell_proxy_aws_socks() {
  [[ "${http_proxy:-}" == "$(aws_http_url)" ]] &&
    [[ "${https_proxy:-}" == "$(aws_http_url)" ]] &&
    [[ "${HTTP_PROXY:-}" == "$(aws_http_url)" ]] &&
    [[ "${HTTPS_PROXY:-}" == "$(aws_http_url)" ]] &&
    [[ -z "${all_proxy:-}${ALL_PROXY:-}" ]]
}

shell_proxy_aws_outline() {
  [[ -z "${http_proxy:-}${https_proxy:-}${HTTP_PROXY:-}${HTTPS_PROXY:-}" ]] &&
    [[ "${all_proxy:-}" == "$(aws_outline_socks_url)" ]] &&
    [[ "${ALL_PROXY:-}" == "$(aws_outline_socks_url)" ]]
}

shell_proxy_aws_outline_http() {
  [[ "${http_proxy:-}" == "$(aws_outline_http_url)" ]] &&
    [[ "${https_proxy:-}" == "$(aws_outline_http_url)" ]] &&
    [[ "${HTTP_PROXY:-}" == "$(aws_outline_http_url)" ]] &&
    [[ "${HTTPS_PROXY:-}" == "$(aws_outline_http_url)" ]] &&
    [[ -z "${all_proxy:-}${ALL_PROXY:-}" ]]
}

mono_listeners_active() {
  mono_http_listener_active && mono_socks_listener_active
}

mono_listeners_inactive() {
  ! mono_http_listener_active && ! mono_socks_listener_active
}

aws_socks_listener_active() {
  [[ -n "$(aws_tunnel_ssh_pids | tr -d '\n')" ]]
}

aws_socks_network_reachable() {
  test_aws_socks >/dev/null 2>&1 && test_aws_http >/dev/null 2>&1
}

aws_outline_listener_active() {
  [[ -n "$(aws_outline_ss_local_pids | tr -d '\n')" ]]
}

aws_outline_network_reachable() {
  test_aws_outline_socks >/dev/null 2>&1
}

aws_outline_http_listener_active() {
  [[ -n "$(aws_outline_privoxy_pids | tr -d '\n')" ]]
}

aws_outline_http_network_reachable() {
  test_aws_outline_socks >/dev/null 2>&1 && test_aws_outline_http >/dev/null 2>&1
}

wireguard_inactive() {
  ! wireguard_active
}

direct_public_ip_matches_aws() {
  [[ "$(public_ipv4_direct 2>/dev/null || true)" == "$AWS_WIREGUARD_PUBLIC_IP" ]]
}

direct_network_reachable() {
  public_ipv4_direct >/dev/null 2>&1
}

mode_a_pass() {
  system_proxy_mono &&
    git_proxy_mono &&
    shell_proxy_mono &&
    mono_listeners_active &&
    wireguard_inactive
}

mode_b_pass() {
  system_proxy_direct &&
    git_proxy_empty &&
    shell_proxy_empty &&
    wireguard_active &&
    mono_listeners_inactive &&
    direct_public_ip_matches_aws
}

mode_c_pass() {
  system_proxy_direct &&
    git_proxy_empty &&
    shell_proxy_empty &&
    wireguard_inactive &&
    mono_listeners_inactive &&
    direct_network_reachable
}

mode_d_pass() {
  system_proxy_aws_socks &&
    git_proxy_aws_socks &&
    shell_proxy_aws_socks &&
    mono_listeners_inactive &&
    wireguard_inactive &&
    aws_socks_listener_active &&
    aws_socks_network_reachable
}

mode_e_pass() {
  system_proxy_aws_outline &&
    git_proxy_aws_outline &&
    shell_proxy_aws_outline &&
    mono_listeners_inactive &&
    wireguard_inactive &&
    aws_outline_listener_active &&
    aws_outline_network_reachable
}

mode_e_http_pass() {
  system_proxy_aws_outline_http &&
    git_proxy_aws_outline_http &&
    shell_proxy_aws_outline_http &&
    mono_listeners_inactive &&
    wireguard_inactive &&
    aws_outline_listener_active &&
    aws_outline_http_listener_active &&
    aws_outline_http_network_reachable
}

echo "== ABCDE/E+ mode verdict =="
pass_fail "Mode A MonoProxy" mode_a_pass
pass_fail "Mode B AWS WireGuard" mode_b_pass
pass_fail "Mode C Direct" mode_c_pass
pass_fail "Mode D AWS SSH HTTP" mode_d_pass
pass_fail "Mode E AWS Outline/Shadowsocks" mode_e_pass
pass_fail "Mode E+ AWS Outline HTTP" mode_e_http_pass

echo
echo "== component checks =="
pass_fail "system proxy -> MonoProxy" system_proxy_mono
pass_fail "system proxy -> direct/off" system_proxy_direct
pass_fail "system proxy -> AWS SSH HTTP" system_proxy_aws_socks
pass_fail "system proxy -> AWS Outline/Shadowsocks" system_proxy_aws_outline
pass_fail "system proxy -> AWS Outline HTTP" system_proxy_aws_outline_http
pass_fail "git proxy -> MonoProxy" git_proxy_mono
pass_fail "git proxy -> empty" git_proxy_empty
pass_fail "git proxy -> AWS SSH HTTP" git_proxy_aws_socks
pass_fail "git proxy -> AWS Outline/Shadowsocks" git_proxy_aws_outline
pass_fail "git proxy -> AWS Outline HTTP" git_proxy_aws_outline_http
pass_fail "shell proxy -> MonoProxy" shell_proxy_mono
pass_fail "shell proxy -> empty" shell_proxy_empty
pass_fail "shell proxy -> AWS SSH HTTP" shell_proxy_aws_socks
pass_fail "shell proxy -> AWS Outline/Shadowsocks" shell_proxy_aws_outline
pass_fail "shell proxy -> AWS Outline HTTP" shell_proxy_aws_outline_http
pass_fail "MonoProxy listeners 8118/8119" mono_listeners_active
pass_fail "AWS SSH HTTP+SOCKS listeners 18082/18080" aws_socks_listener_active
pass_fail "AWS SSH HTTP network reachable" aws_socks_network_reachable
pass_fail "AWS Outline/Shadowsocks listener 18081" aws_outline_listener_active
pass_fail "AWS Outline/Shadowsocks network reachable" aws_outline_network_reachable
pass_fail "AWS Outline HTTP listener 18083" aws_outline_http_listener_active
pass_fail "AWS Outline HTTP network reachable" aws_outline_http_network_reachable
pass_fail "WireGuard 10.89.0.x active" wireguard_active
pass_fail "direct public IPv4 is AWS" direct_public_ip_matches_aws
pass_fail "direct network reachable" direct_network_reachable

echo
echo "== macOS network proxy: ${MAC_PROXY_SERVICE} =="
if command -v networksetup >/dev/null 2>&1; then
  networksetup_proxy_block web || true
  echo
  networksetup_proxy_block secure || true
  echo
  networksetup_proxy_block socks || true
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
echo "== scutil dns =="
if command -v scutil >/dev/null 2>&1; then
  scutil --dns || true
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
env | sort | grep -E '^(all_proxy|http_proxy|https_proxy|ALL_PROXY|HTTP_PROXY|HTTPS_PROXY)=' || true

echo
echo "== listeners =="
echo "MonoProxy HTTP ${MONO_HTTP_PORT}:"
lsof -nP -iTCP:"$MONO_HTTP_PORT" -sTCP:LISTEN || true
echo
echo "MonoProxy SOCKS ${MONO_SOCKS_PORT}:"
lsof -nP -iTCP:"$MONO_SOCKS_PORT" -sTCP:LISTEN || true
echo
echo "AWS SSH SOCKS ${AWS_PROXY_SOCKS_PORT}:"
lsof -nP -iTCP:"$AWS_PROXY_SOCKS_PORT" -sTCP:LISTEN || true
echo
echo "AWS SSH HTTP ${AWS_PROXY_HTTP_PORT}:"
lsof -nP -iTCP:"$AWS_PROXY_HTTP_PORT" -sTCP:LISTEN || true
echo
echo "AWS Outline/Shadowsocks ${AWS_OUTLINE_SOCKS_PORT}:"
lsof -nP -iTCP:"$AWS_OUTLINE_SOCKS_PORT" -sTCP:LISTEN || true
echo
echo "AWS Outline HTTP ${AWS_OUTLINE_HTTP_PORT}:"
lsof -nP -iTCP:"$AWS_OUTLINE_HTTP_PORT" -sTCP:LISTEN || true

echo
echo "== WireGuard direct hints =="
if command -v ifconfig >/dev/null 2>&1; then
  wireguard_ipv4_lines || true
  if ! wireguard_active; then
    echo "no 10.89.0.x WireGuard IPv4 address found"
  fi
else
  echo "ifconfig not found"
fi
if command -v route >/dev/null 2>&1; then
  echo
  route -n get default 2>/dev/null || true
  echo
  route -n get 1.1.1.1 2>/dev/null || true
fi

echo
echo "== quick network checks =="
if command -v curl >/dev/null 2>&1; then
  printf 'direct IPv4 ifconfig.me/ip: '
  public_ipv4_direct || true
  echo
  printf 'mono HTTP ifconfig.me/ip: '
  public_ipv4_mono || true
  echo
  printf 'AWS SSH SOCKS ifconfig.me/ip: '
  clean_curl_env curl --socks5-hostname "${AWS_PROXY_SOCKS_HOST}:${AWS_PROXY_SOCKS_PORT}" --connect-timeout 8 --max-time 20 -fsS https://ifconfig.me/ip || true
  echo
  printf 'AWS SSH HTTP ifconfig.me/ip: '
  clean_curl_env curl -x "$(aws_http_url)" --connect-timeout 8 --max-time 20 -fsS https://ifconfig.me/ip || true
  echo
  printf 'AWS Outline/Shadowsocks ifconfig.me/ip: '
  clean_curl_env curl --socks5-hostname "${AWS_OUTLINE_SOCKS_HOST}:${AWS_OUTLINE_SOCKS_PORT}" --connect-timeout 8 --max-time 20 -fsS https://ifconfig.me/ip || true
  echo
  printf 'AWS Outline HTTP ifconfig.me/ip: '
  clean_curl_env curl -x "$(aws_outline_http_url)" --connect-timeout 8 --max-time 20 -fsS https://ifconfig.me/ip || true
  echo
  printf 'direct Codex endpoint: '
  if test_codex_endpoint_direct; then echo "405"; else echo "fail"; fi
  printf 'mono Codex endpoint: '
  if test_codex_endpoint_mono; then echo "405"; else echo "fail"; fi
  printf 'AWS SSH HTTP Codex endpoint: '
  if test_codex_endpoint_aws_socks; then echo "405"; else echo "fail"; fi
  printf 'AWS Outline/Shadowsocks Codex endpoint: '
  if test_codex_endpoint_aws_outline; then echo "405"; else echo "fail"; fi
  printf 'AWS Outline HTTP Codex endpoint: '
  if test_codex_endpoint_aws_outline_http; then echo "405"; else echo "fail"; fi
else
  echo "curl not found"
fi
