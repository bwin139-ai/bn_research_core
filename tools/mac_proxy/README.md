# Mac Proxy Switch Tools

These scripts make the MacBook proxy mode explicit. They are intended for the
local development machine, not the Aliyun production server.

## Modes

The scripts do not open or quit GUI apps. First switch the visible app state
yourself, then run the matching script to normalize macOS, git, and shell state.

AWS WireGuard direct mode:

- Assumes the WireGuard app/tunnel is already connected.
- Turns off macOS Wi-Fi HTTP/HTTPS/SOCKS proxies.
- Unsets global git `http.proxy` and `https.proxy`.
- Writes a managed `~/.zshrc` block that unsets shell proxy env variables.
- Stops the local AWS SSH SOCKS listener on `127.0.0.1:18080` when that
  listener is owned by `ssh`.

AWS SSH HTTP mode:

- Uses TCP SSH forwarding instead of WireGuard UDP.
- Assumes MonoProxy is quit and WireGuard is stopped.
- Starts a local SSH HTTP/HTTPS proxy forward on `127.0.0.1:18082`, forwarding
  to AWS tinyproxy on the server.
- Starts a local SSH SOCKS tunnel on `127.0.0.1:18080` only for manual probes.
- Points macOS Wi-Fi HTTP/HTTPS proxy to `127.0.0.1:18082` and keeps the macOS
  SOCKS proxy disabled.
- Points global git proxy and new shell HTTP/HTTPS proxy env to
  `http://127.0.0.1:18082`.

AWS Outline/Shadowsocks mode:

- Uses the AWS Tokyo Shadowsocks service on `13.230.97.189:443/tcp`.
- Assumes MonoProxy is quit and WireGuard is stopped.
- Fetches the server Shadowsocks config over SSH and writes a private local
  client config outside the repo at
  `~/.config/bn_research_core/aws_outline_e_macbook.json`.
- Starts local `ss-local` on `127.0.0.1:18081`.
- Points macOS Wi-Fi SOCKS proxy, git proxy, and new shell proxy env to that
  local listener.
- Leaves macOS HTTP and HTTPS proxies disabled.

AWS Outline/Shadowsocks HTTP mode:

- Uses the same AWS Tokyo Shadowsocks service and local `ss-local` listener as
  Mode E.
- Requires `privoxy`; install it with `brew install privoxy` if missing.
- Starts local `privoxy` on `127.0.0.1:18083`, forwarding HTTP CONNECT traffic
  to the `ss-local` SOCKS listener on `127.0.0.1:18081`.
- Generates a neutral, long-connection-friendly `privoxy` config: filtering is
  toggled off, keep-alive is extended, socket timeout is relaxed, and
  lightweight request/connection/error logging is enabled.
- Points macOS Wi-Fi HTTP/HTTPS proxy, git proxy, and new shell HTTP/HTTPS env
  to `http://127.0.0.1:18083`.
- Also keeps macOS SOCKS enabled on `127.0.0.1:18081`, mirroring MonoProxy's
  dual HTTP+SOCKS shape more closely for long Codex threads, historical
  pagination, WebSocket, and streaming traffic.
- Applies standard macOS proxy bypass domains so local networks, Apple/iCloud
  China services, and WeChat/QQ traffic can go direct instead of competing with
  Codex over the AWS Outline path.

MonoProxy mode:

- Points macOS Wi-Fi HTTP/HTTPS proxy to `127.0.0.1:8118`.
- Points macOS Wi-Fi SOCKS proxy to `127.0.0.1:8119`.
- Points global git proxy to `http://127.0.0.1:8118`.
- Writes a managed proxy block at the end of `~/.zshrc`.

## Commands

```bash
tools/mac_proxy/proxy_status.sh
tools/mac_proxy/probe_codex_network.sh
tools/mac_proxy/install_aws_lightsail_key.sh
tools/mac_proxy/use_mode_a_monoproxy.sh
tools/mac_proxy/use_mode_b_aws_wireguard.sh
tools/mac_proxy/use_mode_c_direct.sh
tools/mac_proxy/use_mode_d_aws_ssh_socks.sh
tools/mac_proxy/use_mode_e_aws_outline.sh
tools/mac_proxy/use_mode_e_aws_outline_http.sh
```

Manual app state before running each mode:

- Mode A: MonoProxy running and `Set As System Proxy` checked; WireGuard stopped.
- Mode B: WireGuard tunnel started; MonoProxy quit.
- Mode C: MonoProxy quit; WireGuard stopped.
- Mode D: MonoProxy quit; WireGuard stopped; AWS SSH reachable.
- Mode E: MonoProxy quit; WireGuard stopped; AWS Shadowsocks service reachable.
- Mode E+: MonoProxy quit; WireGuard stopped; AWS Shadowsocks service reachable;
  `privoxy` installed.

Before using Mode D or E for the first time, install the AWS Lightsail key into
`~/.ssh`:

```bash
tools/mac_proxy/install_aws_lightsail_key.sh
```

This copies the key from `~/Downloads/LightsailDefaultKey-ap-northeast-1.pem` to
`~/.ssh/aws_lightsail_tokyo.pem`, sets `600` permissions, removes common macOS
quarantine attributes, and verifies that `ssh` can read the key. Keeping the key
under `Downloads` can trigger macOS privacy/quarantine failures such as
`Load key "...pem": Operation not permitted`.

Compatibility aliases:

```bash
tools/mac_proxy/use_monoproxy.sh
tools/mac_proxy/use_aws_wireguard_direct.sh
tools/mac_proxy/use_aws_proxy.sh
tools/mac_proxy/use_aws_ssh_socks.sh
```

`use_monoproxy.sh` maps to Mode A. `use_aws_wireguard_direct.sh` maps to Mode B.
`use_aws_proxy.sh` and `use_aws_ssh_socks.sh` are kept as compatibility aliases
for AWS SSH HTTP mode; prefer `use_mode_d_aws_ssh_socks.sh` for the explicit
mode.

Open a new terminal after switching modes so the updated `~/.zshrc` proxy block
is applied to new shells. If Codex Desktop was already open, quit and reopen it
after switching so the app does not keep using stale proxy state.

For WireGuard direct mode, `proxy_status.sh` should show no macOS HTTP/HTTPS/SOCKS
proxy, no git global proxy, no shell proxy env, IPv4 public IP `13.230.97.189`,
and either no IPv6 result or an expected WireGuard-controlled IPv6 route.

For Mode E+, `proxy_status.sh` should show HTTP/HTTPS on `127.0.0.1:18083`,
SOCKS on `127.0.0.1:18081`, git and shell HTTP proxy on `18083`, and shell
`all_proxy` on `18081`. The mode also writes proxy bypass domains for local,
Apple/iCloud China, WeChat, and QQ traffic to reduce background traffic on the
AWS Outline tunnel.

`proxy_status.sh` prints `Mode A/B/C/D/E/E+: PASS/FAIL` first. If the shell proxy result
does not match after switching, open a new terminal or run `source ~/.zshrc`.

When Codex Desktop shows `stream disconnected before completion`, run:

```bash
LOOPS=5 tools/mac_proxy/probe_codex_network.sh
```

The probe does not change proxy settings. It clears inherited shell proxy env for
its own curl calls, then tests direct, MonoProxy HTTP, MonoProxy SOCKS, AWS SSH
HTTP, AWS SSH SOCKS, and AWS Outline/Shadowsocks against
`chatgpt.com/cdn-cgi/trace` plus the Codex backend endpoint.

## Defaults

```text
MAC_PROXY_SERVICE=Wi-Fi
AWS_PROXY_HOST=13.230.97.189
AWS_PROXY_USER=ubuntu
AWS_PROXY_SSH_KEY=$HOME/.ssh/aws_lightsail_tokyo.pem
AWS_PROXY_SOCKS_PORT=18080
AWS_PROXY_HTTP_PORT=18082
AWS_OUTLINE_SOCKS_PORT=18081
AWS_OUTLINE_HTTP_PORT=18083
AWS_OUTLINE_SS_CONFIG=$HOME/.config/bn_research_core/aws_outline_e_macbook.json
AWS_OUTLINE_HTTP_CONFIG=$HOME/.config/bn_research_core/aws_outline_e_privoxy.conf
MONO_HTTP_PORT=8118
MONO_SOCKS_PORT=8119
```

All values can be overridden with environment variables before running a script.
