# Mac Proxy Switch Tools

These scripts make the MacBook proxy mode explicit. They are intended for the
local development machine, not the Aliyun production server.

## Modes

The scripts do not open or quit GUI apps. First switch the visible app state
yourself, then run the matching script to normalize macOS, git, and shell state.

AWS SSH SOCKS mode:

- Starts a local SSH SOCKS tunnel to the AWS Lightsail Tokyo proxy.
- Points macOS Wi-Fi SOCKS proxy to `127.0.0.1:18080`.
- Disables macOS HTTP and HTTPS proxies.
- Points global git proxy to `socks5h://127.0.0.1:18080`.
- Writes a managed proxy block at the end of `~/.zshrc`.

AWS WireGuard direct mode:

- Assumes the WireGuard app/tunnel is already connected.
- Turns off macOS Wi-Fi HTTP/HTTPS/SOCKS proxies.
- Unsets global git `http.proxy` and `https.proxy`.
- Writes a managed `~/.zshrc` block that unsets shell proxy env variables.
- Stops the local AWS SSH SOCKS listener on `127.0.0.1:18080` when that
  listener is owned by `ssh`.

MonoProxy mode:

- Points macOS Wi-Fi HTTP/HTTPS proxy to `127.0.0.1:8118`.
- Points macOS Wi-Fi SOCKS proxy to `127.0.0.1:8119`.
- Points global git proxy to `http://127.0.0.1:8118`.
- Writes a managed proxy block at the end of `~/.zshrc`.

## Commands

```bash
tools/mac_proxy/proxy_status.sh
tools/mac_proxy/probe_codex_network.sh
tools/mac_proxy/use_mode_a_monoproxy.sh
tools/mac_proxy/use_mode_b_aws_wireguard.sh
tools/mac_proxy/use_mode_c_direct.sh
```

Manual app state before running each mode:

- Mode A: MonoProxy running and `Set As System Proxy` checked; WireGuard stopped.
- Mode B: WireGuard tunnel started; MonoProxy quit.
- Mode C: MonoProxy quit; WireGuard stopped.

Compatibility aliases:

```bash
tools/mac_proxy/use_monoproxy.sh
tools/mac_proxy/use_aws_wireguard_direct.sh
tools/mac_proxy/use_aws_proxy.sh
tools/mac_proxy/use_aws_ssh_socks.sh
```

`use_monoproxy.sh` maps to Mode A. `use_aws_wireguard_direct.sh` maps to Mode B.
`use_aws_proxy.sh` and `use_aws_ssh_socks.sh` are debug-only AWS SSH SOCKS tools,
not part of the normal A/B/C workflow.

Open a new terminal after switching modes so the updated `~/.zshrc` proxy block
is applied to new shells. If Codex Desktop was already open, quit and reopen it
after switching so the app does not keep using stale proxy state.

For WireGuard direct mode, `proxy_status.sh` should show no macOS HTTP/HTTPS/SOCKS
proxy, no git global proxy, no shell proxy env, IPv4 public IP `13.230.97.189`,
and either no IPv6 result or an expected WireGuard-controlled IPv6 route.

`proxy_status.sh` prints `Mode A/B/C: PASS/FAIL` first. If the shell proxy result
does not match after switching, open a new terminal or run `source ~/.zshrc`.

When Codex Desktop shows `stream disconnected before completion`, run:

```bash
LOOPS=5 tools/mac_proxy/probe_codex_network.sh
```

The probe does not change proxy settings. It clears inherited shell proxy env for
its own curl calls, then tests direct, MonoProxy HTTP, MonoProxy SOCKS, and AWS
SOCKS against `chatgpt.com/cdn-cgi/trace` plus the Codex backend endpoint.

## Defaults

```text
MAC_PROXY_SERVICE=Wi-Fi
AWS_PROXY_HOST=13.230.97.189
AWS_PROXY_USER=ubuntu
AWS_PROXY_SSH_KEY=$HOME/Downloads/LightsailDefaultKey-ap-northeast-1.pem
AWS_PROXY_SOCKS_PORT=18080
MONO_HTTP_PORT=8118
MONO_SOCKS_PORT=8119
```

All values can be overridden with environment variables before running a script.
