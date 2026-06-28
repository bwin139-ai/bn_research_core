# Mac Proxy Switch Tools

These scripts make the MacBook proxy mode explicit. They are intended for the
local development machine, not the Aliyun production server.

## Modes

AWS mode:

- Starts a local SSH SOCKS tunnel to the AWS Lightsail Tokyo proxy.
- Points macOS Wi-Fi SOCKS proxy to `127.0.0.1:18080`.
- Disables macOS HTTP and HTTPS proxies.
- Points global git proxy to `socks5h://127.0.0.1:18080`.
- Writes a managed proxy block at the end of `~/.zshrc`.

MonoProxy mode:

- Points macOS Wi-Fi HTTP/HTTPS proxy to `127.0.0.1:8118`.
- Points macOS Wi-Fi SOCKS proxy to `127.0.0.1:8119`.
- Points global git proxy to `http://127.0.0.1:8118`.
- Writes a managed proxy block at the end of `~/.zshrc`.

## Commands

```bash
tools/mac_proxy/proxy_status.sh
tools/mac_proxy/use_aws_proxy.sh
tools/mac_proxy/use_monoproxy.sh
```

Open a new terminal after switching modes so the updated `~/.zshrc` proxy block
is applied to new shells. If Codex Desktop was already open, quit and reopen it
after switching so the app does not keep using stale proxy state.

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
