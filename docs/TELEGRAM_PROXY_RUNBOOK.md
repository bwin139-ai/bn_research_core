# Telegram Proxy Runbook

本文档记录 Telegram Bot API 专用代理的生产运维事实与排查步骤。

## 1. 目标

阿里云交易服务器同时访问 Binance API 与 Telegram Bot API：

- Binance API 必须继续从阿里云出口 IP 发起，满足 Binance API key IP 白名单。
- Telegram Bot API 在阿里云直连出现 `502` / timeout 时，通过专用代理链访问。

因此代理只允许用于 Telegram 请求，禁止配置为全局代理。

## 1.1 控制面韧性计划

2026-06-27 现场确认过一种软故障：`run_manual_trade_bot.py` 进程仍存活，但 Telegram Bot API 通过代理返回 `502 Bad Gateway` 或 read timeout，导致用户命令无法进入 bot；原 `process_monitor` 只看进程数量，无法识别这种“进程活着、控制面不可用”的状态。

根治分三层推进：

1. Telegram API 健康探测：`process_monitor` 每轮通过 `TG_BOT_TOKEN` 与 `TG_PROXY_URLS` / `TG_PROXY_URL` 调用 `getMe`，要求至少一个代理返回 HTTP 200 且 JSON `ok=true`。该层只记录、告警和恢复提示，不启停进程、不访问 Binance、不改变交易状态。
2. Telegram 代理冗余：使用 `TG_PROXY_URLS` 配置多个代理。`tg_queue_sender` 发送每条消息时按顺序尝试代理；`run_manual_trade_bot.py` 启动时探测代理并选择第一个健康代理用于 polling，若运行中的 polling 层出现 Telegram 网络超时/连接错误，会原地 re-exec 进程并重新探测代理列表，从而跳过失效的首选代理。
3. Telegram 失效时的应急管理入口：保留服务器侧只读/低风险 CLI 管理能力，至少覆盖 `status`、`account_detail`、`pending_orders`、必要撤单等关键操作，避免 Telegram 控制面不可用时完全失去策略管理入口。

当前已推进第 1 层与第 2 层：生产环境使用 AWS Lightsail Tokyo 作为首选 Telegram 代理，旧 DigitalOcean 代理作为备用。

## 2. 主机与职责

阿里云交易服务器：

```text
ssh aliyun-bn
path: /root/bn_research_core
role: production trading host
```

AWS Lightsail Tokyo Telegram 代理机：

```text
ssh -i /Users/lyqmac/Downloads/LightsailDefaultKey-ap-northeast-1.pem ubuntu@13.230.97.189
static public ip: 13.230.97.189
static ip name: proxy-toyko
region: Tokyo / ap-northeast-1a
service: tinyproxy
port: 80
role: primary Telegram Bot API proxy and personal WireGuard backup
```

AWS Lightsail Tokyo SSH 登录策略：

```text
admin user: ubuntu
auth: SSH key only
sudo: ubuntu is in sudo group
PasswordAuthentication: no
KbdInteractiveAuthentication: no
PermitRootLogin: no
managed file: /etc/ssh/sshd_config.d/99-disable-root-login.conf
```

AWS tinyproxy 访问控制：

```text
Allow 127.0.0.1
Allow 8.218.96.252
```

其中 `8.218.96.252` 是阿里云交易服务器公网出口 IP。Lightsail 外层防火墙需要允许 `TCP 80` 与 `UDP 51820`；`TCP 80` 只用于阿里云访问 Telegram Bot API，`UDP 51820` 只用于个人 WireGuard 备用连接。

DigitalOcean Telegram 代理机：

```text
ssh do-proxy
public ip: 206.189.90.153
region: SGP1
service: tinyproxy
port: 8888
role: fallback Telegram Bot API proxy
```

代理访问控制：

```text
Allow 127.0.0.1
Allow 8.218.96.252
```

其中 `8.218.96.252` 是阿里云交易服务器公网出口 IP。

## 3. 配置原则

阿里云服务器只配置 Telegram 专用变量。当前生产代理顺序为 AWS Tokyo 优先、DigitalOcean 备用：

```text
TG_PROXY_URLS=http://13.230.97.189:80,http://206.189.90.153:8888
TG_PROXY_URL=http://206.189.90.153:8888
```

`TG_PROXY_URL` 保留为旧代码兼容变量；新代码优先读取 `TG_PROXY_URLS`。多代理使用新变量，逗号或空白分隔，按顺序优先：

```text
TG_PROXY_URLS=http://13.230.97.189:80,http://206.189.90.153:8888
```

若同时配置 `TG_PROXY_URLS` 与 `TG_PROXY_URL`，系统先使用 `TG_PROXY_URLS` 中的地址，再追加 `TG_PROXY_URL`；重复 URL 会去重。

不得在阿里云交易服务器生产环境中配置：

```text
HTTP_PROXY
HTTPS_PROXY
ALL_PROXY
http_proxy
https_proxy
all_proxy
```

原因：这些全局代理变量会被 Binance SDK / HTTP 客户端继承，导致 Binance 请求从 DigitalOcean IP 发出，并触发 Binance API key IP 白名单拒绝：

```text
APIError(code=-2015): Invalid API-key, IP, or permissions for action, request ip: <proxy-ip>
```

## 4. 代码路径

Telegram 代理只在以下入口生效：

```text
core/manual_trade_bot.py
core/notify/tg_queue_sender.py
core/process_monitor.py
```

`run_manual_trade_bot.py` 启动时读取 `TG_PROXY_URLS` / `TG_PROXY_URL`，先对每个代理调用 Telegram `getMe`，选择第一个健康代理配置 Telegram HTTPX 请求：

```text
Application.builder().proxy_url(...).get_updates_proxy_url(...)
```

Telegram polling 使用的是启动时选中的单个代理，不是 per-request 轮转。为避免首选代理运行中失效后长期卡住，`core/manual_trade_bot.py` 注册 polling 层网络错误处理：当 `update is None` 且异常为 Telegram `NetworkError` / `TimedOut`，并且配置了多个代理时，进程会原地 `exec` 自身；新进程启动阶段重新执行 `getMe` 健康探测，选择当前第一个健康代理。该机制只处理 polling 层网络错误，不处理用户命令执行过程中的业务异常，避免打断正在执行的交易指令。

`core/notify/tg_queue_sender.py` 通过 `TG_PROXY_URLS` / `TG_PROXY_URL` 配置 `requests` 的 per-request `proxies`。每条消息发送时按代理顺序尝试；当前代理失败会继续尝试下一个代理。sender 设置：

```text
session.trust_env = False
```

该设置避免 sender 继承系统全局代理变量。

`core/process_monitor.py` 的 `telegram_api` check 同样只读取 `TG_PROXY_URLS` / `TG_PROXY_URL` 并设置 `session.trust_env = False`，用于验证 Telegram Bot API 控制面是否可用；只要任一代理可用，检查即为健康。

## 4.1 MacBook 本地代理切换

MacBook 本地可能同时存在三类代理设置：

1. macOS Wi-Fi 系统代理。
2. `~/.zshrc` 中的 shell 代理环境变量。
3. `~/.gitconfig` 中的 git 全局代理。

若三者不一致，Codex / git / 浏览器可能走不同出口，表现为普通网页可用，但 Codex 长连接或 git push 卡住。仓库提供明确切换脚本：

```bash
tools/mac_proxy/proxy_status.sh
tools/mac_proxy/probe_codex_network.sh
tools/mac_proxy/use_mode_a_monoproxy.sh
tools/mac_proxy/use_mode_b_aws_wireguard.sh
tools/mac_proxy/use_mode_c_direct.sh
```

三档模式：

1. A / MonoProxy 备用模式：先手动启动 MonoProxy 并点击 `Set As System Proxy`，确认 WireGuard 已关闭，再运行 `tools/mac_proxy/use_mode_a_monoproxy.sh`。脚本要求 `127.0.0.1:8118/8119` 正在监听，并设置 macOS Wi-Fi 系统代理、git proxy 与 `~/.zshrc` 托管 proxy block。
2. B / AWS WireGuard 主力模式：先手动 Quit MonoProxy，再在 WireGuard App 里启动 `personal-proxy-tokyo-test-macbook`，然后运行 `tools/mac_proxy/use_mode_b_aws_wireguard.sh`。脚本要求看到 `10.89.0.x` 地址，关闭本机 HTTP/HTTPS/SOCKS 系统代理，清空 git proxy 和 shell proxy，并验证 direct 出口 IPv4 是 `13.230.97.189`。
3. C / Direct 直连模式：先手动 Quit MonoProxy 并停止 WireGuard tunnel，再运行 `tools/mac_proxy/use_mode_c_direct.sh`。脚本关闭全部本机代理残留，清空 git proxy 和 shell proxy，并验证普通直连网络可达；该模式不要求 ChatGPT 可直连。

也可以完全手动开关 MonoProxy / WireGuard；脚本的职责不是替代肉眼可见的软件开关，而是把系统代理、git proxy、shell proxy 和出口状态统一校准并给出 PASS/FAIL。

兼容入口：

```bash
tools/mac_proxy/use_monoproxy.sh
tools/mac_proxy/use_aws_wireguard_direct.sh
tools/mac_proxy/use_aws_proxy.sh
tools/mac_proxy/use_aws_ssh_socks.sh
```

其中 `use_monoproxy.sh` 映射到 A，`use_aws_wireguard_direct.sh` 映射到 B。`use_aws_proxy.sh` / `use_aws_ssh_socks.sh` 是 AWS SSH SOCKS 调试工具，不是日常 A/B/C 主路径。

AWS SSH SOCKS 调试模式会启动本机 SSH SOCKS 隧道：

```text
127.0.0.1:18080 -> ubuntu@13.230.97.189
```

并将 macOS Wi-Fi SOCKS、git 全局代理和新 shell 环境切到该隧道。MonoProxy 模式会恢复：

```text
HTTP/HTTPS: 127.0.0.1:8118
SOCKS:      127.0.0.1:8119
```

AWS WireGuard direct 模式用于测试 Codex Desktop 直接走 WireGuard 出口，不叠加任何本机 HTTP/HTTPS/SOCKS 代理。该模式会：

1. 关闭 macOS Wi-Fi HTTP / HTTPS / SOCKS 系统代理。
2. 删除 git 全局 `http.proxy` / `https.proxy`。
3. 在 `~/.zshrc` 托管代理块中 unset `http_proxy` / `https_proxy` / `all_proxy` 及大写变量。
4. 若 `127.0.0.1:18080` 是本脚本启动的 SSH SOCKS listener，则停止该 listener。

WireGuard direct 模式切换后，先确认 WireGuard App 中 AWS Tokyo tunnel 已连接，再运行：

```bash
tools/mac_proxy/proxy_status.sh
```

预期状态：

```text
macOS HTTP/HTTPS/SOCKS proxies: disabled
git global proxy: empty
shell proxy env: empty in new terminal
system IPv4 ifconfig.me/ip: 13.230.97.189
system IPv6 ifconfig.me/ip: no result, or explicitly confirmed non-leaking route
```

脚本只引用本机 SSH key 路径，不把私钥或 WireGuard client private key 写入仓库。切换后应新开一个 terminal，或执行：

```bash
source ~/.zshrc
```

若 Codex Desktop 在切换前已经打开，应退出并重新打开 Codex Desktop，避免 GUI 进程继续使用切换前的代理状态。

`proxy_status.sh` 会优先输出：

```text
Mode A MonoProxy: PASS/FAIL
Mode B AWS WireGuard: PASS/FAIL
Mode C Direct: PASS/FAIL
```

若刚运行过切换脚本但 shell proxy 仍显示旧值，说明当前 terminal / Codex 子进程继承了旧环境；新开 terminal 或 `source ~/.zshrc` 后再查。

该本地代理切换只影响 MacBook 开发环境；不得复制到阿里云生产交易进程环境。

若 Codex Desktop 报：

```text
stream disconnected before completion: error sending request for url (https://chatgpt.com/backend-api/codex/responses)
```

先不要反复切换系统代理；运行只读探针保留事实：

```bash
LOOPS=5 tools/mac_proxy/probe_codex_network.sh
```

探针会清理自身 curl 调用的 inherited proxy env，并分别测试 direct、MonoProxy HTTP、MonoProxy SOCKS、AWS SOCKS 到 `chatgpt.com/cdn-cgi/trace` 和 Codex backend endpoint 的 HTTP code、HTTP version、耗时与出口 trace。2026-06-28 已观察到：MonoProxy 新节点可访问 ChatGPT，Cloudflare trace 为 `colo=NRT`、`http=http/2`、出口 IP 为 IPv6；因此同一报错不能再简单归因于 AWS WireGuard，必须同时记录具体 MonoProxy 节点、IPv4/IPv6 出口、HTTP/2 streaming 长连接稳定性与 Codex Desktop 进程是否继承了旧代理环境。

## 5. 常用检查

检查 AWS Tokyo 代理：

```bash
ssh -i /Users/lyqmac/Downloads/LightsailDefaultKey-ap-northeast-1.pem ubuntu@13.230.97.189 'date -Is && systemctl is-active tinyproxy wg-quick@wg0 && sudo ss -ltnup | grep -E ":80\\b|:51820\\b" && grep -E "^(Port|Listen|Allow)" /etc/tinyproxy/tinyproxy.conf'
```

检查 AWS Tokyo WireGuard 转发与 MSS clamp：

```bash
ssh -i /Users/lyqmac/Downloads/LightsailDefaultKey-ap-northeast-1.pem ubuntu@13.230.97.189 'date -Is && sudo wg show && ip -br addr && ip route && sudo iptables -t nat -S && sudo iptables -t mangle -S && sudo iptables -S FORWARD'
```

2026-06-28 只读检查事实：`wg0` MTU 为 `1280`，IPv4 forwarding 已开启，UFW 允许 `wg0 <-> ens5` 转发，NAT `MASQUERADE` 已存在，但 `iptables -t mangle -S` 为空，尚未配置 TCP MSS clamp。同日已补上运行时规则和持久化 `wg0.conf`：

```text
iptables -t mangle -A FORWARD -i wg0 -o ens5 -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --clamp-mss-to-pmtu
```

该规则只影响从 WireGuard client 经 AWS 出口访问外网的新建 TCP 连接 SYN 包，不改变 Telegram tinyproxy、阿里云生产交易进程或 Binance API 出口。

从阿里云测试 AWS Telegram 代理：

```bash
ssh -o RemoteCommand=none -T aliyun-bn 'cd /root/bn_research_core && set -a && . deploy.env && set +a && curl -x http://13.230.97.189:80 -sS --max-time 25 -o /tmp/tg_getme_aws.out -w "http=%{http_code} time=%{time_total}\n" "https://api.telegram.org/bot${TG_BOT_TOKEN}/getMe" && head -c 300 /tmp/tg_getme_aws.out && rm -f /tmp/tg_getme_aws.out'
```

检查 DigitalOcean 备用代理：

```bash
ssh do-proxy 'systemctl is-active tinyproxy && ss -lntp | grep 8888 && grep -nE "^(Port|Listen|Allow|DisableViaHeader)" /etc/tinyproxy/tinyproxy.conf'
```

从阿里云测试当前首选 Telegram 代理变量：

```bash
ssh -o RemoteCommand=none -T aliyun-bn 'cd /root/bn_research_core && set -a && . deploy.env && set +a && first_proxy="${TG_PROXY_URLS%%,*}" && curl -x "$first_proxy" -sS --max-time 25 -o /tmp/tg_getme.out -w "proxy=$first_proxy http=%{http_code} time=%{time_total}\n" "https://api.telegram.org/bot${TG_BOT_TOKEN}/getMe" && head -c 300 /tmp/tg_getme.out && rm -f /tmp/tg_getme.out'
```

检查阿里云没有全局代理：

```bash
ssh -o RemoteCommand=none -T aliyun-bn 'cd /root/bn_research_core && grep -nE "PROXY" .env deploy.env'
```

预期只出现 `TG_PROXY_URLS` / `TG_PROXY_URL`，例如：

```text
TG_PROXY_URLS=http://13.230.97.189:80,http://206.189.90.153:8888
TG_PROXY_URL=http://206.189.90.153:8888
```

检查生产进程：

```bash
ssh -o RemoteCommand=none -T aliyun-bn 'ps -eo pid,lstart,etime,stat,args | awk "/[r]un_manual_trade_bot.py|[c]ore\\/notify\\/tg_queue_sender.py|[p]rocess_monitor.py/ {print}"'
```

预期 `tg_queue_sender.py` 与 `run_manual_trade_bot.py` 各只有一个。

检查 `process_monitor` 是否已经记录 Telegram API 健康状态：

```bash
ssh -o RemoteCommand=none -T aliyun-bn 'cd /root/bn_research_core && tail -n 5 output/logs/process_monitor.log | grep telegram_bot_api'
```

异常时 `reason` 常见为：

```text
telegram_request_exception
telegram_api_all_proxies_failed
missing_token_env
missing_proxy_env
```

其中 `telegram_api_all_proxies_failed` 对应所有 Telegram 代理链路都不可用；`missing_*_env` 对应进程启动环境或配置问题。

检查 Binance 仍从阿里云直连：

```bash
ssh -o RemoteCommand=none -T aliyun-bn 'curl -sS --max-time 15 -o /tmp/binance_time.out -w "http=%{http_code} time=%{time_total}\n" https://fapi.binance.com/fapi/v1/time && head -c 200 /tmp/binance_time.out && rm -f /tmp/binance_time.out'
```

可进一步用生产 Python 环境检查账户查询：

```bash
ssh -o RemoteCommand=none -T aliyun-bn 'cd /root/bn_research_core && set -a && . ./.env && set +a && /root/service_env/bin/python - <<'"'"'PY'"'"'
from core.live.binance_exec import get_account_status
for account in ["chen912", "deepa999", "junjie2026", "mybwin139"]:
    res = get_account_status(account)
    print(account, "ok=" + str(res.get("ok")), "err=" + str(res.get("reason") or res.get("error") or "")[:160])
PY'
```

## 6. 重启命令

重启 Telegram sender 与 manual bot：

```bash
ssh -o RemoteCommand=none -T aliyun-bn 'cd /root/bn_research_core && python3 - <<'"'"'PY'"'"'
import os, signal, subprocess, time
exact = {
    "/root/service_env/bin/python -u run_manual_trade_bot.py",
    "/root/service_env/bin/python -u core/notify/tg_queue_sender.py",
}
out = subprocess.check_output(["ps", "-eo", "pid,args"], text=True)
for line in out.splitlines()[1:]:
    line = line.strip()
    if not line:
        continue
    pid_s, args = line.split(None, 1)
    if args in exact:
        os.kill(int(pid_s), signal.SIGINT)
        print(f"sent SIGINT {pid_s} {args}")
time.sleep(8)
PY
set -a
. ./.env
set +a
nohup /root/service_env/bin/python -u run_manual_trade_bot.py >> output/logs/manual_trade_bot.log 2>&1 &
nohup bash -lc "cd /root/bn_research_core && set -a && source deploy.env && set +a && PYTHONPATH=/root/bn_research_core /root/service_env/bin/python -u core/notify/tg_queue_sender.py" >> output/logs/tg_queue_sender.console.log 2>&1 &
sleep 15
ps -eo pid,lstart,etime,stat,args | awk "/[r]un_manual_trade_bot.py|[c]ore\\/notify\\/tg_queue_sender.py|[p]rocess_monitor.py/ {print}"'
```

重启 AWS Tokyo tinyproxy：

```bash
ssh -i /Users/lyqmac/Downloads/LightsailDefaultKey-ap-northeast-1.pem ubuntu@13.230.97.189 'sudo systemctl restart tinyproxy && systemctl is-active tinyproxy && sudo ss -lntp | grep ":80\\b"'
```

重启 DigitalOcean 备用 tinyproxy：

```bash
ssh do-proxy 'systemctl restart tinyproxy && systemctl is-active tinyproxy && ss -lntp | grep 8888'
```

## 7. 注意事项

1. AWS Lightsail Tokyo 已绑定 Lightsail Static IP `13.230.97.189`（名称 `proxy-toyko`）。只要该 Static IP 保持 attached 且未 release，实例 stop/start 后公网入口也应保持不变；若 detach/release 或重建代理机，必须同步更新 `TG_PROXY_URLS`、WireGuard client config 与本文档。
2. AWS Tokyo WireGuard 只作为个人网络备用，不参与 Binance API 调用。任何时候都不得在阿里云生产进程环境设置全局代理变量。
3. AWS Tokyo SSH 管理入口为 `ubuntu` 用户加 Lightsail SSH key；密码登录和 root 直接 SSH 登录均已关闭。不要删除 `ubuntu` 用户、不要移除其 `sudo` 权限、不要删除本机 Lightsail 私钥，除非先建立并验证新的管理员 key 入口。
4. DigitalOcean 上原有 Caddy 仍监听 `80/443`，当前用途是历史 OpenAI API reverse proxy；Telegram 备用代理使用 `8888`，不占用 `80/443`。
5. 2026-06-27 安装 `tinyproxy` 时发现旧 Caddy apt source GPG key 失效，已在服务器上禁用该 apt source 文件以便系统 apt 正常更新；不影响正在运行的 Caddy 服务。
6. DigitalOcean Droplet 控制台显示系统提示 `System restart required`。生产代理当前已正常运行；是否重启该 Droplet 应另行确认窗口，不要在交易时段随意重启。
7. 2026-06-28 测试过 DigitalOcean SGP1 个人 WireGuard 节点 `139.59.116.55`，MacBook/iPhone 访问速度过慢，用户已销毁该 Droplet；不要把该 IP 作为活跃代理或文档中的生产节点。
8. 若 Binance 再次报 `request ip: 13.230.97.189` 或 `request ip: 206.189.90.153`，第一优先级是检查阿里云 `.env` / `deploy.env` 和运行中进程环境是否误配了全局代理变量。
9. 若 Telegram 推送正常但命令无响应，检查 `run_manual_trade_bot.py` 是否存活；若只有推送进程活着，bot 轮询进程可能已退出。
