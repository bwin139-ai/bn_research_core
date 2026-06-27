# Telegram Proxy Runbook

本文档记录 Telegram Bot API 专用代理的生产运维事实与排查步骤。

## 1. 目标

阿里云交易服务器同时访问 Binance API 与 Telegram Bot API：

- Binance API 必须继续从阿里云出口 IP 发起，满足 Binance API key IP 白名单。
- Telegram Bot API 在阿里云直连出现 `502` / timeout 时，通过 DigitalOcean 代理访问。

因此代理只允许用于 Telegram 请求，禁止配置为全局代理。

## 1.1 控制面韧性计划

2026-06-27 现场确认过一种软故障：`run_manual_trade_bot.py` 进程仍存活，但 Telegram Bot API 通过代理返回 `502 Bad Gateway` 或 read timeout，导致用户命令无法进入 bot；原 `process_monitor` 只看进程数量，无法识别这种“进程活着、控制面不可用”的状态。

根治分三层推进：

1. Telegram API 健康探测：`process_monitor` 每轮通过 `TG_BOT_TOKEN` 与 `TG_PROXY_URL` 调用 `getMe`，要求 HTTP 200 且 JSON `ok=true`。该层只记录、告警和恢复提示，不启停进程、不访问 Binance、不改变交易状态。
2. Telegram 代理冗余：准备至少一个备用代理节点后，扩展发送队列和 bot polling 的代理切换能力。单一代理节点无法保证 Telegram 区域性故障时的连续可用。
3. Telegram 失效时的应急管理入口：保留服务器侧只读/低风险 CLI 管理能力，至少覆盖 `status`、`account_detail`、`pending_orders`、必要撤单等关键操作，避免 Telegram 控制面不可用时完全失去策略管理入口。

当前已先推进第 1 层。

## 2. 主机与职责

阿里云交易服务器：

```text
ssh aliyun-bn
path: /root/bn_research_core
role: production trading host
```

DigitalOcean Telegram 代理机：

```text
ssh do-proxy
public ip: 206.189.90.153
region: SGP1
service: tinyproxy
port: 8888
```

代理访问控制：

```text
Allow 127.0.0.1
Allow 8.218.96.252
```

其中 `8.218.96.252` 是阿里云交易服务器公网出口 IP。

## 3. 配置原则

阿里云服务器只配置 Telegram 专用变量：

```text
TG_PROXY_URL=http://206.189.90.153:8888
```

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
APIError(code=-2015): Invalid API-key, IP, or permissions for action, request ip: 206.189.90.153
```

## 4. 代码路径

Telegram 代理只在以下入口生效：

```text
core/manual_trade_bot.py
core/notify/tg_queue_sender.py
core/process_monitor.py
```

`run_manual_trade_bot.py` 通过 `TG_PROXY_URL` 配置 Telegram HTTPX 请求：

```text
Application.builder().proxy_url(...).get_updates_proxy_url(...)
```

`core/notify/tg_queue_sender.py` 通过 `TG_PROXY_URL` 配置 `requests` 的 per-request `proxies`，并设置：

```text
session.trust_env = False
```

该设置避免 sender 继承系统全局代理变量。

`core/process_monitor.py` 的 `telegram_api` check 同样只读取 `TG_PROXY_URL` 并设置 `session.trust_env = False`，用于验证 Telegram Bot API 控制面是否可用。

## 5. 常用检查

检查 DigitalOcean 代理：

```bash
ssh do-proxy 'systemctl is-active tinyproxy && ss -lntp | grep 8888 && grep -nE "^(Port|Listen|Allow|DisableViaHeader)" /etc/tinyproxy/tinyproxy.conf'
```

从阿里云测试 Telegram 代理：

```bash
ssh -o RemoteCommand=none -T aliyun-bn 'cd /root/bn_research_core && set -a && . ./.env && set +a && curl -x "$TG_PROXY_URL" -sS --max-time 25 -o /tmp/tg_getme.out -w "http=%{http_code} time=%{time_total}\n" "https://api.telegram.org/bot${TG_BOT_TOKEN}/getMe" && head -c 300 /tmp/tg_getme.out && rm -f /tmp/tg_getme.out'
```

检查阿里云没有全局代理：

```bash
ssh -o RemoteCommand=none -T aliyun-bn 'cd /root/bn_research_core && grep -nE "PROXY" .env deploy.env'
```

预期只出现：

```text
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
telegram_api_bad_response
missing_token_env
missing_proxy_env
```

其中 `telegram_request_exception` / `telegram_api_bad_response` 对应 Telegram 链路不可用；`missing_*_env` 对应进程启动环境或配置问题。

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

重启 DigitalOcean tinyproxy：

```bash
ssh do-proxy 'systemctl restart tinyproxy && systemctl is-active tinyproxy && ss -lntp | grep 8888'
```

## 7. 注意事项

1. DigitalOcean 上原有 Caddy 仍监听 `80/443`，当前用途是历史 OpenAI API reverse proxy；Telegram 代理使用 `8888`，不占用 `80/443`。
2. 2026-06-27 安装 `tinyproxy` 时发现旧 Caddy apt source GPG key 失效，已在服务器上禁用该 apt source 文件以便系统 apt 正常更新；不影响正在运行的 Caddy 服务。
3. DigitalOcean Droplet 控制台显示系统提示 `System restart required`。生产代理当前已正常运行；是否重启该 Droplet 应另行确认窗口，不要在交易时段随意重启。
4. 若 Binance 再次报 `request ip: 206.189.90.153`，第一优先级是检查阿里云 `.env` / `deploy.env` 和运行中进程环境是否误配了全局代理变量。
5. 若 Telegram 推送正常但命令无响应，检查 `run_manual_trade_bot.py` 是否存活；若只有推送进程活着，bot 轮询进程可能已退出。
