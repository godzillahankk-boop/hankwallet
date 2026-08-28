# Wallet Agent

Wallet Agent 是一个面向链上交易用户的 AI 持仓监控与风险情报 Agent。当前版本先不接 LLM，也不做前端网页和自动交易，只做公开钱包读取、持仓识别、定时监控和 Telegram 主动提醒。

## 当前版本

V0.4.0 - Attention Engine V1

已实现：

- 添加公开 EVM 钱包地址
- 读取 token balance 和近期 token transfer
- 根据同一交易中的 quote token 支出与目标 token 收入识别主动买入
- 创建、更新、关闭 Position
- 加仓不重复创建 Position
- 部分卖出保持 OPEN
- 连续两次低于 dust 阈值后关闭 Position
- Scheduler 定时扫描
- Telegram 菜单、首次扫描、立即扫描、持仓查看、钱包管理
- `position_transactions` 重复交易保护
- SQLite 持久化
- 基础测试
- GMGN Read Only 数据源验证
- 使用 GMGN Holdings 自动识别 Robinhood 当前持仓
- Price Guardian 独立定时任务
- 自动记录持仓 Token 价格快照
- 5m / 15m / 60m 价格异动进入 Attention 评分
- Attention Engine 统一聚合 Price、Holder Structure、Smart Money、KOL、Liquidity
- WARNING / CRITICAL 级别 Telegram 主动提醒
- Attention Alert 去重、升级提醒、CRITICAL 冷却重发
- Watch Session 隔离：重新买入同一 Token 不继承旧持仓周期的实时评分状态

## 环境要求

- Python 3.11+
- Telegram Bot Token
- 一个 Blockscout 兼容的链上浏览器 API 地址，或后续替换为 Alchemy/Moralis/QuickNode 等数据源

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 配置

```bash
cp .env.example .env
```

编辑 `.env`：

```env
TELEGRAM_BOT_TOKEN=你的 Telegram Bot Token
DATABASE_URL=sqlite:///./data/wallet_agent.db
WALLET_SCAN_INTERVAL_SECONDS=60
LEGACY_WALLET_SCAN_ENABLED=false
DEFAULT_CHAIN=robinhood
CHAIN_API_BASE_URL=https://api.blockscout.com/4663
CHAIN_API_KEY=你的 Blockscout Pro API Key
CHAIN_RPC_URL=https://rpc.mainnet.chain.robinhood.com/
CHAIN_TOKEN_SEARCH_SYMBOLS=DTF
TOKEN_TRANSFER_LOOKBACK_LIMIT=500

GMGN_ENABLED=false
GMGN_API_BASE_URL=https://openapi.gmgn.ai
GMGN_API_KEY=
GMGN_PRIVATE_KEY_PATH=

PRICE_GUARDIAN_ENABLED=true
PRICE_GUARDIAN_ALERTS_ENABLED=false
PRICE_SCAN_INTERVAL_SECONDS=60
PRICE_MONITOR_MIN_USD_VALUE=5
PRICE_EXCLUDED_SYMBOLS=USDG,USDC,USDT,ETH,WETH
PRICE_ALERT_5M_PERCENT=10
PRICE_ALERT_15M_PERCENT=20
PRICE_ALERT_60M_PERCENT=30
```

Robinhood Chain 主网使用：

- Chain ID: `4663`
- Block explorer: `https://robinhoodchain.blockscout.com`
- Blockscout Pro REST API base: `https://api.blockscout.com/4663`

`CHAIN_API_KEY` 需要在 Blockscout 开发者后台申请；免费 tier 也需要 key。当前项目使用的是索引后的 token balances 和 token transfers，所以普通 RPC endpoint 不能直接替代这个配置。

Robinhood Chain 上 Blockscout 的余额索引可能滞后，项目会用 `CHAIN_RPC_URL` 对原生 ETH 和 ERC20 `balanceOf` 做实时校准；Blockscout 仍用于 token 列表和 transfer 记录。

如果 Blockscout 的地址 token 列表漏掉了某些 Robinhood token，可以先把 symbol 加到 `CHAIN_TOKEN_SEARCH_SYMBOLS`。系统会通过 Blockscout search 找候选合约，再用 RPC `balanceOf` 确认余额，大于 0 才展示。当前先用于补全 `DTF`。

如果钱包交易频繁，`TOKEN_TRANSFER_LOOKBACK_LIMIT` 太低会导致首次扫描只看到余额、看不到对应买入交易。Robinhood Chain 建议先用 `500`，后续再按 API 消耗和扫描耗时调整。

`CHAIN_API_BASE_URL` 需要指向 Blockscout v2 兼容 API，例如支持：

- `/api/v2/addresses/{address}/tokens`
- `/api/v2/addresses/{address}/token-transfers`
- `/api/v2/transactions/{tx_hash}`

如果你的数据源字段不同，只需要替换 `app/services/chain_client.py` 的适配器，业务扫描逻辑不用改。

## GMGN 数据源验证

当前 GMGN 只作为新增数据源和验证来源，不会接管现有 Position / PnL，也不会调用任何交易接口。

已封装的只读接口：

- `GET /v1/user/wallet_holdings`
- `GET /v1/user/wallet_activity`
- `GET /v1/user/wallet_stats`
- `GET /v1/user/wallet_token_balance`

`wallet_activity` 和 `wallet_stats` 使用 GMGN API Key；`wallet_holdings` 根据 GMGN CLI 当前实现需要 signing key 签名。Signing key 是 GMGN API 请求签名密钥，不是链上钱包私钥。

诊断脚本：

```bash
python scripts/gmgn_probe.py 0x...
```

脚本只读取 GMGN 和 Robinhood RPC/Blockscout 数据，不修改数据库。

## Price Guardian

Price Guardian 使用 GMGN `wallet_holdings` 中的 `token.price` 作为价格源。正常价格扫描路径不调用 Blockscout，也不做全量 RPC `balanceOf`。

默认每 60 秒扫描一次活跃钱包：

```env
PRICE_GUARDIAN_ENABLED=true
PRICE_GUARDIAN_ALERTS_ENABLED=false
PRICE_SCAN_INTERVAL_SECONDS=60
PRICE_MONITOR_MIN_USD_VALUE=5
PRICE_EXCLUDED_SYMBOLS=USDG,USDC,USDT,ETH,WETH
PRICE_HISTORY_RETENTION_HOURS=24
PRICE_ALERT_5M_PERCENT=10
PRICE_ALERT_15M_PERCENT=20
PRICE_ALERT_60M_PERCENT=30
PRICE_ALERT_ESCALATION_STEP_PERCENT=10
PRICE_ALERT_RESET_RATIO=0.5
PRICE_HOLDINGS_MAX_PAGES=10
PRICE_WALLET_CONCURRENCY=3
```

V0.4 推荐保持 `PRICE_GUARDIAN_ENABLED=true`，让 Price Guardian 继续负责 GMGN Holdings、Trading Position Watch 和 PriceSnapshot 数据底座；同时保持 `PRICE_GUARDIAN_ALERTS_ENABLED=false`，避免旧固定阈值价格提醒和 Attention Engine 重复给 Telegram 发同类提醒。

诊断当前钱包哪些 Token 会进入价格监控：

```bash
python scripts/price_guardian_probe.py 0x... --once
```

输出会标记 `MONITORED`、`SKIPPED_EXCLUDED_SYMBOL`、`SKIPPED_BELOW_MIN_VALUE`、`SKIPPED_NO_PRICE` 等状态。该脚本不发送 Telegram 消息，也不写入正式数据库。

发送一条真实 Telegram 通知链路测试消息：

```bash
python scripts/price_guardian_probe.py 0x... --test-alert
```

这条消息会明确标识为 `🧪 Price Guardian 测试提醒`，不会写入 `PriceAlertState`，也不会伪造成真实行情异动。

## Attention Engine

Attention Engine V1 使用 GMGN Read Only 数据源，把 Price、Holder Structure、Smart Money、KOL、Liquidity 事件统一转换成 Attention Assessment。它只判断“是否值得打扰用户”，不输出买入、卖出、加仓、清仓等交易建议。

默认调度：

```env
ATTENTION_ENGINE_ENABLED=true
ATTENTION_SMART_MONEY_INTERVAL_SECONDS=60
ATTENTION_KOL_INTERVAL_SECONDS=60
ATTENTION_MARKET_SIGNAL_INTERVAL_SECONDS=120
ATTENTION_TOKEN_SNAPSHOT_DUE_SECONDS=600
ATTENTION_TOKEN_SNAPSHOT_DISPATCH_SECONDS=120
ATTENTION_TOP_HOLDER_DUE_SECONDS=900
ATTENTION_TOP_HOLDER_DISPATCH_SECONDS=60
ATTENTION_TOKEN_SNAPSHOT_BATCH_SIZE=3
ATTENTION_TOP_HOLDER_BATCH_SIZE=1
ATTENTION_FEED_WINDOW_MINUTES=15
ATTENTION_EVENT_AGGREGATION_MINUTES=5
ATTENTION_WARNING_COOLDOWN_MINUTES=30
ATTENTION_CRITICAL_COOLDOWN_MINUTES=60
```

`DUE_SECONDS` 表示同一个 Token 至少间隔多久才允许再次采集；`DISPATCH_SECONDS` 表示 Scheduler 多久尝试处理下一小批 due Token。

V0.4 正式运行推荐：

```env
GMGN_ENABLED=true
PRICE_GUARDIAN_ENABLED=true
PRICE_GUARDIAN_ALERTS_ENABLED=false
PRICE_SCAN_INTERVAL_SECONDS=60
ATTENTION_ENGINE_ENABLED=true
ATTENTION_SMART_MONEY_INTERVAL_SECONDS=60
ATTENTION_KOL_INTERVAL_SECONDS=60
ATTENTION_MARKET_SIGNAL_INTERVAL_SECONDS=120
ATTENTION_TOKEN_SNAPSHOT_DUE_SECONDS=600
ATTENTION_TOKEN_SNAPSHOT_DISPATCH_SECONDS=120
ATTENTION_TOKEN_SNAPSHOT_BATCH_SIZE=3
ATTENTION_TOP_HOLDER_DUE_SECONDS=900
ATTENTION_TOP_HOLDER_DISPATCH_SECONDS=60
ATTENTION_TOP_HOLDER_BATCH_SIZE=1
ATTENTION_FEED_WINDOW_MINUTES=15
ATTENTION_EVENT_AGGREGATION_MINUTES=5
ATTENTION_WARNING_COOLDOWN_MINUTES=30
ATTENTION_CRITICAL_COOLDOWN_MINUTES=60
```

如果 `ATTENTION_ENGINE_ENABLED=true`，V0.4 要求 `PRICE_GUARDIAN_ENABLED=true`，否则启动会直接失败。`PRICE_GUARDIAN_ALERTS_ENABLED=false` 只关闭旧固定阈值价格 Telegram 提醒，不关闭 PriceSnapshot 和 Watch Session 数据采集。

Telegram 调试最近一次评分：

```text
/attention WINK
```

开发期验证 Attention 与 Telegram 链路：

```bash
python scripts/attention_engine_probe.py --token 0x... --simulate --send-telegram
```

测试消息会明确标识为 `🧪 Attention Engine 测试提醒`，不写入真实 GMGN 数据或正式评估状态。

## 启动

```bash
python run.py
```

启动后会同时运行：

- FastAPI health endpoint: `http://127.0.0.1:8000/health`
- Telegram Bot polling
- APScheduler Price Guardian 持仓识别和 PriceSnapshot 数据采集
- APScheduler Attention Engine 异动情报扫描

旧的 Blockscout/RPC 钱包自动扫描默认不注册后台 Scheduler job：

```env
LEGACY_WALLET_SCAN_ENABLED=false
```

Telegram 的“立即扫描”等旧功能仍保留。需要恢复旧后台自动扫描时，把该配置改为 `true`。

日志写入：

```text
logs/wallet_agent.log
```

## Telegram 使用方法

```text
/start
👛 钱包管理
➕ 添加钱包
🔄 立即扫描
💼 我的持仓
```

添加钱包后系统会自动触发首次扫描。之后 Scheduler 会按 `.env` 中的 `WALLET_SCAN_INTERVAL_SECONDS` 持续扫描活跃钱包。

## 当前识别逻辑

V0.1 保守识别真实持仓：

- 同一 tx 中钱包支出 `ETH/WETH/USDC/USDT/DAI/WBTC` 等 quote token，同时收到非 quote token，识别为 `BUY`
- 同一 tx 中钱包支出非 quote token，同时收到 quote token，识别为 `SELL`
- 单纯转入默认不创建 OPEN position
- 极小余额或明显 spam 名称会进入 `IGNORED`
- 没有足够证据时不编造买入、成本或收益

## 测试

```bash
pytest
```

测试覆盖：

- Address Validation
- Position Creation
- Position Increase
- Partial Sell
- Position Close
- Duplicate Scan
- Dust

## 当前限制

- 暂无真实 PnL
- 当前价格仅使用 GMGN Holdings 的 `token.price`
- 暂无完整 Cost Basis / Avg Cost / PnL Guardian
- 暂无 Dev Wallet / Dev X / Official X
- 已有 GMGN KOL 链上 Feed V1，暂无社交平台 KOL 内容 / X 叙事分析
- 已有 Liquidity Family V1，暂无更细的 LP migration / rug 自动判定
- 暂无完整 Risk Score 产品化展示
- 暂无 LLM 判断
- 暂无自动交易
- 当前默认 Blockscout v2 风格接口；不同 Explorer 可能需要调整 adapter
- 旧 Blockscout/RPC transaction parser 保留为 fallback，复杂 swap 语义优先使用 GMGN 数据源

## 版本管理

本地 Git 仓库已初始化。第一个稳定版本建议提交：

```bash
git add .
git commit -m "feat: initialize wallet agent v0.1"
git tag v0.1.0
```

不要在未确认前 push 到远程仓库。
