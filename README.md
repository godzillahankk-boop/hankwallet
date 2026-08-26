# Wallet Agent

Wallet Agent 是一个面向链上交易用户的 AI 持仓监控与风险情报 Agent。当前版本先不接 LLM，也不做前端网页和自动交易，只做公开钱包读取、持仓识别、定时监控和 Telegram 主动提醒。

## 当前版本

V0.1.0 - Wallet Position Tracker

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
DEFAULT_CHAIN=robinhood
CHAIN_API_BASE_URL=https://api.blockscout.com/4663
CHAIN_API_KEY=你的 Blockscout Pro API Key
CHAIN_RPC_URL=https://rpc.mainnet.chain.robinhood.com/
CHAIN_TOKEN_SEARCH_SYMBOLS=DTF
TOKEN_TRANSFER_LOOKBACK_LIMIT=500
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

## 启动

```bash
python run.py
```

启动后会同时运行：

- FastAPI health endpoint: `http://127.0.0.1:8000/health`
- Telegram Bot polling
- APScheduler 定时钱包扫描

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
- 暂无价格
- 暂无流动性判断
- 暂无 Dev Wallet / Dev X / Official X
- 暂无 KOL 和社交热度
- 暂无 Risk Score
- 暂无 LLM 判断
- 暂无自动交易
- 当前默认 Blockscout v2 风格接口；不同 Explorer 可能需要调整 adapter
- 原生 ETH 参与 swap 的精确识别需要更完整的交易和 receipt 解析，计划放到 V0.2

## 版本管理

本地 Git 仓库已初始化。第一个稳定版本建议提交：

```bash
git add .
git commit -m "feat: initialize wallet agent v0.1"
git tag v0.1.0
```

不要在未确认前 push 到远程仓库。
