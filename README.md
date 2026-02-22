# Following: Telegram -> Bitget 交易执行器使用指南

本项目已经可以运行，建议先以 `dry_run=true` 验证流程，再切实盘。

当前能力：
- 监听 Telegram 频道（新消息 + 编辑消息）
- 解析信号（规则解析 + 可选 LLM 语义解析）
- 风控校验（白名单、杠杆、冷却、偏离保护、仓位）
- Bitget USDT 永续下单（或 dry-run 模拟）
- SQLite 幂等、日志、回执、LLM 缓存

## 1. 安装与启动

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
pip install -e .[dev]
```

复制配置：

```bash
cp config.example.yaml config.yaml
```

运行：

```bash
python -m trader run --config config.yaml
# 或
trader run --config config.yaml
```

测试：

```bash
pytest
```

## 2. 关键安全原则

- `dry_run` 默认必须是 `true`
- 只有你显式改成 `dry_run: false` 才会实盘下单
- API Key 仅开交易权限，关闭提币权限
- 建议先在子账户/小资金上跑

## 3. 配置总览（config.yaml）

```yaml
dry_run: true

telegram:
  api_id: 123456
  api_hash: "replace_me"
  session_name: "ivan_listener"
  channel: "@IvanCryptotalk"
  notify_chat_id: null

bitget:
  base_url: "https://api.bitget.com"
  api_key: "replace_me"
  api_secret: "replace_me"
  passphrase: "replace_me"
  product_type: "USDT-FUTURES"

filters:
  symbol_whitelist: ["BTCUSDT", "ETHUSDT", "SOLUSDT", "CYBERUSDT", "KITEUSDT"]
  max_leverage: 10
  allow_sides: ["LONG", "SHORT"]
  max_signal_age_seconds: 20
  leverage_over_limit_action: "CLAMP"

risk:
  account_risk_per_trade: 0.005
  max_notional_per_trade: 200
  entry_slippage_pct: 0.3
  cooldown_seconds: 300
  default_stop_loss_pct: 1.0
  assumed_equity_usdt: 1000

logging:
  level: "INFO"
  file: "trader.log"
  rich: true

storage:
  db_path: "trader.db"

execution:
  limit_price_strategy: "MID"

llm:
  enabled: true
  mode: "hybrid"  # rules_only / hybrid / llm_only
  provider: "openai"
  model: "YOUR_MODEL_NAME"
  api_key_env: "OPENAI_API_KEY"
  base_url: null
  timeout_seconds: 15
  max_retries: 2
  confidence_threshold: 0.75
  require_confirmation_below_threshold: true
  redact_patterns:
    - "(?i)api_key\\s*[:=]\\s*\\S+"
    - "(?i)secret\\s*[:=]\\s*\\S+"
```

## 4. Telegram 如何配置

本项目使用 Telethon（用户号 MTProto，不依赖 bot 被拉进频道）。

### 4.1 申请 `api_id` / `api_hash`

官方入口：
- https://my.telegram.org
- 文档： https://core.telegram.org/api/obtaining_api_id

步骤：
1. 用你的 Telegram 账号登录 `my.telegram.org`
2. 进入 `API development tools`
3. 创建应用后拿到 `api_id` 和 `api_hash`

### 4.2 写入 config

```yaml
telegram:
  api_id: 你的_api_id
  api_hash: "你的_api_hash"
  session_name: "ivan_listener"
  channel: "@IvanCryptotalk"
```

### 4.3 首次登录行为

首次运行会提示输入：
- 手机号
- 验证码
- 两步验证密码（若开启）

成功后会在本地生成 session 文件，后续复用。

## 5. DeepSeek API 如何配置（LLM 语义解析层）

本项目的 LLM 客户端是 OpenAI SDK `responses.create`，DeepSeek 可通过 OpenAI 兼容接口接入。

DeepSeek 官方文档：
- https://api-docs.deepseek.com/

官方说明要点：
- 可兼容 OpenAI SDK
- `base_url` 可用 `https://api.deepseek.com` 或 `https://api.deepseek.com/v1`

### 5.1 设置环境变量

```bash
export DEEPSEEK_API_KEY='你的key'
```

### 5.2 config.yaml 中这样写

```yaml
llm:
  enabled: true
  mode: "hybrid"
  provider: "openai"
  model: "deepseek-chat"
  api_key_env: "DEEPSEEK_API_KEY"
  base_url: "https://api.deepseek.com/v1"
  timeout_seconds: 15
  max_retries: 2
  confidence_threshold: 0.75
  require_confirmation_below_threshold: true
```

### 5.3 模式建议

- `rules_only`：不用 LLM，最保守
- `hybrid`：推荐，规则解析失败/不完整时才调 LLM
- `llm_only`：全部走 LLM，不推荐直接实盘

### 5.4 低置信度保护

当：
- `require_confirmation_below_threshold=true`
- 且 `confidence < confidence_threshold`

系统会：
- 记录 `PENDING_CONFIRMATION`
- 只通知，不执行下单

## 6. Bitget API 如何配置？在哪里申请？

Bitget API 官方文档：
- 快速开始： https://www.bitget.com/api-doc/common/quick-start

根据官方文档：登录后可进入 API Key 管理页面创建 Key，并设置权限。

### 6.1 申请入口（Web）

1. 登录 Bitget 账号
2. 进入 API Key 管理（API Management）
3. 创建 API Key，保存三项：
   - API Key
   - Secret Key
   - Passphrase

### 6.2 权限建议（务必）

只开：
- Read（可选）
- Trade（必需）

关闭：
- Transfer
- Withdraw

### 6.3 填入 config.yaml

```yaml
bitget:
  base_url: "https://api.bitget.com"
  api_key: "你的_api_key"
  api_secret: "你的_api_secret"
  passphrase: "你的_passphrase"
  product_type: "USDT-FUTURES"
```

## 7. 你现在最关心的结论

可以使用，但请按这个顺序：
1. `dry_run: true` 跑通 1~2 天
2. 检查 `trader.log` + `trader.db` 中解析、拒单原因、拟执行记录
3. 小资金 + 子账户切 `dry_run: false`
4. 再逐步放量

## 8. 常见问题

### Q1: 为什么我明明开了 LLM 还没下单？
- 可能是低置信度触发了 `PENDING_CONFIRMATION`
- 可能被风控拒绝（看 `executions.reason`）

### Q2: edited message 会自动重下单吗？
- 当前策略是：编辑消息会记录版本，但默认跳过执行（防止重复/歧义触发）

### Q3: DeepSeek 能不能直接替代 OpenAI SDK？
- 本项目是 OpenAI SDK 调用方式
- 通过 `llm.base_url + llm.api_key_env + llm.model` 可接 DeepSeek 兼容接口

## 9. 免责声明

本项目仅为技术实现示例，不构成投资建议。实盘前请自行评估风险并承担全部后果。
