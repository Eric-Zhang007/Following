# Following: Telegram -> Bitget 交易执行器

本项目实现：Telegram 信号监听 -> 解析（规则 + 可选 LLM）-> 风控筛选 -> Bitget USDT 永续执行。

默认安全策略：`dry_run: true`。
只有显式改成 `dry_run: false` 才会真实下单。

## 功能概览
- Telegram: Telethon 用户号监听（新消息 + 编辑消息）
- 解析层：`rules_only` / `hybrid` / `llm_only`
- 风控层：符号策略、黑白名单、杠杆上限、时效、冷却、偏离保护、仓位
- 交易所层：Bitget REST（余额、行情、持仓、下单、撤单、订单查询）
- 幂等与审计：SQLite 记录消息版本、解析结果、执行记录、回执

## 安装
```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
pip install -e .
pip install -e .[dev]
```

## 运行
```bash
cp config.example.yaml config.yaml
python -m trader run --config config.yaml
# 或 trader run --config config.yaml
```

## 配置重点

### 1) `dry_run`
```yaml
dry_run: true
```
- `true`：允许拉公开行情、允许 LLM；禁止鉴权实盘下单接口
- `false`：才会真实下单

### 2) Bitget 执行参数（已修复实盘关键字段）
```yaml
bitget:
  base_url: "https://api.bitget.com"
  api_key: "..."
  api_secret: "..."
  passphrase: "..."
  product_type: "USDT-FUTURES"
  margin_mode: "isolated"      # isolated / crossed
  position_mode: "one_way_mode" # one_way_mode / hedge_mode
  force: "gtc"                 # gtc / ioc / fok / post_only
```

下单行为：
- `place-order` 请求体包含 `marginMode`
- 限价单自动包含 `force`
- `hedge_mode`：使用 `tradeSide=open/close`
- `one_way_mode`：使用 `reduceOnly`

建议：默认使用 `one_way_mode`，更直观、更少歧义。

### 3) 交易标的策略（支持更多小众币）
```yaml
filters:
  symbol_policy: "ALLOWLIST"   # ALLOWLIST / ALLOW_ALL
  symbol_whitelist: ["BTCUSDT", "ETHUSDT"]
  symbol_blacklist: []
  require_exchange_symbol: true
  min_usdt_volume_24h: null
```

策略说明：
- `ALLOWLIST`：仅允许 `symbol_whitelist`
- `ALLOW_ALL`：允许所有币（排除 `symbol_blacklist`），并可要求交易所存在性校验
- `require_exchange_symbol=true`：必须是 Bitget USDT 永续真实存在的 symbol
- `min_usdt_volume_24h`：启用后，24h 成交额低于阈值会拒单

这意味着：小众币信号可以放开，但仍会被交易所可交易性与流动性门槛过滤。

## 动态 SymbolRegistry
启动时会拉取：
- `GET /api/v2/mix/market/contracts?productType=USDT-FUTURES`

并周期刷新（默认 30 分钟），用于：
- 判断 symbol 是否可交易
- 获取精度规则（`sizePlace` / `pricePlace`）
- 获取最小下单量（`minTradeNum`）
- 可选 24h 成交额过滤

## 数量与价格精度
执行前会按合约配置处理：
- 数量按 `sizePlace` 向下取整
- 限价按 `pricePlace` 向下取整
- 若数量 `< minTradeNum`，拒单并写入 `executions.reason`

## LLM 说明
LLM 只做语义解析，不做交易决策。
所有实际执行仍必须通过 `risk.py`。

低置信度保护：
- 当 `confidence < confidence_threshold` 且 `require_confirmation_below_threshold=true`
- 状态标记 `PENDING_CONFIRMATION`，不下单

## 测试
```bash
pytest
```

新增覆盖：
- `test_allow_all_symbol_policy.py`
- `test_bitget_place_order_payload.py`
- `test_quantity_rounding.py`

## Bitget API 申请
官方文档：
- https://www.bitget.com/api-doc/common/quick-start

权限建议：
- 开启：交易权限（Trade）
- 关闭：提币、划转权限

## 实盘前检查
1. 连续 dry-run 观察日志与数据库记录
2. 确认过滤策略（`ALLOWLIST/ALLOW_ALL`）和黑名单
3. 确认 `position_mode` 与账户一致
4. 小资金先跑，再逐步放量

## 免责声明
仅供技术研究与工程示例，不构成投资建议。
