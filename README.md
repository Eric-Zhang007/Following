# Following: Telegram/Web 预览 -> Bitget 交易执行器

本项目实现：
`监听` -> `解析（规则 + VLM）` -> `结构化校验` -> `风控` -> `Bitget 执行`。

默认安全策略：`dry_run: true`。

## 本次补丁重点
- 新增 `web_preview` 监听模式：轮询 `https://t.me/s/IvanCryptotalk`，无需 Telegram `api_id/api_hash`
- 新增网页帖子解析：提取 `message_id / text / image_url`
- 新增媒体层：下载图片、计算 `sha256`、本地落盘、SQLite 去重
- 新增 VLM 抽取层：`nim/kimi` 可配置，接口 `VLMClient.extract(image_bytes, text_context)`
- 新增抗幻觉 schema：`evidence/source/uncertain_fields/extraction_warnings/safety/confidence`
- Hybrid pipeline 升级：规则优先，规则不完整或含图时调用 VLM
- 低置信度/关键字段缺失/校验失败：`notify_only`，不自动下单
- 风控升级：
  - 严格止损与仓位计算（按账户风险预算）
  - 50x 杠杆策略（`CAP` 或 `REJECT`）
  - 回撤熔断、最大持仓数、信号质量阈值
- 新增生产守护体系：
  - `account_poller` 主动监测账户/持仓/挂单
  - `order_reconciler` 订单对账与部分成交补救
  - `risk_daemon` 缺止损修复、强平距离检查、熔断
  - `price_feed` 行情刷新（`ws` 请求会自动降级到 `rest`）
  - `kill_switch` 本地文件/环境变量/SQLite 开关
  - `health_server` 提供 `/healthz` `/readyz` `/metrics`

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
```

## 关键配置
### 1) 监听模式
```yaml
listener:
  mode: "web_preview"    # telegram / web_preview
  polling_seconds: 5
  target_url: "https://t.me/s/IvanCryptotalk"
```

- `web_preview`：无需 Telegram 用户 API 凭证
- `telegram`：需要 `telegram.api_id/api_hash`

### 2) VLM
```yaml
vlm:
  enabled: true
  provider: "nim"        # nim / kimi
  model: "..."
  api_key_env: "NIM_API_KEY"
  base_url: "..."
  confidence_threshold: 0.8
  below_threshold_action: "notify_only"
```

### 3) 风控（严格止损）
```yaml
risk:
  max_account_drawdown_pct: 0.15
  account_risk_per_trade: 0.003
  max_leverage: 10
  leverage_policy: "CAP"           # CAP / REJECT
  default_stop_loss_pct: 0.006
  hard_stop_loss_required: true
  max_entry_slippage_pct: 0.003
  max_notional_per_trade: 200
  max_open_positions: 3
  cooldown_seconds: 300
  min_signal_quality: 0.8
```

### 4) 主动监测与守护
```yaml
monitor:
  enabled: true
  poll_intervals:
    account_seconds: 5
    positions_seconds: 3
    open_orders_seconds: 3
  price_feed:
    mode: "ws"           # ws / rest（ws优先，失败自动降级rest）
    interval_seconds: 2
  health:
    host: "127.0.0.1"
    port: 8080
```

### 5) 计划委托能力探测
```yaml
bitget:
  plan_orders_probe_on_startup: true
  plan_orders_capability_ttl_seconds: 300
  plan_orders_probe_timeout_seconds: 6
  plan_orders_probe_safe_mode_on_failure: true
```

监控启动后，即使没有新信号也会持续运行：
- 主动刷新权益、保证金、持仓、挂单
- 检查不变量（仓位必须有保护、重复开仓防护、异常持仓告警）
- 风险触发时自动进入 `safe_mode`（禁止新开仓）

## 执行层止损说明（生产）
`ENTRY_SIGNAL` 会生成成套订单意图（Entry + Stop-loss + 可选 TP）。

当前支持两种止损模式：
- `risk.stoploss.sl_order_type: trigger`（推荐）：使用 Bitget 原生计划委托止损（plan/trigger）。
- `risk.stoploss.sl_order_type: local_guard`：本地守护止损兜底（依赖 `price_feed`）。

Plan/Trigger 能力探测（生产建议开启）：
- 启动时会按 `bitget.plan_orders_probe_on_startup` 主动探测计划委托能力并缓存。
- `supports_plan_orders()` 使用 TTL 缓存（`plan_orders_capability_ttl_seconds`），到期自动重探测。
- 若探测明确不支持且当前为 `trigger/plan`，运行时自动降级为 `local_guard`，并写入事件：
  - `PLAN_ORDER_CAPABILITY_PROBE`
  - `PLAN_ORDER_FALLBACK`
- 网络超时场景会标记为 `unknown`（短 TTL 重试），不会直接永久判定不支持。

关键语义：
- ENTRY 成交后立刻补挂 SL；部分成交按已成交数量挂对应 SL。
- 风控守护发现缺 SL 时先补挂，超时失败才触发保护性平仓 + 熔断。
- `move_sl_to_be` 会撤旧 SL 并重挂到保本位（含 `break_even_buffer_pct` 缓冲）。
- 支持分批 TP（reduce-only / tradeSide=close）并纳入对账。

one-way / hedge 差异：
- `one_way_mode`：平仓/止损使用 `reduceOnly=YES`。
- `hedge_mode`：平仓/止损使用 `tradeSide=close`，并按 `holdSide` 映射 long/short。

## 生产运行建议
1. 推荐 `isolated` + `risk.max_leverage` 硬上限，避免高杠杆信号直接照抄执行。
2. 上线顺序：`dry_run=true` 观察 -> 小额实盘 -> 再逐步扩大规模。
3. 配置 kill switch：
   - 文件触发：创建 `./KILL_SWITCH`（内容为空或 `safe` 进入 `SAFE_MODE`，`panic` 进入 `PANIC_CLOSE`）
   - 环境变量：`TRADER_KILL_SWITCH=1`（SAFE_MODE）或 `TRADER_KILL_SWITCH=panic`（PANIC_CLOSE）
4. 明确安全模式语义：
   - `safe_mode`：禁止新开仓，只允许风控修复、止损、减仓和平仓。
   - `panic_close`：一次性触发保护性平仓流程，并保持禁止开仓。
5. `ws` 行情建议保持开启；若降级到 `rest` 且使用 `local_guard`，系统会执行额外安全降级动作（可配置）。
6. 若启用 `monitor.price_feed.ws_required_for_local_guard=true`：
   - `local_guard` + `rest` 降级会导致 `readyz` 不通过；
   - 建议配合 `safe_mode` 自动启用，防止在低质量行情源下继续开仓。

## 测试
```bash
pytest
```

新增测试覆盖：
- `tests/test_web_preview_listener.py`
- `tests/test_vlm_anti_hallucination.py`
- `tests/test_vlm_pipeline_branch.py`
- `tests/test_risk_stoploss_required.py`
- `tests/test_leverage_cap.py`
- `tests/test_position_sizing.py`
- `tests/test_circuit_breaker.py`
- `tests/test_account_poller.py`
- `tests/test_risk_daemon_stoploss_autofix.py`
- `tests/test_risk_daemon_sl_autofix_then_panic.py`
- `tests/test_reconciler_partial_fill.py`
- `tests/test_reconciler_partial_fill_places_sl.py`
- `tests/test_stoploss_trigger_orders.py`
- `tests/test_move_sl_to_be.py`
- `tests/test_ws_price_feed_fallback.py`
- `tests/test_plan_order_capability_probe.py`
- `tests/test_ws_ticker_parsing.py`
- `tests/test_readyz_ws_gate.py`
- `tests/test_tp_size_resolution.py`
- `tests/test_kill_switch.py`
- `tests/test_circuit_breaker_drawdown.py`
- `tests/test_rate_limiter_backoff.py`

## 免责声明
仅供技术研究与工程示例，不构成投资建议。
