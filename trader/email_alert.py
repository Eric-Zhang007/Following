from __future__ import annotations

import json
import os
import smtplib
import time
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any

from trader.config import EmailAlertConfig

_EVENT_LABELS = {
    "RISK_MODE_DISABLED": "风控开关关闭",
    "CROSS_MARGIN": "全仓模式提醒",
    "HIGH_LEVERAGE": "高杠杆提醒",
    "STOPLOSS_PLACE_FAIL": "止损下单失败",
    "PANIC_CLOSE": "紧急平仓触发",
    "PLAN_ORDER_FALLBACK": "计划单降级执行",
    "WS_DEGRADED": "行情通道降级",
    "PRICE_FEED_WS_FALLBACK": "价格源降级到轮询",
    "PRICE_FEED_LOCAL_GUARD_DEGRADED": "本地保护降级",
    "PRICE_FEED_ERROR": "价格源异常",
    "ORDER_SUBMITTED": "挂单已提交",
    "ORDER_FILLED": "订单成交回报",
    "TP_SUBMITTED": "止盈已提交",
    "TP_SUBMIT_FAILED": "止盈提交失败",
    "SL_TRIGGER_SUBMITTED": "止损已提交",
    "SL_TRIGGER_FAILED": "止损提交失败",
    "PRIVATE_MESSAGE_SKIPPED_STARTUP": "启动前消息已跳过",
    "PRESTARTUP_STOPLOSS_GUARD_REJECTED": "历史止损触发拒单",
    "LIQUIDATION_DISTANCE_RISK": "强平距离过近",
    "POSITION_CLOSED_SUMMARY": "平仓总结",
    "POSITION_CLOSED_PNL_FETCH_FAIL": "平仓盈亏查询失败",
    "DRAWDOWN_BREAKER": "回撤断路器触发",
    "MARGIN_USED_HIGH": "保证金占用过高",
    "API_ERROR_BURST": "接口错误突增",
    "API_ERROR_BURST_RECOVERED": "接口错误恢复",
    "MARGIN_USED_HIGH_RECOVERED": "保证金占用恢复",
    "KILL_SWITCH": "风控熔断",
    "UNKNOWN_POSITION": "未知仓位",
    "UNKNOWN_POSITION_RECOVERED": "未知仓位恢复",
    "PROTECTIVE_CLOSE": "保护性平仓",
    "PROTECTIVE_CLOSE_FAILED": "保护性平仓失败",
    "LOCAL_GUARD_TRIGGERED": "本地止损触发",
    "LOCAL_GUARD_TRIGGER_FAILED": "本地止损失败",
    "SL_MISSING_RECOVERED": "止损缺失已恢复",
    "PRICE_FEED_ERROR_RECOVERED": "价格源异常恢复",
    "PRICE_FEED_WS_RECONNECT_RECOVERED": "行情连接恢复",
    "SL_AUTOFIX_FAILED_THEN_PANIC": "止损修复失败并触发紧急流程",
    "NO_SL_DRAWDOWN_20": "无止损仓位亏损超阈值",
}

_PAYLOAD_LABELS = {
    "symbol": "币种",
    "side": "方向",
    "leverage": "杠杆",
    "reason": "原因",
    "thread_id": "线程ID",
    "purpose": "用途",
    "order_type": "订单类型",
    "order_id": "订单ID",
    "client_order_id": "客户端订单ID",
    "message_id": "消息ID",
    "drawdown": "回撤比例",
    "margin_ratio": "保证金占比",
    "mark_price": "标记价格",
    "last_mark_price": "最后标记价格",
    "liq_price": "强平价格",
    "max_liquidation_distance_pct": "强平距离阈值",
    "entry_price": "开仓均价",
    "realized_pnl": "本单已实现盈亏",
    "pnl_source": "盈亏来源",
    "account_equity": "合约账户权益",
    "account_available": "合约可用余额",
    "account_margin_used": "已用保证金",
    "position_side": "仓位方向",
    "position_size": "仓位数量",
    "entry_times": "建仓次数",
    "add_times": "补仓次数",
    "reduce_times": "减仓次数",
    "qty": "数量",
    "quantity": "数量",
    "size": "仓位数量",
    "status": "订单状态",
    "filled": "已成交数量",
    "filled_delta": "本次新增成交量",
    "avg_price": "成交均价",
    "tp_count": "止盈提交数",
    "tp_total": "止盈目标总数",
    "failed_count": "失败数量",
    "placed": "成功数量",
    "skipped": "跳过数量",
    "total_size": "对应仓位数量",
    "source": "来源",
    "mode": "模式",
    "elapsed_ms": "耗时(毫秒)",
    "loss_pct": "亏损比例(%)",
    "threshold_pct": "告警阈值(%)",
    "cross_seq": "跨阈值序号",
}


class SMTPAlertSender:
    _MAX_SEND_RETRIES = 2
    _RETRY_BACKOFF_SECONDS = 1.0

    def __init__(self, config: EmailAlertConfig) -> None:
        self.config = config
        self._last_sent_at_by_key: dict[str, float] = {}
        self._active_incident_keys: set[str] = set()

    def should_send(
        self,
        event_type: str,
        payload: dict[str, Any] | None = None,
        msg: str | None = None,
        level: str | None = None,
    ) -> bool:
        if not self.config.enabled:
            return False
        if self._is_cross_margin_event(event_type=event_type, payload=payload):
            return False
        allowed = {item.strip() for item in self.config.send_on if item.strip()}
        if not allowed:
            return False
        if event_type not in allowed:
            incident_type = _RECOVERY_TO_INCIDENT.get(event_type)
            if incident_type is None or incident_type not in allowed:
                return False
        if event_type == "HIGH_LEVERAGE":
            leverage = self._extract_leverage(payload)
            if leverage is not None and leverage <= 60:
                return False
        incident_action, incident_key = self._classify_incident(
            event_type=event_type,
            level=level,
            payload=payload,
        )
        if incident_action == "activate" and incident_key in self._active_incident_keys:
            return False
        if incident_action == "recover" and incident_key not in self._active_incident_keys:
            return False
        if incident_action == "none" and self.config.dedupe_seconds > 0:
            now = time.time()
            key = self._build_dedupe_key(event_type=event_type, payload=payload, msg=msg)
            last = self._last_sent_at_by_key.get(key)
            if last is not None and (now - last) < float(self.config.dedupe_seconds):
                return False
        return True

    def send(
        self,
        *,
        event_type: str,
        level: str,
        msg: str,
        trace_id: str,
        payload: dict[str, Any] | None,
    ) -> None:
        if not self.should_send(event_type, payload, msg, level=level):
            return
        if not self.config.smtp_host or not self.config.to_addrs:
            return

        event_label = _EVENT_LABELS.get(event_type, event_type)
        email_msg = EmailMessage()
        email_msg["Subject"] = f"[Following][{level}] {event_label}"
        email_msg["From"] = self.config.from_addr or self.config.smtp_user
        email_msg["To"] = ", ".join(self.config.to_addrs)
        email_msg.set_content(self._render_email_text(event_type, level, msg, trace_id, payload))

        password = os.getenv(self.config.smtp_pass_env, "")
        last_error: Exception | None = None
        for attempt in range(self._MAX_SEND_RETRIES + 1):
            try:
                with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port, timeout=10) as smtp:
                    smtp.ehlo()
                    try:
                        smtp.starttls()
                        smtp.ehlo()
                    except Exception:
                        pass
                    if self.config.smtp_user:
                        smtp.login(self.config.smtp_user, password)
                    smtp.send_message(email_msg)
                last_error = None
                break
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt >= self._MAX_SEND_RETRIES:
                    raise
                time.sleep(self._RETRY_BACKOFF_SECONDS * (2**attempt))
        if last_error is not None:
            raise last_error
        incident_action, incident_key = self._classify_incident(
            event_type=event_type,
            level=level,
            payload=payload,
        )
        if incident_action == "activate":
            self._active_incident_keys.add(incident_key)
        elif incident_action == "recover":
            self._active_incident_keys.discard(incident_key)
        if incident_action == "none" and self.config.dedupe_seconds > 0:
            key = self._build_dedupe_key(event_type=event_type, payload=payload, msg=msg)
            self._last_sent_at_by_key[key] = time.time()

    @staticmethod
    def _is_cross_margin_event(*, event_type: str, payload: dict[str, Any] | None) -> bool:
        if event_type == "CROSS_MARGIN":
            return True
        if not payload:
            return False
        for key in ("margin_mode", "mode"):
            raw = payload.get(key)
            if raw in (None, ""):
                continue
            normalized = str(raw).strip().lower()
            if normalized in {"cross", "crossed", "全仓", "全倉"}:
                return True
        return False

    def _extract_leverage(self, payload: dict[str, Any] | None) -> float | None:
        if not payload:
            return None
        raw = payload.get("leverage")
        if raw in (None, ""):
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None

    def _render_email_text(
        self,
        event_type: str,
        level: str,
        msg: str,
        trace_id: str,
        payload: dict[str, Any] | None,
    ) -> str:
        event_label = _EVENT_LABELS.get(event_type, event_type)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        lines = [
            "Following 交易风控提醒",
            f"事件：{event_label}（{event_type}）",
            f"级别：{level}",
            f"时间：{now}",
            f"摘要：{msg}",
            f"追踪ID：{trace_id}",
        ]
        if payload:
            lines.append("")
            lines.append("关键信息：")
            for key, value in payload.items():
                if value in (None, ""):
                    continue
                label = _PAYLOAD_LABELS.get(key, key)
                lines.append(f"- {label}: {value}")
        lines.extend(
            [
                "",
                "请手动登录服务器核查：",
                "1) run.out 日志",
                "2) trader.db 执行记录",
                "3) Bitget 挂单/持仓/止损状态",
            ]
        )
        return "\n".join(lines)

    @staticmethod
    def _build_dedupe_key(event_type: str, payload: dict[str, Any] | None, msg: str | None) -> str:
        if not payload:
            return f"{event_type}|{msg or ''}"
        if event_type == "API_ERROR_BURST":
            return (
                f"{event_type}|{payload.get('purpose') or ''}|"
                f"{payload.get('reason') or ''}|{payload.get('window_seconds') or ''}"
            )
        volatile_keys = {"ts", "time", "timestamp", "elapsed_ms", "count", "retry"}
        stable = {
            key: value
            for key, value in payload.items()
            if key not in volatile_keys
        }
        normalized = json.dumps(stable, ensure_ascii=False, sort_keys=True, default=str)
        return f"{event_type}|{msg or ''}|{normalized}"

    def _classify_incident(
        self,
        *,
        event_type: str,
        level: str | None,
        payload: dict[str, Any] | None,
    ) -> tuple[str, str]:
        incident_type = _RECOVERY_TO_INCIDENT.get(event_type)
        if incident_type is not None:
            return "recover", self._build_incident_key(incident_type, payload)
        if str(level or "").upper() in {"WARN", "ERROR", "CRITICAL"}:
            return "activate", self._build_incident_key(event_type, payload)
        return "none", ""

    @staticmethod
    def _build_incident_key(event_type: str, payload: dict[str, Any] | None) -> str:
        if not payload:
            return event_type
        volatile_keys = {
            "ts",
            "time",
            "timestamp",
            "elapsed_ms",
            "count",
            "retry",
            "trace_id",
            "reason",
            "error",
            "status",
        }
        stable = {key: value for key, value in payload.items() if key not in volatile_keys}
        normalized = json.dumps(stable, ensure_ascii=False, sort_keys=True, default=str)
        return f"{event_type}|{normalized}"


_RECOVERY_TO_INCIDENT = {
    "API_ERROR_BURST_RECOVERED": "API_ERROR_BURST",
    "UNKNOWN_POSITION_RECOVERED": "UNKNOWN_POSITION",
    "MARGIN_USED_HIGH_RECOVERED": "MARGIN_USED_HIGH",
    "SL_MISSING_RECOVERED": "SL_MISSING",
    "PRICE_FEED_ERROR_RECOVERED": "PRICE_FEED_ERROR",
    "PRICE_FEED_WS_RECONNECT_RECOVERED": "PRICE_FEED_WS_RECONNECT",
}
