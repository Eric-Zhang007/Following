from __future__ import annotations

from trader.alerts import AlertManager
from trader.bitget_client import BitgetClient
from trader.config import AppConfig
from trader.state import StateStore


def probe_plan_order_capability_on_startup(
    *,
    config: AppConfig,
    bitget: BitgetClient,
    alerts: AlertManager,
    runtime_state: StateStore,
) -> None:
    if not config.bitget.plan_orders_probe_on_startup:
        return

    state = bitget.probe_plan_orders_capability(force=True)
    supported = state.get("supported")
    reason = str(state.get("reason") or "unknown")
    if state.get("expires_at") and state.get("ts"):
        ttl = max(0, int(float(state.get("expires_at", 0.0)) - float(state.get("ts", 0.0))))
    else:
        ttl = None

    payload = {
        "supported": supported,
        "reason": reason,
        "ttl_seconds": ttl,
        "sl_order_type": config.risk.stoploss.sl_order_type,
    }
    if supported is True:
        alerts.info("PLAN_ORDER_CAPABILITY_PROBE", "plan order capability probe succeeded", payload)
        return

    level_emit = alerts.error if supported is False else alerts.warn
    level_emit("PLAN_ORDER_CAPABILITY_PROBE", "plan order capability probe did not confirm support", payload)

    if supported is not False:
        return
    if config.risk.stoploss.sl_order_type not in {"trigger", "plan"}:
        return

    old_mode = config.risk.stoploss.sl_order_type
    config.risk.stoploss.sl_order_type = "local_guard"
    alerts.error(
        "PLAN_ORDER_FALLBACK",
        "plan orders unsupported; runtime fallback to local_guard",
        {
            "reason": reason,
            "previous_mode": old_mode,
            "new_mode": "local_guard",
        },
    )

    if config.bitget.plan_orders_probe_safe_mode_on_failure:
        runtime_state.enable_safe_mode("plan order capability unsupported")
        alerts.error(
            "PLAN_ORDER_FALLBACK",
            "safe_mode enabled because plan order capability unsupported",
            {
                "reason": reason,
                "safe_mode": True,
            },
        )
