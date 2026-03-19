import os

from trader.kill_switch import KillSwitch, KillSwitchAction
from trader.store import SQLiteStore


def test_kill_switch_supports_file_env_and_sqlite(tmp_path, monkeypatch) -> None:
    store = SQLiteStore(str(tmp_path / "kill.db"))

    # file trigger
    kill_file = tmp_path / "KILL_SWITCH"
    kill_file.write_text("panic", encoding="utf-8")
    ks_file = KillSwitch(store=store, file_path=str(kill_file))
    assert ks_file.read_action() == KillSwitchAction.PANIC_CLOSE

    # env trigger
    kill_file.unlink()
    monkeypatch.setenv("TRADER_KILL_SWITCH", "1")
    ks_env = KillSwitch(store=store, file_path=str(kill_file))
    assert ks_env.read_action() == KillSwitchAction.SAFE_MODE

    # sqlite trigger
    monkeypatch.delenv("TRADER_KILL_SWITCH", raising=False)
    store.set_system_flag("kill_switch", "safe")
    ks_sql = KillSwitch(store=store, file_path=str(kill_file))
    assert ks_sql.read_action() == KillSwitchAction.SAFE_MODE
