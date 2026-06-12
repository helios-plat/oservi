"""AppInstallerEngine 测试 — ≥10 tests."""

from __future__ import annotations

import asyncio
import pytest

from oservi import AppInstallerEngine, list_skeletons


# ===== Fake injectables =====


def fake_catalog(*, app_id):
    return {
        "image": f"registry.example.com/{app_id}:latest",
        "compose_file": f"/apps/{app_id}/docker-compose.yml",
        "env_vars": {"APP_PORT": "8080"},
        "routes": [{"host": f"{app_id}.example.com", "upstream": "localhost:8080"}],
        "service_url": f"http://localhost:8080/health",
    }


def fake_catalog_error(*, app_id):
    raise RuntimeError(f"catalog not found: {app_id}")


def fake_compose_pull(*, compose_file):
    return {"status": "pulled", "compose_file": compose_file}


def fake_compose_up(*, compose_file, env):
    return {"status": "up", "compose_file": compose_file}


def fake_compose_up_error(*, compose_file, env):
    raise RuntimeError("compose up failed")


def fake_caddy_route_add(*, routes):
    return {"added": len(routes)}


def fake_verify_health_ok(*, service_url, retries):
    return True


def fake_verify_health_fail(*, service_url, retries):
    return False


# ===== Tests =====


def _make_engine(**overrides):
    defaults = dict(
        catalog_fetch=fake_catalog,
        compose_up=fake_compose_up,
        compose_pull=fake_compose_pull,
        caddy_route_add=fake_caddy_route_add,
        verify_health=fake_verify_health_ok,
        trigger={"on_interval": 0},
        config={},
        name="test-installer",
    )
    defaults.update(overrides)
    return AppInstallerEngine(**defaults)


def test_app_installer_registered():
    assert "app_installer" in list_skeletons()


def test_app_installer_instantiates():
    engine = _make_engine()
    assert engine.name == "test-installer"
    assert engine._running is False


def test_app_installer_injection_points():
    pts = AppInstallerEngine.injection_points
    assert pts["catalog_fetch"].kind == "oprim"
    assert pts["compose_up"].kind == "oprim"
    assert pts["compose_pull"].kind == "oprim"
    assert pts["caddy_route_add"].kind == "oskill"
    assert pts["verify_health"].kind == "oskill"


def test_install_app_success():
    engine = _make_engine()
    result = asyncio.run(engine._install_app({"app_id": "myapp"}))
    assert result["status"] == "installed"
    assert result["app_id"] == "myapp"
    steps = [t["step"] for t in result["trail"]]
    assert "catalog_fetch" in steps
    assert "compose_up" in steps
    assert "verify_health" in steps


def test_install_app_catalog_error_returns_failed():
    engine = _make_engine(catalog_fetch=fake_catalog_error)
    result = asyncio.run(engine._install_app({"app_id": "missing"}))
    assert result["status"] == "failed"
    assert result["failed_step"] == "catalog_fetch"
    assert "ERROR" in result["error"]


def test_install_app_compose_up_error_returns_failed():
    engine = _make_engine(compose_up=fake_compose_up_error)
    result = asyncio.run(engine._install_app({"app_id": "myapp"}))
    assert result["status"] == "failed"
    assert result["failed_step"] == "compose_up"


def test_install_app_unhealthy_returns_installed_unhealthy():
    engine = _make_engine(verify_health=fake_verify_health_fail)
    result = asyncio.run(engine._install_app({"app_id": "myapp"}))
    assert result["status"] == "installed_unhealthy"


def test_install_app_skip_pull():
    pull_called = []

    def tracking_pull(*, compose_file):
        pull_called.append(True)
        return {}

    engine = _make_engine(
        compose_pull=tracking_pull,
        config={"skip_pull": True},
    )
    asyncio.run(engine._install_app({"app_id": "myapp"}))
    assert pull_called == []


def test_install_app_includes_compose_pull_by_default():
    pull_called = []

    def tracking_pull(*, compose_file):
        pull_called.append(True)
        return {}

    engine = _make_engine(compose_pull=tracking_pull, config={})
    asyncio.run(engine._install_app({"app_id": "myapp"}))
    assert pull_called == [True]


def test_submit_install_enqueues():
    engine = _make_engine()
    engine.submit_install({"app_id": "test"})
    assert engine._install_queue.qsize() == 1


def test_stop_sets_not_running():
    engine = _make_engine()
    engine._running = True
    engine.stop()
    assert engine._running is False


def test_health_stopped():
    engine = _make_engine()
    h = engine.health()
    assert h["status"] == "stopped"
    assert h["details"]["installs_done"] == 0


def test_on_install_done_callback():
    done = []
    engine = _make_engine(on_install_done=done.append)
    result = asyncio.run(engine._install_app({"app_id": "myapp"}))
    engine.on_install_done(result)
    assert len(done) == 1
    assert done[0]["status"] == "installed"


def test_trail_has_all_steps():
    engine = _make_engine()
    result = asyncio.run(engine._install_app({"app_id": "myapp"}))
    step_names = [t["step"] for t in result["trail"]]
    assert "catalog_fetch" in step_names
    assert "compose_pull" in step_names
    assert "compose_up" in step_names
    assert "caddy_route_add" in step_names
    assert "verify_health" in step_names
