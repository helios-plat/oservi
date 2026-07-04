"""Tests for oservi.manifest ServiceManifest.depends_on (aegis DESIGN §10.3)."""

from __future__ import annotations

import pytest

from oservi.manifest import ServiceManifest


def _mk(**over):
    base = {"name": "svc", "skeleton": "alerter", "inject": {}, "trigger": {}}
    base.update(over)
    return ServiceManifest(**base)


class TestServiceManifestDependsOn:
    def test_defaults_to_empty_list(self):
        assert _mk().depends_on == []

    def test_declared_dependencies_preserved(self):
        m = _mk(depends_on=["postgres-main", "tide-collector"])
        assert m.depends_on == ["postgres-main", "tide-collector"]

    def test_non_list_rejected(self):
        with pytest.raises(TypeError):
            _mk(depends_on="postgres-main")

    def test_backward_compat_construct_without_depends_on(self):
        # 既有构造(不传 depends_on)仍有效,其它字段行为不变
        m = ServiceManifest(name="x", skeleton="s", inject={}, trigger={"on_interval": 300})
        assert m.depends_on == []
        assert m.trigger == {"on_interval": 300}

    def test_existing_validation_still_enforced(self):
        with pytest.raises(ValueError):
            _mk(name="")
