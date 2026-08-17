"""oservi facade for omodul.SandboxBroker. oservi may import omodul; reverse is forbidden."""

from __future__ import annotations

from omodul.sandbox_broker import (  # noqa: F401
    DEFAULT_SLOTS,
    SandboxBroker,
    get_broker,
    reset_broker,
    set_broker,
)
