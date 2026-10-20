"""Gloom 3O command grammar and injectable dispatch layer.

This is command-level composition, not an oservi engine skeleton: market,
risk, and Helios API behavior is supplied through handlers. Missing live data
handlers fail explicitly rather than returning placeholder prices or balances.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import shlex
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

COMMANDS = ("QUOTE", "DES", "GP", "OPX", "KELLY", "RISK", "PORT", "FUSION", "HELP")

COMMAND_HELP = {
    "QUOTE": "QUOTE <symbol> [exchange=okx] — public quote via configured Helios API",
    "DES": "DES <symbol> [expiry=YYYY-MM-DD] — derivatives summary via Helios API",
    "GP": "GP <symbol> [period=...] — Gloom profile via Helios API",
    "OPX": "OPX <underlying> [exchange=deribit|okx|all] — public options chain indicators",
    "KELLY": "KELLY p=<probability> b=<win/loss> [fraction=<0..1>] [max_fraction=<0..1>]",
    "RISK": "RISK returns=<comma-separated returns> [method=historical|parametric] [equity=...]",
    "PORT": "PORT [account=...] — read-only portfolio via configured Helios API",
    "FUSION": "FUSION <symbol> — Fusion data via configured Helios API",
    "HELP": "HELP — show this command list",
}


class CommandError(ValueError):
    """Raised for malformed or unsupported command input."""


class CommandUnavailable(RuntimeError):
    """Raised when a valid command has no configured live data handler."""


@dataclass(frozen=True)
class CommandRequest:
    """Parsed command name, positional arguments, and string-valued options."""

    name: str
    arguments: tuple[str, ...]
    options: dict[str, str]


CommandHandler = Callable[[CommandRequest], Any]


def parse_command(text: str) -> CommandRequest:
    """Parse a command line with shell quoting, positional args, and key/value flags."""
    if not text or not text.strip():
        raise CommandError("command is empty")
    try:
        tokens = shlex.split(text)
    except ValueError as exc:
        raise CommandError(f"invalid quoting: {exc}") from exc
    if not tokens:
        raise CommandError("command is empty")

    name = tokens[0].upper()
    if name not in COMMANDS:
        raise CommandError(f"unsupported command {tokens[0]!r}; use HELP to list supported commands")

    arguments: list[str] = []
    options: dict[str, str] = {}
    index = 1
    while index < len(tokens):
        token = tokens[index]
        if token.startswith("--"):
            key_value = token[2:]
            if not key_value:
                raise CommandError("option name after '--' is empty")
            if "=" in key_value:
                key, value = key_value.split("=", 1)
            else:
                key = key_value
                if index + 1 < len(tokens) and "=" not in tokens[index + 1] and not tokens[index + 1].startswith("--"):
                    index += 1
                    value = tokens[index]
                else:
                    value = "true"
        elif "=" in token:
            key, value = token.split("=", 1)
        else:
            arguments.append(token)
            index += 1
            continue

        normalized_key = key.strip().lower().replace("-", "_")
        if not normalized_key:
            raise CommandError("option name must not be empty")
        if normalized_key in options:
            raise CommandError(f"duplicate option {normalized_key!r}")
        options[normalized_key] = value
        index += 1

    return CommandRequest(name=name, arguments=tuple(arguments), options=options)


class CommandDispatcher:
    """Route parsed commands to injected sync or async handlers."""

    def __init__(self, handlers: Mapping[str, CommandHandler] | None = None) -> None:
        self._handlers: dict[str, CommandHandler] = {"HELP": _help_handler}
        for name, handler in (handlers or {}).items():
            normalized_name = name.upper()
            if normalized_name not in COMMANDS:
                raise CommandError(f"cannot register unsupported command {name!r}")
            if not callable(handler):
                raise TypeError(f"handler for {normalized_name} must be callable")
            self._handlers[normalized_name] = handler

    async def dispatch(self, command: CommandRequest | str) -> Any:
        """Invoke a command handler and await it when the handler is asynchronous."""
        request = parse_command(command) if isinstance(command, str) else command
        handler = self._handlers.get(request.name)
        if handler is None:
            raise CommandUnavailable(f"no live handler configured for {request.name}")
        result = handler(request)
        return await result if inspect.isawaitable(result) else result


def _help_handler(_: CommandRequest) -> dict[str, Any]:
    return {"commands": list(COMMANDS), "syntax": COMMAND_HELP}


def _required_option(request: CommandRequest, *names: str) -> str:
    for name in names:
        value = request.options.get(name)
        if value is not None and value.strip():
            return value
    raise CommandError(f"{request.name} requires {names[0]}=<value>")


def _required_argument_or_option(request: CommandRequest, option: str) -> str:
    value = request.options.get(option)
    if value:
        return value
    if request.arguments:
        return request.arguments[0]
    raise CommandError(f"{request.name} requires <{option}> or {option}=<value>")


def _parse_float(request: CommandRequest, value: str, name: str) -> float:
    try:
        return float(value)
    except ValueError as exc:
        raise CommandError(f"{name} must be numeric") from exc


def _parse_float_list(request: CommandRequest, value: str, name: str) -> list[float]:
    try:
        items = [float(part.strip()) for part in value.split(",") if part.strip()]
    except ValueError as exc:
        raise CommandError(f"{name} must be a comma-separated numeric list") from exc
    if not items:
        raise CommandError(f"{name} must contain at least one observation")
    return items


def _kelly_handler(request: CommandRequest) -> dict[str, float]:
    from oprim.risk.kelly_position import calculate_kelly_criterion

    probability = _parse_float(request, _required_option(request, "p", "win_probability"), "p")
    odds = _parse_float(request, _required_option(request, "b", "win_loss_ratio"), "b")
    fraction = _parse_float(request, request.options.get("fraction", "1"), "fraction")
    cap = _parse_float(request, request.options.get("max_fraction", "1"), "max_fraction")
    return {
        "kelly_fraction": calculate_kelly_criterion(
            probability, odds, fraction, max_fraction=cap
        )
    }


def _risk_handler(request: CommandRequest) -> dict[str, Any]:
    from oprim.risk.kelly_position import (
        historical_cvar,
        historical_var,
        maximum_drawdown,
        parametric_cvar,
        parametric_var,
    )

    returns = _parse_float_list(request, _required_option(request, "returns"), "returns")
    confidence = _parse_float(request, request.options.get("confidence", "0.95"), "confidence")
    method = request.options.get("method", "historical").lower()
    if method == "historical":
        var_value = historical_var(returns, confidence=confidence)
        cvar_value = historical_cvar(returns, confidence=confidence)
    elif method == "parametric":
        var_value = parametric_var(returns, confidence=confidence)
        cvar_value = parametric_cvar(returns, confidence=confidence)
    else:
        raise CommandError("method must be 'historical' or 'parametric'")
    result: dict[str, Any] = {"method": method, "confidence": confidence, "var": var_value, "cvar": cvar_value}
    equity_value = request.options.get("equity")
    if equity_value:
        result["maximum_drawdown"] = maximum_drawdown(
            _parse_float_list(request, equity_value, "equity")
        )
    return result


async def _options_handler(request: CommandRequest) -> dict[str, Any]:
    from obase.options_marketdata import OptionsMarketDataAdapter
    from oprim.derivatives.analytics import analyze_option_chain

    underlying = _required_argument_or_option(request, "underlying")
    selected_exchange = request.options.get("exchange", "all").lower()
    if selected_exchange not in ("all", "deribit", "okx"):
        raise CommandError("exchange must be 'deribit', 'okx', or 'all'")
    exchanges = ("deribit", "okx") if selected_exchange == "all" else (selected_exchange,)
    adapter = OptionsMarketDataAdapter()
    output = {}
    for exchange in exchanges:
        snapshot = await adapter.fetch_options(exchange, underlying=underlying)
        output[exchange] = {
            "provider_timestamp_ms": snapshot["provider_timestamp"],
            "spot": snapshot["spot"],
            "instrument_count": len(snapshot["options"]),
            **analyze_option_chain(snapshot),
        }
    return {"underlying": underlying.upper(), "exchanges": output}


async def _helios_api_handler(request: CommandRequest) -> Any:
    base_url = os.environ.get("GLOOM3O_HELIOS_URL", "").rstrip("/")
    if not base_url:
        raise CommandUnavailable(
            f"{request.name} needs GLOOM3O_HELIOS_URL; no live data source is configured"
        )
    url = f"{base_url}/api/gloom/commands"
    body = {
        "command": request.name,
        "arguments": list(request.arguments),
        "options": request.options,
    }

    def post_command() -> Any:
        request_data = urllib.request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request_data, timeout=15.0) as response:
            return json.loads(response.read().decode("utf-8"))

    try:
        return await asyncio.to_thread(post_command)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise CommandUnavailable(f"Helios command API request failed: {exc}") from exc


def create_default_dispatcher() -> CommandDispatcher:
    """Build the CLI handler map using live API endpoints and pure risk functions."""
    remote_commands = ("QUOTE", "DES", "GP", "PORT", "FUSION")
    handlers: dict[str, CommandHandler] = {
        "KELLY": _kelly_handler,
        "RISK": _risk_handler,
        "OPX": _options_handler,
    }
    handlers.update({name: _helios_api_handler for name in remote_commands})
    return CommandDispatcher(handlers)
