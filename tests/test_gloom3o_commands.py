"""Gloom 3O command parsing, dispatch, and CLI tests."""

from __future__ import annotations

import asyncio
import json

import pytest

from oservi.cli import main
from oservi.gloom3o.commands import (
    COMMANDS,
    CommandDispatcher,
    CommandError,
    CommandRequest,
    CommandUnavailable,
    _helios_api_handler,
    _kelly_handler,
    _risk_handler,
    create_default_dispatcher,
    parse_command,
)


def test_command_set_contains_all_required_routes() -> None:
    assert COMMANDS == ("QUOTE", "DES", "GP", "OPX", "KELLY", "RISK", "PORT", "FUSION", "HELP")


def test_parser_normalizes_command_and_preserves_positionals() -> None:
    request = parse_command('quote "BTC-USD" exchange=okx')
    assert request == CommandRequest("QUOTE", ("BTC-USD",), {"exchange": "okx"})


def test_parser_supports_long_flags_with_separate_and_inline_values() -> None:
    request = parse_command("KELLY --win-probability 0.6 --win-loss-ratio=2")
    assert request.options == {"win_probability": "0.6", "win_loss_ratio": "2"}


@pytest.mark.parametrize("text", ["", "   "])
def test_parser_rejects_empty_command(text: str) -> None:
    with pytest.raises(CommandError, match="empty"):
        parse_command(text)


def test_parser_rejects_unclosed_quotes() -> None:
    with pytest.raises(CommandError, match="quoting"):
        parse_command('QUOTE "BTC')


def test_parser_rejects_unknown_command() -> None:
    with pytest.raises(CommandError, match="unsupported"):
        parse_command("UNKNOWN value")


def test_parser_rejects_duplicate_options() -> None:
    with pytest.raises(CommandError, match="duplicate"):
        parse_command("KELLY p=0.5 p=0.6")


def test_dispatcher_validates_handler_registry() -> None:
    with pytest.raises(CommandError, match="unsupported"):
        CommandDispatcher({"NOT_A_COMMAND": lambda _: None})
    with pytest.raises(TypeError, match="callable"):
        CommandDispatcher({"QUOTE": None})


def test_dispatcher_invokes_sync_handler_with_parsed_request() -> None:
    dispatcher = CommandDispatcher({"QUOTE": lambda request: {"symbol": request.arguments[0]}})
    result = asyncio.run(dispatcher.dispatch("QUOTE BTC-USD"))
    assert result == {"symbol": "BTC-USD"}


def test_dispatcher_invokes_and_awaits_async_handler() -> None:
    async def handler(request: CommandRequest) -> dict[str, str]:
        return {"routed": request.name}

    result = asyncio.run(CommandDispatcher({"DES": handler}).dispatch("DES BTC"))
    assert result == {"routed": "DES"}


def test_dispatcher_reports_missing_live_handler_without_fabricating_data() -> None:
    with pytest.raises(CommandUnavailable, match="no live handler"):
        asyncio.run(CommandDispatcher().dispatch("QUOTE BTC"))


def test_injected_handlers_route_every_supported_command() -> None:
    handlers = {name: (lambda request, command=name: command) for name in COMMANDS}
    dispatcher = CommandDispatcher(handlers)
    for name in COMMANDS:
        assert asyncio.run(dispatcher.dispatch(name)) == name


def test_default_help_lists_all_routes() -> None:
    result = asyncio.run(create_default_dispatcher().dispatch("HELP"))
    assert result["commands"] == list(COMMANDS)
    assert set(result["syntax"]) == set(COMMANDS)


def test_kelly_handler_requires_inputs_and_uses_oprim_calculation() -> None:
    with pytest.raises(CommandError, match="requires p"):
        _kelly_handler(parse_command("KELLY"))
    result = _kelly_handler(parse_command("KELLY p=0.5 b=1 fraction=0.5"))
    assert result["kelly_fraction"] == 0.0


def test_risk_handler_uses_only_supplied_returns_and_optional_equity() -> None:
    result = _risk_handler(parse_command("RISK returns=-0.1,-0.05,0,0.02 confidence=0.75"))
    assert result["method"] == "historical"
    assert result["var"] == pytest.approx(0.0625)
    assert "maximum_drawdown" not in result


def test_risk_handler_computes_drawdown_from_explicit_equity_curve() -> None:
    result = _risk_handler(parse_command(
        "RISK returns=-0.1,-0.05,0,0.02 confidence=0.75 equity=100,120,90"
    ))
    assert result["maximum_drawdown"] == pytest.approx(0.25)


def test_risk_handler_rejects_unsupported_method() -> None:
    with pytest.raises(CommandError, match="method must"):
        _risk_handler(parse_command("RISK returns=-0.1,0.1 method=guess"))


def test_remote_handler_requires_explicit_helios_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GLOOM3O_HELIOS_URL", raising=False)
    with pytest.raises(CommandUnavailable, match="GLOOM3O_HELIOS_URL"):
        asyncio.run(_helios_api_handler(parse_command("QUOTE BTC")))


def test_cli_help_prints_machine_readable_routes(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["HELP"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["commands"] == list(COMMANDS)


def test_cli_without_live_quote_configuration_fails_not_fake_success(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("GLOOM3O_HELIOS_URL", raising=False)
    assert main(["QUOTE", "BTC"]) == 2
    error = json.loads(capsys.readouterr().err)
    assert "GLOOM3O_HELIOS_URL" in error["error"]


def test_cli_kelly_returns_computed_result(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["KELLY", "p=0.6", "b=2"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["kelly_fraction"] == pytest.approx(0.4)
