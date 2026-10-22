"""Console entry point for the Gloom 3O command interface."""

from __future__ import annotations

import asyncio
import json
import shlex
import sys
from collections.abc import Sequence
from typing import Any

from oservi.gloom3o.commands import (
    CommandError,
    CommandUnavailable,
    create_default_dispatcher,
    parse_command,
)


def _json_default(value: Any) -> str:
    return str(value)


def main(argv: Sequence[str] | None = None) -> int:
    """Run one command and print its actual JSON response to stdout."""
    tokens = list(sys.argv[1:] if argv is None else argv)
    if not tokens or tokens[0].lower() in ("-h", "--help"):
        tokens = ["HELP"]
    command_line = " ".join(shlex.quote(token) for token in tokens)
    try:
        parsed = parse_command(command_line)
        result = asyncio.run(create_default_dispatcher().dispatch(parsed))
    except (CommandError, CommandUnavailable, ValueError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, default=_json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
