"""Minimal Hermes Dev Bridge plugin."""
from .tools import bridge_smoke

_SCHEMA = {
    "name": "bridge_smoke",
    "description": "Submit a fixed smoke acknowledgement request; no fixture is executed.",
    "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
}


def register(ctx) -> None:
    ctx.register_tool(
        name="bridge_smoke",
        toolset="hermes_dev_bridge",
        schema=_SCHEMA,
        handler=bridge_smoke,
        description="Fixed acknowledgement-only Hermes development bridge smoke check.",
    )
