"""Minimal Hermes Dev Bridge plugin."""
from .tools import bridge_execute_smoke, bridge_pfy_preflight, bridge_smoke

_ACK_SCHEMA = {
    "name": "bridge_smoke",
    "description": "Submit a fixed smoke acknowledgement request; no fixture is executed.",
    "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
}

_EXEC_SCHEMA = {
    "name": "bridge_execute_smoke",
    "description": "Request execution of the fixed smoke fixture through the authenticated bridge; operator authorization required.",
    "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
}

_PFY_SCHEMA = {
    "name": "bridge_pfy_preflight",
    "description": "Read the operator-controlled PFY readiness snapshot from the authenticated bridge; no repository access and no execution.",
    "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
}


def register(ctx) -> None:
    ctx.register_tool(
        name="bridge_smoke",
        toolset="hermes_dev_bridge",
        schema=_ACK_SCHEMA,
        handler=bridge_smoke,
        description="Fixed acknowledgement-only Hermes development bridge smoke check.",
    )
    ctx.register_tool(
        name="bridge_execute_smoke",
        toolset="hermes_dev_bridge",
        schema=_EXEC_SCHEMA,
        handler=bridge_execute_smoke,
        description="Request execution of the fixed smoke fixture through the authenticated bridge.",
    )
    ctx.register_tool(
        name="bridge_pfy_preflight",
        toolset="hermes_dev_bridge",
        schema=_PFY_SCHEMA,
        handler=bridge_pfy_preflight,
        description="Read the operator-controlled PFY readiness snapshot through the authenticated bridge.",
    )
