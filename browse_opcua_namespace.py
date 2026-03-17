#!/usr/bin/env python3
"""
browse_opcua_namespace.py  —  Browse all nodes in an OPC UA server.

Usage:
    python3 browse_opcua_namespace.py opc.tcp://localhost:4840/nectarcam/
    python3 browse_opcua_namespace.py opc.tcp://localhost:4840/ --ns 2
    python3 browse_opcua_namespace.py opc.tcp://localhost:4840/ --user admin --password secret
"""

import argparse
import asyncio
import sys

try:
    from asyncua import Client, ua
except ImportError:
    sys.exit("asyncua not installed — run: pip install asyncua")

# ── ANSI colours (disabled on Windows / non-tty) ─────────────────────────
import os
_USE_COLOUR = sys.stdout.isatty() and os.name != "nt"

def _c(code, text):
    return f"\033[{code}m{text}\033[0m" if _USE_COLOUR else text

BOLD   = lambda t: _c("1",     t)
DIM    = lambda t: _c("2",     t)
CYAN   = lambda t: _c("36",    t)
GREEN  = lambda t: _c("32",    t)
YELLOW = lambda t: _c("33",    t)
BLUE   = lambda t: _c("34",    t)
RED    = lambda t: _c("31",    t)


# ── Node-class helpers ────────────────────────────────────────────────────

def _nc_label(node_class):
    return {
        ua.NodeClass.Object:    CYAN("Object"),
        ua.NodeClass.Variable:  GREEN("Variable"),
        ua.NodeClass.Method:    YELLOW("Method"),
        ua.NodeClass.ObjectType: DIM("ObjectType"),
        ua.NodeClass.VariableType: DIM("VariableType"),
        ua.NodeClass.DataType:  DIM("DataType"),
        ua.NodeClass.View:      DIM("View"),
    }.get(node_class, DIM(str(node_class)))


# OPC UA built-in type IDs (Part 3 / Part 6 of the spec)
_TYPE_NAMES = {
    0:  "Null",      1:  "Boolean",   2:  "SByte",     3:  "Byte",
    4:  "Int16",     5:  "UInt16",    6:  "Int32",      7:  "UInt32",
    8:  "Int64",     9:  "UInt64",    10: "Float",      11: "Double",
    12: "String",    13: "DateTime",  14: "Guid",       15: "ByteString",
    16: "XmlElement",17: "NodeId",    18: "ExpandedNodeId",
    19: "StatusCode",20: "QualName",  21: "LocalText",  22: "ExtObject",
    23: "DataValue", 24: "Variant",   25: "DiagInfo",
}

def _type_name(identifier):
    """Return a human-readable type name for an OPC UA built-in type identifier."""
    if isinstance(identifier, int):
        return _TYPE_NAMES.get(identifier, str(identifier))
    # Non-integer identifiers are custom DataTypes — show as-is
    return str(identifier)


async def _read_value(node):
    """Return a short string representation of a variable's current value."""
    try:
        val  = await node.read_value()
        vtype = await node.read_data_type()
        tname = _type_name(vtype.Identifier)
        s = repr(val)
        # Don't truncate — show full value; long lists will wrap naturally
        return f"{DIM(tname)}  {s}"
    except Exception as exc:
        return RED(f"<read error: {exc}>")


async def _read_method_args(node):
    """Return (in_args, out_args) as short description strings."""
    try:
        in_args, out_args = [], []
        for child in await node.get_children():
            bn = (await child.read_browse_name()).Name
            if bn == "InputArguments":
                for a in await child.read_value():
                    in_args.append(f"{a.Name}:{_type_name(a.DataType.Identifier)}")
            elif bn == "OutputArguments":
                for a in await child.read_value():
                    out_args.append(f"{a.Name}:{_type_name(a.DataType.Identifier)}")
        return in_args, out_args
    except Exception:
        return [], []


# ── Recursive browser ─────────────────────────────────────────────────────

async def browse(node, prefix="", ns_filter=None, show_values=True):
    """Recursively print the node tree rooted at *node*."""
    try:
        children = await node.get_children()
    except Exception as exc:
        print(prefix + RED(f"  [browse error: {exc}]"))
        return

    for i, child in enumerate(children):
        is_last = (i == len(children) - 1)
        branch  = "└── " if is_last else "├── "
        cont    = "    " if is_last else "│   "

        try:
            bn         = await child.read_browse_name()
            node_class = await child.read_node_class()
            node_id    = child.nodeid
        except Exception as exc:
            print(prefix + branch + RED(f"[error: {exc}]"))
            continue

        # Optional namespace filter
        if ns_filter is not None and node_id.NamespaceIndex not in (0, ns_filter):
            continue

        ns_tag  = DIM(f"[ns={node_id.NamespaceIndex}]") if node_id.NamespaceIndex else ""
        id_tag  = DIM(f" ({node_id.Identifier})")
        nc_tag  = _nc_label(node_class)

        if node_class == ua.NodeClass.Variable and show_values:
            value = await _read_value(child)
            print(f"{prefix}{branch}{BOLD(bn.Name)}{id_tag}  {nc_tag}  {ns_tag}  = {value}")
            try:
                desc = (await child.read_description()).Text
                if desc:
                    print(f"{prefix}{cont}{DIM(desc)}")
            except Exception:
                pass

        elif node_class == ua.NodeClass.Method:
            in_args, out_args = await _read_method_args(child)
            in_str  = ", ".join(in_args)  or "—"
            out_str = ", ".join(out_args) or "—"
            print(f"{prefix}{branch}{BOLD(bn.Name)}{id_tag}  {nc_tag}  {ns_tag}")
            print(f"{prefix}{cont}in:  {in_str}")
            print(f"{prefix}{cont}out: {out_str}")

        else:
            print(f"{prefix}{branch}{BOLD(bn.Name)}{id_tag}  {nc_tag}  {ns_tag}")
            try:
                desc = (await child.read_description()).Text
                if desc:
                    print(f"{prefix}{cont}{DIM(desc)}")
            except Exception:
                pass

        # Recurse (skip built-in type nodes to avoid noise)
        if node_id.NamespaceIndex != 0:
            await browse(child, prefix + cont, ns_filter, show_values)


# ── Main ──────────────────────────────────────────────────────────────────

async def main(args):
    print(f"\nConnecting to {BOLD(args.endpoint)} …")

    client = Client(args.endpoint)
    if args.user:
        client.set_user(args.user)
        client.set_password(args.password or "")

    async with client:
        print("Connected.\n")

        # Print registered namespaces
        ns_array = await client.get_namespace_array()
        print(BOLD("Namespaces:"))
        for i, uri in enumerate(ns_array):
            marker = "  ← filter active" if (args.ns and i == args.ns) else ""
            print(f"  [{i}] {uri}{marker}")
        print()

        ns_filter = args.ns  # None means show all

        print(BOLD("Node tree (Objects):"))
        print(DIM("  Colour key: ") +
              CYAN("Object") + "  " + GREEN("Variable") + "  " + YELLOW("Method") + "\n")

        objects = client.nodes.objects
        await browse(objects, prefix="", ns_filter=ns_filter,
                     show_values=not args.no_values)

    print("\nDone.")


def parse_args():
    p = argparse.ArgumentParser(
        description="Browse all variables and methods in an OPC UA server.")
    p.add_argument("endpoint",
                   help="OPC UA endpoint, e.g. opc.tcp://localhost:4840/nectarcam/")
    p.add_argument("--ns", type=int, default=None, metavar="INDEX",
                   help="Only show nodes in this namespace index (default: all)")
    p.add_argument("--user",     default=None, help="Username for authentication")
    p.add_argument("--password", default=None, help="Password for authentication")
    p.add_argument("--no-values", action="store_true",
                   help="Skip reading variable values (faster, less noise)")
    return p.parse_args()


if __name__ == "__main__":
    asyncio.run(main(parse_args()))
