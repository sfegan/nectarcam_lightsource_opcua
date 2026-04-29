#!/usr/bin/env python3
"""
monitor_opcua_variables.py — Poll OPC UA variables and display them in a live table.

Usage:
    python3 monitor_opcua_variables.py opc.tcp://localhost:4840/nectarcam/
    python3 monitor_opcua_variables.py opc.tcp://localhost:4840/ --path Monitoring --interval 0.5
"""

import argparse
import asyncio
import sys
import os
from datetime import datetime, timezone

try:
    from asyncua import Client, ua
except ImportError:
    sys.exit("asyncua not installed — run: pip install asyncua")

# -- ANSI colours ---------------------------------------------------------
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

# -- Terminal control -----------------------------------------------------
CLEAR_SCREEN = "\033[H\033[J"
CURSOR_HOME  = "\033[H"
HIDE_CURSOR  = "\033[?25l"
SHOW_CURSOR  = "\033[?25h"
CLEAR_EOL    = "\033[K"
CLEAR_EOS    = "\033[J"

def get_status_label(status_code_int):
    """Return (colored_label, raw_name) for display and padding."""
    sc = ua.StatusCode(status_code_int)
    name = sc.name
    
    # Extract descriptive name from parentheses if present
    if "(" in name and name.endswith(")"):
        name = name[name.rfind("(") + 1:-1]
    
    severity = sc.value & 0xC0000000
    if severity == 0x00000000: # Good
        return GREEN(name), name
    elif severity == 0x40000000: # Uncertain
        return YELLOW(name), name
    else: # Bad
        return RED(name), name

def format_age(dt):
    if not dt:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    
    diff = datetime.now(timezone.utc) - dt
    seconds = int(diff.total_seconds())
    
    if seconds < 0: return "future"
    if seconds < 1: return "just now"
    if seconds < 60: return f"{seconds}s ago"
    if seconds < 3600: return f"{seconds // 60}m ago"
    if seconds < 86400: return f"{seconds // 3600}h ago"
    return f"{seconds // 86400}d ago"

class Monitor:
    def __init__(self, endpoint, path, interval=1.0, user=None, password=None):
        self.endpoint = endpoint
        self.path = path
        self.interval = interval
        self.user = user
        self.password = password
        self.variables = {} # nodeid -> {name, value, status, timestamp, node_obj}
        self.connected = False

    def _unwrap_value(self, val):
        """Extract the actual value from a Variant or complex object."""
        if hasattr(val, "Value"):
            val = val.Value
        return val

    async def find_variables(self, node, prefix=""):
        """Recursively find all variables under the given node."""
        try:
            children = await node.get_children()
        except Exception:
            return

        for child in children:
            node_class = await child.read_node_class()
            browse_name = (await child.read_browse_name()).Name
            
            if node_class == ua.NodeClass.Variable:
                full_name = f"{prefix}/{browse_name}" if prefix else browse_name
                self.variables[child.nodeid] = {
                    "name": full_name,
                    "value": None,
                    "status": 0,
                    "timestamp": None,
                    "node_obj": child
                }
            elif node_class == ua.NodeClass.Object:
                await self.find_variables(child, f"{prefix}/{browse_name}" if prefix else browse_name)

    async def poll_all(self):
        """Poll all variables in parallel."""
        if not self.variables:
            return

        tasks = []
        node_ids = list(self.variables.keys())
        
        for nid in node_ids:
            tasks.append(self.variables[nid]["node_obj"].read_data_value(raise_on_bad_status=False))
            
        results = await asyncio.gather(*tasks)
        
        for nid, dv in zip(node_ids, results):
            self.variables[nid]["value"] = self._unwrap_value(dv.Value)
            self.variables[nid]["status"] = dv.StatusCode.value
            self.variables[nid]["timestamp"] = dv.SourceTimestamp or dv.ServerTimestamp or datetime.now(timezone.utc)

    def draw(self, error=None):
        lines = [CURSOR_HOME]
        lines.append(BOLD(f"Monitoring OPC UA Variables at {self.endpoint}"))
        lines.append(DIM(f"Path: {self.path} | Interval: {self.interval}s | Local Time: {datetime.now().strftime('%H:%M:%S')}"))
        lines.append("")

        if not self.connected:
            lines.append(YELLOW("Waiting for OPC UA server ...") + CLEAR_EOL)
            if error:
                lines.append(RED(f"Last Error: {error}") + CLEAR_EOL)
            lines.append(CLEAR_EOS)
            sys.stdout.write("\n".join(lines))
            sys.stdout.flush()
            return
            
        if not self.variables:
            lines.append("Connected, browsing variables..." + CLEAR_EOL)
            lines.append(CLEAR_EOS)
            sys.stdout.write("\n".join(lines))
            sys.stdout.flush()
            return
        
        # Name(40), Value(25), Status(30), Timestamp(20), Age(15)
        header = f"{'Name':<40} {'Value':<25} {'Status':<30} {'Source Timestamp':<24} {'Age'}"
        lines.append(BOLD(header))
        lines.append(DIM("-" * (len(header) + 10)))

        sorted_vars = sorted(self.variables.values(), key=lambda x: x["name"])
        
        for var in sorted_vars:
            name = var["name"]
            if len(name) > 39:
                name = "..." + name[-36:]
            
            val = str(var["value"])
            if len(val) > 24:
                val = val[:21] + "..."
            
            status_label, raw_name = get_status_label(var["status"])
            padding = " " * max(0, 30 - len(raw_name))
            
            ts_obj = var["timestamp"]
            ts_str = ts_obj.strftime("%H:%M:%S.%f")[:-3] if ts_obj else "N/A"
            age_str = format_age(ts_obj)
            
            line = f"{name:<40} {val:<25} {status_label}{padding} {ts_str:<24} {age_str}"
            lines.append(line + CLEAR_EOL)

        lines.append(CLEAR_EOS)
        sys.stdout.write("\n".join(lines))
        sys.stdout.flush()

    async def resolve_path(self, root, path):
        if not path:
            return root
        parts = path.replace(".", "/").split("/")
        current = root
        for part in parts:
            if not part: continue
            found = False
            for child in await current.get_children():
                if (await child.read_browse_name()).Name == part:
                    current = child
                    found = True
                    break
            if not found:
                if current == root:
                    for child in await current.get_children():
                        try:
                            deep = await self.resolve_path(child, part)
                            if deep != child:
                                current = deep
                                found = True
                                break
                        except Exception: continue
                if not found:
                    raise ValueError(f"Could not find path element: {part}")
        return current

    async def run(self):
        sys.stdout.write(CLEAR_SCREEN)
        sys.stdout.write(HIDE_CURSOR)
        
        try:
            while True:
                client = Client(self.endpoint)
                if self.user:
                    client.set_user(self.user)
                    client.set_password(self.password or "")

                try:
                    async with client:
                        self.connected = True
                        target_node = await self.resolve_path(client.nodes.objects, self.path)
                        await self.find_variables(target_node)
                        
                        while True:
                            start_time = asyncio.get_event_loop().time()
                            await self.poll_all()
                            self.draw()
                            
                            elapsed = asyncio.get_event_loop().time() - start_time
                            sleep_time = max(0.01, self.interval - elapsed)
                            await asyncio.sleep(sleep_time)

                except (asyncio.CancelledError, KeyboardInterrupt):
                    raise
                except Exception as e:
                    self.connected = False
                    self.variables = {}
                    self.draw(error=str(e))
                    await asyncio.sleep(self.interval)

        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
        finally:
            sys.stdout.write(SHOW_CURSOR)
            print("\nMonitoring stopped.")

def parse_args():
    p = argparse.ArgumentParser(description="Monitor OPC UA variables by polling.")
    p.add_argument("endpoint", help="OPC UA endpoint")
    p.add_argument("--path", default="Monitoring", help="Path to object to monitor")
    p.add_argument("--interval", type=float, default=1.0, help="Polling interval in seconds")
    p.add_argument("--user", help="Username")
    p.add_argument("--password", help="Password")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    monitor = Monitor(args.endpoint, args.path, args.interval, args.user, args.password)
    try:
        asyncio.run(monitor.run())
    except KeyboardInterrupt:
        pass
