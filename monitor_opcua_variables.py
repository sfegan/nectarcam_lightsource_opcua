#!/usr/bin/env python3
"""
monitor_opcua_variables.py — Subscribe to OPC UA variables and display them in a live table.

Usage:
    python3 monitor_opcua_variables.py opc.tcp://localhost:4840/nectarcam/
    python3 monitor_opcua_variables.py opc.tcp://localhost:4840/ --path Monitoring
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
    
    # Logic from initial version and browse script:
    # Extract the descriptive name from parentheses if present.
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

class SubscriptionHandler:
    def __init__(self, update_callback):
        self.update_callback = update_callback

    def datachange_notification(self, node, val, data):
        # asyncua behavior varies by version. 
        # Attempt to find the DataValue object which contains Status and Timestamp.
        datavalue = None
        if isinstance(val, ua.DataValue):
            datavalue = val
        elif hasattr(data, "Value") and isinstance(data.Value, ua.DataValue):
            datavalue = data.Value
        elif isinstance(data, ua.DataValue):
            datavalue = data
        
        if datavalue:
            self.update_callback(node, datavalue)
        else:
            # Fallback for raw value updates
            self.update_callback(node, val, is_raw=True)

    def event_notification(self, event):
        pass

class Monitor:
    def __init__(self, endpoint, path, user=None, password=None):
        self.endpoint = endpoint
        self.path = path
        self.user = user
        self.password = password
        self.variables = {} # nodeid -> {name, value, status, timestamp}
        self.updated = False

    def _unwrap_value(self, val):
        """Extract the actual value from a Variant or complex object."""
        if hasattr(val, "Value"):
            val = val.Value
        return val

    async def find_variables(self, node, prefix=""):
        try:
            children = await node.get_children()
        except Exception:
            return

        for child in children:
            node_class = await child.read_node_class()
            browse_name = (await child.read_browse_name()).Name
            
            if node_class == ua.NodeClass.Variable:
                full_name = f"{prefix}/{browse_name}" if prefix else browse_name
                try:
                    # CRITICAL: raise_on_bad_status=False ensures we get the real StatusCode 
                    # from the server (like BadNoCommunication) instead of an exception.
                    dv = await child.read_data_value(raise_on_bad_status=False)
                    self.variables[child.nodeid] = {
                        "name": full_name,
                        "value": self._unwrap_value(dv.Value),
                        "status": dv.StatusCode.value,
                        "timestamp": dv.SourceTimestamp
                    }
                except Exception as e:
                    self.variables[child.nodeid] = {
                        "name": full_name,
                        "value": f"Error: {e}",
                        "status": 0x80000000, # Bad
                        "timestamp": None
                    }
            elif node_class == ua.NodeClass.Object:
                await self.find_variables(child, f"{prefix}/{browse_name}" if prefix else browse_name)

    def update_variable(self, node, data, is_raw=False):
        nodeid = node.nodeid
        if nodeid in self.variables:
            if is_raw:
                # data is the raw value here
                self.variables[nodeid]["value"] = self._unwrap_value(data)
            else:
                # data is a DataValue object
                self.variables[nodeid]["value"] = self._unwrap_value(data.Value)
                self.variables[nodeid]["status"] = data.StatusCode.value
                self.variables[nodeid]["timestamp"] = data.SourceTimestamp
            self.updated = True

    def draw(self):
        if not self.variables:
            return
        
        lines = [CURSOR_HOME]
        lines.append(BOLD(f"Monitoring OPC UA Variables at {self.endpoint}"))
        lines.append(DIM(f"Path: {self.path} | Local Time: {datetime.now().strftime('%H:%M:%S')}"))
        lines.append("")
        
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
            # Manual padding because ANSI codes break field width calculation
            padding = " " * max(0, 30 - len(raw_name))
            
            ts_obj = var["timestamp"]
            ts_str = ts_obj.strftime("%H:%M:%S.%f")[:-3] if ts_obj else "N/A"
            age_str = format_age(ts_obj)
            
            line = f"{name:<40} {val:<25} {status_label}{padding} {ts_str:<24} {age_str}"
            lines.append(line + CLEAR_EOL)

        lines.append(CLEAR_EOS)
        sys.stdout.write("\n".join(lines))
        sys.stdout.flush()
        self.updated = False

    async def resolve_path(self, root, path):
        """Recursively find a node by name path, ignoring namespaces for ease of use."""
        if not path:
            return root
        
        parts = path.replace(".", "/").split("/")
        current = root
        
        for part in parts:
            if not part: continue
            found = False
            children = await current.get_children()
            for child in children:
                bn = await child.read_browse_name()
                if bn.Name == part:
                    current = child
                    found = True
                    break
            if not found:
                # If not found directly, try to search recursively one level deeper
                if current == root:
                    for child in children:
                        try:
                            deep_found = await self.resolve_path(child, part)
                            if deep_found != child:
                                current = deep_found
                                found = True
                                break
                        except Exception:
                            continue
                
                if not found:
                    raise ValueError(f"Could not find path element: {part}")
        return current

    async def run(self):
        client = Client(self.endpoint)
        if self.user:
            client.set_user(self.user)
            client.set_password(self.password or "")

        try:
            async with client:
                print(f"Connecting to {self.endpoint} ...")
                target_node = await self.resolve_path(client.nodes.objects, self.path)
                
                print(f"Browsing variables under {BOLD((await target_node.read_browse_name()).Name)} ...")
                await self.find_variables(target_node)
                
                if not self.variables:
                    print("No variables found to monitor.")
                    return

                print(f"Subscribing to {len(self.variables)} variables ...")
                handler = SubscriptionHandler(self.update_variable)
                sub = await client.create_subscription(500, handler)
                
                nodes = list(self.variables.keys())
                await sub.subscribe_data_change([client.get_node(nid) for nid in nodes])

                # Start drawing loop
                sys.stdout.write(CLEAR_SCREEN)
                sys.stdout.write(HIDE_CURSOR)
                self.updated = True
                
                try:
                    while True:
                        self.draw()
                        await asyncio.sleep(0.1) 
                except asyncio.CancelledError:
                    pass
                finally:
                    sys.stdout.write(SHOW_CURSOR)
                    print("\nMonitoring stopped.")

        except Exception as e:
            sys.stdout.write(SHOW_CURSOR)
            sys.exit(f"\nError: {e}")

def parse_args():
    p = argparse.ArgumentParser(description="Monitor OPC UA variables.")
    p.add_argument("endpoint", help="OPC UA endpoint")
    p.add_argument("--path", default="Monitoring", help="Path to object to monitor")
    p.add_argument("--user", help="Username")
    p.add_argument("--password", help="Password")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    monitor = Monitor(args.endpoint, args.path, args.user, args.password)
    try:
        asyncio.run(monitor.run())
    except KeyboardInterrupt:
        pass
