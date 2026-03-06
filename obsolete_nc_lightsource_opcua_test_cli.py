"""
nc_lightsource_opcua_test_cli.py
NectarCAM Calibration Box OPC UA Test CLI

Connects to the CalibrationBoxServer and exposes all methods
defined in nc_lightsource_opcua_server.py via an interactive CLI.
"""

import argparse
from opcua import Client, ua


DEFAULT_ENDPOINT = "opc.tcp://localhost:4840/nectarcam/"


# ==========================================================
# Subscription Handler
# ==========================================================
class SubHandler:

    def __init__(self):
        self.node_names = {}

    def datachange_notification(self, node, val, data):
        name = self.node_names.get(node.nodeid, str(node.nodeid))
        print(f"  [MONITOR] {name} = {val}")

    def event_notification(self, event):
        print("  [EVENT]", event)


# ==========================================================
# Client Wrapper
# ==========================================================
class CalibrationBoxClient:

    def __init__(self, endpoint: str, username: str = None,
                 password: str = None):
        self.client = Client(endpoint)
        if username:
            self.client.set_user(username)
            self.client.set_password(password or "")

        self.subscription   = None
        self._sub_handler   = None
        self._monitored     = {}   # nodeid -> display name
        self.device         = None
        self.monitoring     = None

    # ----------------------------------------------------------
    def connect(self):
        self.client.connect()
        root = self.client.get_root_node()
        self.device = root.get_child(
            ["0:Objects", "2:NectarCAM", "2:CalibrationBox"]
        )
        self.monitoring = self.device.get_child(["2:Monitoring"])
        print("Connected to server.")

    def disconnect(self):
        self.stop_monitoring()
        self.client.disconnect()
        print("Disconnected.")

    # ----------------------------------------------------------
    # Generic method caller
    # ----------------------------------------------------------
    def call_method(self, name: str, *args):
        method = self.device.get_child([f"2:{name}"])
        # python-opcua raises BadNothingToDo when an empty argument list is
        # passed explicitly for a no-argument method, so only splat args when
        # there are some.
        if args:
            self.device.call_method(method, *args)
        else:
            self.device.call_method(method)
        print(f"  OK: {name}() executed.")

    # ----------------------------------------------------------
    # Variable read
    # ----------------------------------------------------------
    def read_variable(self, name: str):
        """
        The server nests each variable under a sub-object:
            Monitoring/<name>/<name>_v
        """
        try:
            container = self.monitoring.get_child([f"2:{name}"])
            node      = container.get_child([f"2:{name}_v"])
            value     = node.get_value()
            print(f"  {name} = {value}")
        except Exception:
            # Fallback: try the node directly (flat layout)
            node  = self.monitoring.get_child([f"2:{name}"])
            value = node.get_value()
            print(f"  {name} = {value}")

    def read_all(self):
        """Read every variable under Monitoring."""
        for container in self.monitoring.get_children():
            name = container.get_browse_name().Name
            try:
                leaf  = container.get_child([f"2:{name}_v"])
                value = leaf.get_value()
            except Exception:
                value = container.get_value()
            print(f"  {name} = {value}")

    # ----------------------------------------------------------
    # Monitoring / subscriptions
    # ----------------------------------------------------------
    def start_monitoring(self):
        if self.subscription:
            print("  Already monitoring. Run 'unmonitor' first.")
            return

        self._sub_handler = SubHandler()
        self.subscription = self.client.create_subscription(
            500, self._sub_handler
        )
        self._monitored = {}

        for container in self.monitoring.get_children():
            cname = container.get_browse_name().Name
            try:
                leaf = container.get_child([f"2:{cname}_v"])
            except Exception:
                leaf = container
            handle = self.subscription.subscribe_data_change(leaf)
            self._monitored[leaf.nodeid] = cname

        self._sub_handler.node_names = self._monitored
        print(f"  Monitoring {len(self._monitored)} variable(s). "
              "Changes will be printed as they arrive.")

    def stop_monitoring(self):
        if self.subscription:
            self.subscription.delete()
            self.subscription  = None
            self._sub_handler  = None
            self._monitored    = {}
            print("  Monitoring stopped.")
        else:
            print("  Not currently monitoring.")


# ==========================================================
# Help text
# ==========================================================
HELP = """
╔══════════════════════════════════════════════════════════════╗
║        NectarCAM Calibration Box — OPC UA Test CLI           ║
╠══════════════════════════════════════════════════════════════╣
║  Device commands                                             ║
║    start                    Enable light pulses              ║
║    stop                     Disable light pulses             ║
║    reboot                   Reboot the device                ║
║    reconnect                Close and re-open TCP connection ║
║    getstatus                Poll hardware and refresh vars   ║
║                                                              ║
║  Parameter commands (one at a time)                          ║
║    setleds     <int>        LED bitmask (0–8191)             ║
║    setvoltage  <float>      Voltage in V                     ║
║    setduration <int>        Duration in μs                   ║
║    setfrequency <float> <int>  Dividend  Divider             ║
║    setcurrent  <float>      Central LED current in mA        ║
║    setwidth    <int>        Pulse width in ns                ║
║    setcontrol  <lemo> <f1> <f2> <ext> <enabled>              ║
║                             (true/false for each flag)       ║
║                                                              ║
║  Bulk configure                                              ║
║    configure <led_mask> <voltage_set> <duration>             ║
║              <freq_dividend> <freq_divider> <width>          ║
║              <lemo_out> <fiber1_out> <fiber2_out>            ║
║              <external> <enabled> <central_current>          ║
║    Example:                                                  ║
║      configure 8191 10.0 0 10000.0 1 1 true true true        ║
║                false true 0.0                                ║
║                                                              ║
║  Monitoring                                                  ║
║    read    <variable>       Read one monitoring variable     ║
║    readall                  Read all monitoring variables    ║
║    monitor                  Subscribe to all changes (live)  ║
║    unmonitor                Cancel subscription              ║
║                                                              ║
║  Other                                                       ║
║    ?  /  help               Show this menu                   ║
║    exit  /  quit            Disconnect and exit              ║
╚══════════════════════════════════════════════════════════════╝
"""


# ==========================================================
# CLI loop
# ==========================================================
def run_cli(cli: CalibrationBoxClient):
    print(HELP)

    while True:
        try:
            raw = input("calbox>> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        # Ignore empty lines
        if not raw:
            continue

        parts = raw.split()
        cmd   = parts[0].lower()

        try:
            # ---- Help -----------------------------------------------
            if cmd in ("?", "help"):
                print(HELP)

            # ---- No-argument device commands ------------------------
            elif cmd == "start":
                cli.call_method("Start")

            elif cmd == "stop":
                cli.call_method("Stop")

            elif cmd == "reboot":
                cli.call_method("Reboot")

            elif cmd == "reconnect":
                cli.call_method("Reconnect")

            elif cmd == "getstatus":
                cli.call_method("GetStatus")

            # ---- Single-argument commands ---------------------------
            elif cmd == "setleds":
                if len(parts) != 2:
                    print("  Usage: setleds <int>")
                    continue
                cli.call_method("SetLeds", int(parts[1]))

            elif cmd == "setvoltage":
                if len(parts) != 2:
                    print("  Usage: setvoltage <float>")
                    continue
                cli.call_method("SetVoltage", float(parts[1]))

            elif cmd == "setduration":
                if len(parts) != 2:
                    print("  Usage: setduration <int>")
                    continue
                cli.call_method("SetDuration", int(parts[1]))

            elif cmd == "setcurrent":
                if len(parts) != 2:
                    print("  Usage: setcurrent <float>")
                    continue
                cli.call_method("SetCurrent", float(parts[1]))

            elif cmd == "setwidth":
                if len(parts) != 2:
                    print("  Usage: setwidth <int>")
                    continue
                cli.call_method("SetWidth", int(parts[1]))

            # ---- Two-argument commands ------------------------------
            elif cmd == "setfrequency":
                if len(parts) != 3:
                    print("  Usage: setfrequency <dividend:float> "
                          "<divider:int>")
                    continue
                cli.call_method("SetFrequency",
                                float(parts[1]), int(parts[2]))

            # ---- Five-argument commands -----------------------------
            elif cmd == "setcontrol":
                if len(parts) != 6:
                    print("  Usage: setcontrol <lemo> <fiber1> <fiber2> "
                          "<external> <enabled>  (true/false)")
                    continue
                flags = [p.lower() == "true" for p in parts[1:6]]
                cli.call_method("SetControl", *flags)

            # ---- Bulk configure ------------------------------------
            elif cmd == "configure":
                if len(parts) != 13:
                    print("  Usage: configure <led_mask> <voltage_set> "
                          "<duration> <freq_dividend> <freq_divider> "
                          "<width> <lemo_out> <fiber1_out> <fiber2_out> "
                          "<external> <enabled> <central_current>")
                    continue
                args = [
                    int(parts[1]),               # led_mask
                    float(parts[2]),             # voltage_set
                    int(parts[3]),               # duration
                    float(parts[4]),             # frequency_dividend
                    int(parts[5]),               # frequency_divider
                    int(parts[6]),               # width
                    parts[7].lower()  == "true", # lemo_out
                    parts[8].lower()  == "true", # fiber1_out
                    parts[9].lower()  == "true", # fiber2_out
                    parts[10].lower() == "true", # external
                    parts[11].lower() == "true", # enabled
                    float(parts[12]),            # central_current
                ]
                cli.call_method("Configure", *args)

            # ---- Read variables ------------------------------------
            elif cmd == "read":
                if len(parts) != 2:
                    print("  Usage: read <variable_name>")
                    continue
                cli.read_variable(parts[1])

            elif cmd == "readall":
                cli.read_all()

            # ---- Monitoring ----------------------------------------
            elif cmd == "monitor":
                cli.start_monitoring()

            elif cmd == "unmonitor":
                cli.stop_monitoring()

            # ---- Exit ----------------------------------------------
            elif cmd in ("exit", "quit"):
                break

            else:
                print(f"  Unknown command: '{cmd}'.  Type '?' for help.")

        except ua.UaStatusCodeError as exc:
            print(f"  OPC UA error: {exc}")
        except (ValueError, IndexError) as exc:
            print(f"  Argument error: {exc}")
        except Exception as exc:
            print(f"  Error: {exc}")


# ==========================================================
# Entry point
# ==========================================================
def parse_args():
    p = argparse.ArgumentParser(
        description="NectarCAM CalibrationBox OPC UA Test CLI"
    )
    p.add_argument("--endpoint", default=DEFAULT_ENDPOINT,
                   help="OPC UA server endpoint URL")
    p.add_argument("--user",     default=None,
                   help="OPC UA username (omit for anonymous)")
    p.add_argument("--password", default=None,
                   help="OPC UA password")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    cli  = CalibrationBoxClient(args.endpoint, args.user, args.password)
    try:
        cli.connect()
        run_cli(cli)
    finally:
        cli.disconnect()
