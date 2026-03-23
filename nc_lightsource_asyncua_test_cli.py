"""
nc_lightsource_asyncua_test_cli.py
NectarCAM Calibration Light Source OPC UA Test CLI

Connects to the CalibrationBoxServer and exposes all methods
defined in nc_lightsource_asyncua_server.py via an interactive CLI.

Requires: asyncua  (pip install asyncua)
"""

import argparse
import asyncio
from asyncua import Client, ua


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
        self.endpoint  = endpoint
        self.username  = username
        self.password  = password

        self.client      = None
        self.subscription   = None
        self._sub_handler   = None
        self._monitored     = {}   # nodeid -> display name
        self.device         = None
        self.monitoring     = None

    # ----------------------------------------------------------
    async def connect(self):
        self.client = Client(self.endpoint, watchdog_intervall=30.0)

        if self.username:
            self.client.set_user(self.username)
            self.client.set_password(self.password or "")

        await self.client.connect()
        root = self.client.get_root_node()
        self.device = await root.get_child(
            ["0:Objects", "2:CalibrationLightSource"]
        )
        self.monitoring = await self.device.get_child(["2:Monitoring"])
        print("Connected to server.")

    async def disconnect(self):
        await self.stop_monitoring()
        await self.client.disconnect()
        print("Disconnected.")

    # ----------------------------------------------------------
    # Generic method caller
    # ----------------------------------------------------------
    async def call_method(self, name: str, *args):
        method = await self.device.get_child([f"2:{name}"])
        if args:
            await self.device.call_method(method, *args)
        else:
            await self.device.call_method(method)
        print(f"  OK: {name}() executed.")

    # ----------------------------------------------------------
    # Variable read
    # ----------------------------------------------------------
    async def read_variable(self, name: str):
        """Read a single variable from Monitoring/<name>."""
        node  = await self.monitoring.get_child([f"2:{name}"])
        value = await node.get_value()
        print(f"  {name} = {value}")

    async def read_all(self):
        """Read every variable under Monitoring."""
        for node in await self.monitoring.get_children():
            name  = (await node.read_browse_name()).Name
            value = await node.get_value()
            print(f"  {name} = {value}")

    # ----------------------------------------------------------
    # Monitoring / subscriptions
    # ----------------------------------------------------------
    async def start_monitoring(self):
        if self.subscription:
            print("  Already monitoring. Run 'unmonitor' first.")
            return

        self._sub_handler = SubHandler()
        self.subscription = await self.client.create_subscription(
            500, self._sub_handler
        )
        self._monitored = {}

        for node in await self.monitoring.get_children():
            name = (await node.read_browse_name()).Name
            await self.subscription.subscribe_data_change(node)
            self._monitored[node.nodeid] = name

        self._sub_handler.node_names = self._monitored
        print(f"  Monitoring {len(self._monitored)} variable(s). "
              "Changes will be printed as they arrive.")

    async def stop_monitoring(self):
        if self.subscription:
            await self.subscription.delete()
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
║     NectarCAM Calibration Light Source — OPC UA Test CLI     ║
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
║      static: host, port, dialect                             ║
║      live:   led_mask, voltage_set, voltage_actual,          ║
║              duration, frequency_dividend, frequency_divider,║
║              width, temperature, humidity, faults,           ║
║              light_pulse, lemo_out, fiber1_out, fiber2_out,  ║
║              lemo_in, central_current, device_state          ║
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
async def run_cli(cli: CalibrationBoxClient):
    print(HELP)
    loop = asyncio.get_event_loop()

    while True:
        try:
            # Run blocking input() in a thread executor so the event loop
            # stays alive (needed for subscription callbacks to fire).
            raw = await loop.run_in_executor(None, lambda: input("cls>> "))
            raw = raw.strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

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
                await cli.call_method("Start")

            elif cmd == "stop":
                await cli.call_method("Stop")

            elif cmd == "reboot":
                await cli.call_method("Reboot")

            elif cmd == "reconnect":
                await cli.call_method("Reconnect")

            elif cmd == "getstatus":
                await cli.call_method("GetStatus")

            # ---- Single-argument commands ---------------------------
            elif cmd == "setleds":
                if len(parts) != 2:
                    print("  Usage: setleds <int>")
                    continue
                await cli.call_method("SetLeds", int(parts[1]))

            elif cmd == "setvoltage":
                if len(parts) != 2:
                    print("  Usage: setvoltage <float>")
                    continue
                await cli.call_method("SetVoltage", float(parts[1]))

            elif cmd == "setduration":
                if len(parts) != 2:
                    print("  Usage: setduration <int>")
                    continue
                await cli.call_method("SetDuration", int(parts[1]))

            elif cmd == "setcurrent":
                if len(parts) != 2:
                    print("  Usage: setcurrent <float>")
                    continue
                await cli.call_method("SetCurrent", float(parts[1]))

            elif cmd == "setwidth":
                if len(parts) != 2:
                    print("  Usage: setwidth <int>")
                    continue
                await cli.call_method("SetWidth", int(parts[1]))

            # ---- Two-argument commands ------------------------------
            elif cmd == "setfrequency":
                if len(parts) != 3:
                    print("  Usage: setfrequency <dividend:float> "
                          "<divider:int>")
                    continue
                await cli.call_method("SetFrequency",
                                      float(parts[1]), int(parts[2]))

            # ---- Five-argument commands -----------------------------
            elif cmd == "setcontrol":
                if len(parts) != 6:
                    print("  Usage: setcontrol <lemo> <fiber1> <fiber2> "
                          "<external> <enabled>  (true/false)")
                    continue
                flags = [p.lower() == "true" for p in parts[1:6]]
                await cli.call_method("SetControl", *flags)

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
                await cli.call_method("Configure", *args)

            # ---- Read variables ------------------------------------
            elif cmd == "read":
                if len(parts) != 2:
                    print("  Usage: read <variable_name>")
                    continue
                await cli.read_variable(parts[1])

            elif cmd == "readall":
                await cli.read_all()

            # ---- Monitoring ----------------------------------------
            elif cmd == "monitor":
                await cli.start_monitoring()

            elif cmd == "unmonitor":
                await cli.stop_monitoring()

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
        description="NectarCAM Calibration Light Source OPC UA Test CLI"
    )
    p.add_argument("--endpoint", default=DEFAULT_ENDPOINT,
                   help="OPC UA server endpoint URL")
    p.add_argument("--user",     default=None,
                   help="OPC UA username (omit for anonymous)")
    p.add_argument("--password", default=None,
                   help="OPC UA password")
    return p.parse_args()


async def main():
    args = parse_args()
    cli  = CalibrationBoxClient(args.endpoint, args.user, args.password)
    try:
        await cli.connect()
        await run_cli(cli)
    finally:
        await cli.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
