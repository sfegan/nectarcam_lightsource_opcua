from opcua import Server, ua
import threading
import time
from enum import IntEnum


# ==========================================================
# Hardware Layer (Replace with real TCP implementation)
# ==========================================================
class CalBoxHardware:

    def __init__(self, address, port, password):
        self.address = address
        self.port = port
        self.password = password
        self.connected = False

    def connect(self):
        # TODO: real TCP connection
        self.connected = True
        return True

    def start(self):
        return True

    def stop(self):
        return True

    def configure(self, **kwargs):
        return True

    def get_status(self):
        # Simulated status
        return {
            "led_mask": 8191,
            "voltage_set": 10.0,
            "voltage_actual": 9.98,
            "duration": 0,
            "frequency_dividend": 10000.0,
            "frequency_divider": 1,
            "width": 1,
            "temperature": 22.3,
            "humidity": 45.0,
            "faults": 0,
            "light_pulse": False,
            "lemo_out": True,
            "fiber1_out": True,
            "fiber2_out": True,
            "lemo_in": False,
            "central_current": 0.0,
        }


# ==========================================================
# Device FSM
# ==========================================================
class DeviceState(IntEnum):
    Disabled = 0
    Enabled = 1
    Fault = 2
    Offline = 3


# ==========================================================
# OPC UA Device Server
# ==========================================================
class CalibrationBoxServer:

    def __init__(self):

        self.server = Server()
        self.server.set_endpoint("opc.tcp://0.0.0.0:4840/nectarcam/")
        self.server.set_server_name("NectarCAM CalibrationBox")

        self.idx = self.server.register_namespace(
            "http://nectarcam.calibrationbox"
        )

        objects = self.server.get_objects_node()

        self.root = objects.add_object(self.idx, "NectarCAM")
        self.device = self.root.add_object(self.idx, "CalibrationBox")

        self._create_monitoring()
        self._create_methods()

        self.hardware = CalBoxHardware("10.11.4.69", 50001, "")
        self.state = DeviceState.Disabled

        self._running = True

    # ------------------------------------------------------
    # Monitoring Model
    # ------------------------------------------------------
    def _create_monitoring(self):

        self.monitoring = self.device.add_object(self.idx, "Monitoring")

        def v(name, value, vtype):
            return self.monitoring.add_variable(
                self.idx,
                name,
                ua.Variant(value, vtype),
            )

        self.vars = {
            "led_mask": v("led_mask", 8191, ua.VariantType.Int32),
            "voltage_set": v("voltage_set", 10.0, ua.VariantType.Float),
            "voltage_actual": v("voltage_actual", 10.0, ua.VariantType.Float),
            "duration": v("duration", 0, ua.VariantType.Int32),
            "frequency_dividend": v("frequency_dividend", 10000.0, ua.VariantType.Float),
            "frequency_divider": v("frequency_divider", 1, ua.VariantType.Int32),
            "width": v("width", 1, ua.VariantType.Int32),
            "temperature": v("temperature", 20.0, ua.VariantType.Float),
            "humidity": v("humidity", 50.0, ua.VariantType.Float),
            "faults": v("faults", 0, ua.VariantType.Int16),
            "light_pulse": v("light_pulse", False, ua.VariantType.Boolean),
            "lemo_out": v("lemo_out", True, ua.VariantType.Boolean),
            "fiber1_out": v("fiber1_out", True, ua.VariantType.Boolean),
            "fiber2_out": v("fiber2_out", True, ua.VariantType.Boolean),
            "lemo_in": v("lemo_in", False, ua.VariantType.Boolean),
            "central_current": v("central_current", 0.0, ua.VariantType.Float),
            "cls_state": v("cls_state", 0, ua.VariantType.Int32),
        }

        # Monitoring variables are read-only
        for var in self.vars.values():
            var.set_read_only()

    # ------------------------------------------------------
    # Methods (Typed, No String Dispatch)
    # ------------------------------------------------------
    def _create_methods(self):

        self.device.add_method(
            self.idx,
            "Start",
            self.start_method,
            [],
            []
        )

        self.device.add_method(
            self.idx,
            "Stop",
            self.stop_method,
            [],
            []
        )

        self.device.add_method(
            self.idx,
            "Configure",
            self.configure_method,
            [
                ua.VariantType.Int32,
                ua.VariantType.Float,
                ua.VariantType.Int32,
                ua.VariantType.Float,
                ua.VariantType.Int32,
                ua.VariantType.Int32,
                ua.VariantType.Boolean,
                ua.VariantType.Boolean,
                ua.VariantType.Boolean,
                ua.VariantType.Boolean,
                ua.VariantType.Boolean,
                ua.VariantType.Float,
            ],
            []
        )

    # ------------------------------------------------------
    # Method Implementations
    # ------------------------------------------------------
    def start_method(self, parent):
        if not self.hardware.start():
            raise ua.UaStatusCodeError(ua.StatusCodes.Bad)
        self.state = DeviceState.Enabled
        self._update_state()
        return []

    def stop_method(self, parent):
        if not self.hardware.stop():
            raise ua.UaStatusCodeError(ua.StatusCodes.Bad)
        self.state = DeviceState.Disabled
        self._update_state()
        return []

    def configure_method(self, parent, *args):

        keys = [
            "led_mask",
            "voltage_set",
            "duration",
            "frequency_dividend",
            "frequency_divider",
            "width",
            "light_pulse",
            "lemo_out",
            "fiber1_out",
            "fiber2_out",
            "lemo_in",
            "central_current",
        ]

        kwargs = dict(zip(keys, args))

        if not self.hardware.configure(**kwargs):
            raise ua.UaStatusCodeError(ua.StatusCodes.Bad)

        return []

    # ------------------------------------------------------
    # State & Monitoring Updates
    # ------------------------------------------------------
    def _update_state(self):
        self.vars["cls_state"].set_value(int(self.state))

    def _poll_hardware(self):

        while self._running:
            try:
                status = self.hardware.get_status()
                for k, v in status.items():
                    if k in self.vars:
                        self.vars[k].set_value(v)
            except Exception:
                self.state = DeviceState.Offline
                self._update_state()
            time.sleep(1)

    # ------------------------------------------------------
    def start(self):

        self.hardware.connect()

        self.server.start()

        polling_thread = threading.Thread(
            target=self._poll_hardware,
            daemon=True
        )
        polling_thread.start()

        print("CalibrationBox OPC UA server running")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self._running = False
            self.server.stop()


# ==========================================================
if __name__ == "__main__":
    CalibrationBoxServer().start()