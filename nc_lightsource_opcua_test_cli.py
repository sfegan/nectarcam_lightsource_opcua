from opcua import Client, ua
import sys
import threading


ENDPOINT = "opc.tcp://localhost:4840/nectarcam/"


# ==========================================================
# Subscription Handler
# ==========================================================
class SubHandler:

    def __init__(self):
        self.node_names = {}

    def datachange_notification(self, node, val, data):
        name = self.node_names.get(node.nodeid, str(node.nodeid))
        print(f"[MONITOR] {name} = {val}")
        
    def event_notification(self, event):
        print("[EVENT]", event)


# ==========================================================
# Client Wrapper
# ==========================================================
class CalibrationBoxClient:

    def __init__(self, endpoint):
        self.client = Client(endpoint)
        self.subscription = None
        self.sub_handle = None

    def connect(self):
        self.client.connect()
        print("Connected to server")

        root = self.client.get_root_node()

        self.device = root.get_child(
            ["0:Objects", "2:NectarCAM", "2:CalibrationBox"]
        )

        self.monitoring = self.device.get_child(["2:Monitoring"])

    # ------------------------------------------------------
    # Methods
    # ------------------------------------------------------
    def call_method(self, name, *args):

        method = self.device.get_child([f"2:{name}"])

        self.device.call_method(method, *args)

        print(f"{name}() executed")

    # ------------------------------------------------------
    # Variable Read
    # ------------------------------------------------------
    def read_variable(self, name):
        node = self.monitoring.get_child([f"2:{name}"])
        value = node.get_value()
        print(f"{name} = {value}")

    # ------------------------------------------------------
    # Monitoring
    # ------------------------------------------------------
    def start_monitoring(self):

        handler = SubHandler()

        self.subscription = self.client.create_subscription(500, handler)

        self.monitored_nodes = {}

        for child in self.monitoring.get_children():
            browse_name = child.get_browse_name().Name
            handle = self.subscription.subscribe_data_change(child)

            # Store mapping locally
            self.monitored_nodes[child.nodeid] = browse_name

        handler.node_names = self.monitored_nodes

        print("Monitoring started")

    def stop_monitoring(self):
        if self.subscription:
            self.subscription.delete()
            self.subscription = None
            print("Monitoring stopped")

    # ------------------------------------------------------
    def disconnect(self):
        self.client.disconnect()


# ==========================================================
# CLI Interface
# ==========================================================
def main():

    cli = CalibrationBoxClient(ENDPOINT)
    cli.connect()

    print("""
Available commands:
  start
  stop
  configure <12 args>
  read <variable>
  monitor
  unmonitor
  exit
""")

    while True:
        try:
            cmd = input(">> ").strip()

            if cmd == "start":
                cli.call_method("Start")

            elif cmd == "stop":
                cli.call_method("Stop")

            elif cmd.startswith("configure"):
                parts = cmd.split()
                if len(parts) != 13:
                    print("Expected 12 arguments")
                    continue

                args = [
                    int(parts[1]),
                    float(parts[2]),
                    int(parts[3]),
                    float(parts[4]),
                    int(parts[5]),
                    int(parts[6]),
                    parts[7].lower() == "true",
                    parts[8].lower() == "true",
                    parts[9].lower() == "true",
                    parts[10].lower() == "true",
                    parts[11].lower() == "true",
                    float(parts[12]),
                ]

                cli.call_method("Configure", *args)

            elif cmd.startswith("read"):
                _, var = cmd.split()
                cli.read_variable(var)

            elif cmd == "monitor":
                cli.start_monitoring()

            elif cmd == "unmonitor":
                cli.stop_monitoring()

            elif cmd == "exit":
                break

            else:
                print("Unknown command")

        except Exception as e:
            print("Error:", e)

    cli.disconnect()


if __name__ == "__main__":
    main()
    