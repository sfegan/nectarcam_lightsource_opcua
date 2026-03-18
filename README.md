# NectarCAM Calibration Light Source OPC UA Server and Test Suite

This repository implements a simple OPC UA server for the NectarCAM calibration light sources, based on the [MOS version written by Patrick Sizun](https://gitlab.cta-observatory.org/cta-array-elements/mst/nectarcam/software/cal/calibration-box-mos/-/tree/main?ref_type=heads).

## Components

The repository contains four main components:

1. **`nc_lightsource_asyncua_server.py`**: The OPC UA server that acts as a bridge between the socket-based protocol used by the light sources and the OPC UA network.
2. **`nc_lightsource_asyncua_gui.py`**: A Python/TK-based GUI for sending commands and monitoring the light source via the OPC UA server.
3. **`nc_lightsource_asyncua_test_cli.py`**: A simple test utility that provides a text interface to call OPC UA methods and read/monitor variables.
4. **`nc_lightsource_emulator.py`**: A simple emulator for the light source that responds to commands over TCP and from the terminal.

## Server Architecture and Functioning

The server implements a three-layer architecture:

### 1. Dialect Layer
Pure data classes that handle conversion between device state and raw bytes for different device variants:
- **FFBoxDialect**: Flat Field source, protocol V6+ (32-byte status frame, SHT25 temperature sensor, humidity field)
- **AIVFFBoxDialect**: AIV Flat Field source, protocol V4.5 (29-byte status frame, no humidity, different temperature sensor)
- **SPEBoxDialect**: Single Photon Electron source, protocol V6+ (33-byte status frame, SHT25 temperature, centre-LED current)

### 2. Connection Layer
Manages the async TCP socket: connection, authentication, reconnection, flow control, and timeout. Reads fixed-size responses and verifies protocol trailers.

### 3. OPC UA Layer
Builds the OPC UA address space, polls the connection layer, and exposes methods that forward commands to the device.

### OPC UA Node Hierarchy
```
Objects/
  CalibrationLightSource/
    Monitoring/   -- read-only polled variables
    Methods:      Start, Stop, Reboot, Reconnect, GetStatus,
                  SetLeds, SetVoltage, SetDuration, SetFrequency,
                  SetCurrent, SetWidth, SetControl, Configure
```

Node IDs follow the convention: `ns=2;s=CalibrationLightSource.<name>`

## Server Arguments

The server accepts the following command-line arguments:

- `--address`: Device IP address (default: "10.11.4.69")
- `--port`: Device TCP port (default: 50001)
- `--passwd`: Device authentication password (default: "")
- `--product`: **Required**. Device variant: "SPE", "FF", or "AIVFF"
- `--opcua-endpoint`: OPC UA server endpoint URL (default: "opc.tcp://0.0.0.0:4840/nectarcam/")
- `--opcua-namespace`: OPC UA namespace URI (default: auto-generated based on product)
- `--opcua-root`: Root object path in OPC UA address space (default: "CalibrationLightSource")
- `--monitoring-path`: Name of monitoring object node (default: "Monitoring")
- `--opcua-user`: OPC UA authentication (format: USER:PASS, can be specified multiple times)
- `--log-level`: Logging level (default: "INFO", choices: DEBUG, INFO, WARNING, ERROR)
- `--log-file`: Optional log file path (logs also go to stdout)
- `--poll-interval`: Polling interval in seconds (default: 1.0)
- `--auto-reconnect`: Enable automatic reconnection on connection loss
- `--min-cmd-interval`: Minimum interval between commands in seconds (default: 0.0)

## Running the Components

The components can be used together for testing. Below are the commands to run each component with example arguments.

### Light Source Emulator
```bash
python3 nc_lightsource_emulator.py --password HELLO --log-level ERROR --product-code 0xAA
```

**Arguments:**
- `--port`: TCP port to listen on (default: 50001)
- `--product-code`: Product code (0xAA for SPE, 0xA5 for FF, default: 0xAA)
- `--release`: Firmware release number (default: 6)
- `--password`: Authentication password (default: none)
- `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR, default: INFO)

### OPC UA Server
```bash
python3 nc_lightsource_asyncua_server.py --opcua-endpoint opc.tcp://localhost:4840/nectarcam/ --address localhost --passwd HELLO --product SPE --auto-reconnect
```

**Arguments:** See "Server Arguments" section above.

### OPC UA GUI
```bash
python3 nc_lightsource_asyncua_gui.py --endpoint opc.tcp://localhost:4840/nectarcam/
```

**Arguments:**
- `--endpoint`: OPC UA server endpoint URL (default: "opc.tcp://localhost:4840/nectarcam/")
- `--user`: OPC UA username (optional)
- `--password`: OPC UA password (optional)

### Test CLI
```bash
python3 nc_lightsource_asyncua_test_cli.py --endpoint opc.tcp://localhost:4840/nectarcam/
```

**Arguments:**
- `--endpoint`: OPC UA server endpoint URL (default: "opc.tcp://localhost:4840/nectarcam/")
- `--user`: OPC UA username (optional)
- `--password`: OPC UA password (optional)

## Usage Example

To test the system locally:

1. **Terminal 1** - Start the emulator:
   ```bash
   python3 nc_lightsource_emulator.py --password HELLO --product-code 0xAA
   ```

2. **Terminal 2** - Start the OPC UA server:
   ```bash
   python3 nc_lightsource_asyncua_server.py --product SPE --address localhost --passwd HELLO --auto-reconnect
   ```

3. **Terminal 3** - Use the GUI or CLI:
   ```bash
   python3 nc_lightsource_asyncua_gui.py
   ```
   or
   ```bash
   python3 nc_lightsource_asyncua_test_cli.py
   ```




