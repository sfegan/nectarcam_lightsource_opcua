# NectarCAM Calibration Light Source OPC UA Server

This repository provides an OPC UA interface for the NectarCAM calibration light sources (Flat Field and Single Photon Electron variants). It acts as a high-level bridge between the device's low-level TCP protocol and the OPC UA network, enabling standardized monitoring and control.

Derived from the [MOS version](https://gitlab.cta-observatory.org/cta-array-elements/mst/nectarcam/software/cal/calibration-box-mos/) by Patrick Sizun.

Copyright 2026, Stephen Fegan <sfegan@llr.in2p3.fr>
Laboratoire Leprince-Ringuet, CNRS/IN2P3, Ecole Polytechnique, Institut Polytechnique de Paris

---

## 🛠 Components

| File | Description |
| :--- | :--- |
| **`nc_lightsource_asyncua_server.py`** | **The Core Server.** Manages the device connection and publishes the OPC UA address space. |
| **`nc_lightsource_asyncua_gui.py`** | A Python/Tkinter GUI for interactive control and real-time monitoring. |
| **`nc_lightsource_asyncua_test_cli.py`** | A command-line utility for manual method calls and variable inspection. |
| **`nc_lightsource_emulator.py`** | A hardware emulator for offline development and CI testing. |

---

## 🛰 OPC UA Interface

All nodes are located under the root path (default: `Objects/CalibrationLightSource/`).
**NodeId Convention:** `ns=2;s=CalibrationLightSource.[Monitoring.]<Name>`

### 📊 Monitoring Variables
Located in the `Monitoring/` folder. All variables are **Read-Only**.

| Name | Type | Description |
| :--- | :--- | :--- |
| **Connection Status** |
| `device_host` | String | IP address or hostname of the calibration light source device |
| `device_port` | UInt16 | TCP port of the calibration light source device (port 50001 is fixed by firmware) |
| `device_dialect` | String | Device protocol variant: FF (V6+), AIVFF (V4.5), or SPE |
| `device_polling_interval` | Double | Configured poll interval in milliseconds (OPC UA Duration standard) |
| `device_connected` | Boolean | True when the TCP connection to the hardware is active |
| `device_connection_uptime` | Double | Milliseconds since the device last came online; 0.0 while offline (OPC UA Duration standard) |
| `device_connection_downtime` | Double | Milliseconds since the last successful poll; 0.0 while connected (OPC UA Duration standard) |
| `device_state` | Int32 | 0=Offline (not connected), 1=Disabled (connected, idle), 2=Enabled (connected, pulsing) |
| **LED & Pulse Configuration** |
| `led_mask` | UInt64 | 13-bit mask for active LEDs (Bit N = LED N+1, bits 0–12) |
| `voltage_set` | Double | Current voltage setpoint in V (range 7.9–16.5V, controls brightness of LEDs 1–13) |
| `voltage_actual` | Double | Measured LED supply voltage in V |
| `duration` | Int32 | Flash duration in 0.1s units (range 0–1023; 0 = infinite) |
| `frequency_dividend` | Double | Timer base frequency in Hz (range 244.16–10659.56 Hz with divider=1) |
| `frequency_divider` | Int32 | Frequency divider ratio (range 1–3000; use >1 for frequencies below ~300 Hz, minimum 0.1 Hz) |
| `width` | Int32 | Trigger output pulse width in 62.5ns units (range 1–1000, i.e., 62.5 ns–62.5 µs) |
| `light_pulse` | Boolean | Light pulse flashing active (set by Start/Stop commands) |
| **Control Outputs** |
| `lemo_out` | Boolean | LVDS trigger output active |
| `fiber1_out` | Boolean | Optical transmitter No.1 output active |
| `fiber2_out` | Boolean | Optical transmitter No.2 output active |
| `lemo_in` | Boolean | External LEMO trigger input active |
| **Environmental & Health** |
| `temperature` | Double | Internal device temperature in °C |
| `humidity` | Double | Internal relative humidity in %RH (FF only, LSB = 0.04 %RH; always 50.0 for SPE/AIVFF) |
| `faults` | Int64 | Bitmask: D0=Under-V (<8V), D1=Over-V (>30V), D2=Optical TX 1 fault, D3=Optical TX 2 fault |
| **SPE-Specific** |
| `central_current` | Double | SPE center LED (No.7) current in mA (range 0–12 mA, LSB = 38.15 nA; SPE only, always 0.0 for FF/AIVFF) |

### ⚡ Methods
Methods are called directly on the root `CalibrationLightSource` node.

| Method | Arguments | Description |
| :--- | :--- | :--- |
| `Start` | - | Begins light pulsing using current settings. |
| `Stop` | - | Immediately stops light pulsing. |
| `GetStatus` | - | Forces an immediate refresh of all monitoring variables. |
| `Reboot` | - | Triggers a hardware reset of the calibration box. |
| `Reconnect` | - | Drops and re-establishes the TCP connection. |
| `SetLeds` | `mask` (Int32) | Updates the active LED bitmask. |
| `SetVoltage` | `voltage` (Float) | Sets the LED supply voltage (brightness). |
| `SetFrequency`| `dividend` (Float), `divider` (Int32) | Sets the pulse repetition rate. |
| `SetDuration` | `duration` (Int32) | Sets the automatic stop timer (0.1s units). |
| `SetWidth` | `width` (Int32) | Sets the trigger output pulse width. |
| `SetCurrent` | `current_mA` (Float) | Sets the SPE center LED drive current. |
| `SetControl` | `lemo_out`, `fiber1`, `fiber2`, `lemo_in`, `pulse` | Direct control of all boolean hardware flags. |
| `Configure` | *All parameters* | Bulk update of all device settings in a single command. |

---

## 🏗 Architecture

The server employs a robust three-layer design:
1.  **Dialect Layer**: Logic for encoding/decoding frames for **FF**, **AIVFF**, and **SPE** hardware variants.
2.  **Connection Layer**: Resilient async TCP management with auto-reconnect and rate-limiting.
3.  **OPC UA Layer**: Maps the hardware state to the `asyncua` server and manages user permissions.

---

## 🚀 Getting Started

### 1. Installation
Ensure you have Python 3.9+ and the required dependencies:
```bash
pip install asyncua
```

### 2. Running the Server
```bash
python nc_lightsource_asyncua_server.py --product FF --address 10.11.4.69 --auto-reconnect
```

### 3. Command-Line Options

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| **Device Connection** |
| `--product` | Required | - | **Device variant:** `FF` (Flat Field V6+), `AIVFF` (AIV Flat Field V4.5), or `SPE` (Single Photon Electron) |
| `--address` | String | `10.11.4.69` | IP address or hostname of the calibration light source |
| `--port` | Integer | `50001` | TCP port (fixed by device firmware) |
| `--passwd` | String | `""` | Device authentication password (if required) |
| `--auto-reconnect` | Flag | Off | Enable automatic reconnection with exponential back-off on connection loss |
| `--backoff-interval` | Float | `30.0` | Maximum reconnection backoff interval in seconds |
| `--poll-interval` | Float | `1.0` | Polling interval in seconds for device status updates |
| `--min-cmd-interval` | Float | `0.0` | Minimum time in seconds between consecutive device commands (rate limiting) |
| **OPC UA Server** |
| `--opcua-endpoint` | String | `opc.tcp://0.0.0.0:4840/nectarcam/` | OPC UA server endpoint URL |
| `--opcua-namespace` | String | Auto | OPC UA namespace URI (default: `http://cta-observatory.org/nectarcam/calibrationlightsource/<product>`). Include telescope number for multi-telescope deployments, e.g., `http://cta-observatory.org/nectarcam/1/calibrationlightsource/FF` |
| `--opcua-root` | String | `CalibrationLightSource` | Root object path in the OPC UA address space. Dot-separated components create nested browse levels, e.g., `Camera.CalibrationLightSource` creates `Objects/Camera/CalibrationLightSource/` |
| `--monitoring-path` | String | `Monitoring` | Name of the monitoring object node under the root |
| `--opcua-user` | String | None | Add authenticated user (format: `USER:PASS`). Can be specified multiple times for multiple users, e.g., `--opcua-user admin:secret --opcua-user operator:pass123` |
| **Logging** |
| `--log-level` | Choice | `INFO` | Logging verbosity: `DEBUG`, `INFO`, `WARNING`, or `ERROR` |
| `--log-file` | String | None | Write log output to this file in addition to stdout (stdout is always used for container compatibility) |

**Example with Authentication:**
```bash
python nc_lightsource_asyncua_server.py \
  --product SPE \
  --address 192.168.1.100 \
  --auto-reconnect \
  --opcua-user admin:secure_password \
  --opcua-user operator:readonly \
  --log-level DEBUG
```

### 4. Local Testing (Emulator)
You can test the entire stack on your local machine:
```bash
# Terminal 1: Start Emulator
python nc_lightsource_emulator.py --product-code 0xA5

# Terminal 2: Start Server
python nc_lightsource_asyncua_server.py --product FF --address localhost --auto-reconnect

# Terminal 3: Launch GUI
python nc_lightsource_asyncua_gui.py --endpoint opc.tcp://localhost:4840/nectarcam/
```
