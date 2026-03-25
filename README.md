# NectarCAM Calibration Light Source OPC UA Server

This repository provides an OPC UA interface for the NectarCAM calibration light sources (Flat Field and Single Photon Electron variants). It acts as a high-level bridge between the device's low-level TCP protocol and the OPC UA network, enabling standardized monitoring and control.

Derived from the [MOS version](https://gitlab.cta-observatory.org/cta-array-elements/mst/nectarcam/software/cal/calibration-box-mos/) by Patrick Sizun.

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
| `device_connected` | Boolean | True when the TCP connection to the hardware is active. |
| `device_state` | Int32 | 0=Offline, 1=Disabled (Idle), 2=Enabled (Pulsing). |
| `temperature` | Double | Internal device temperature in °C. |
| `humidity` | Double | Internal relative humidity in %RH (FF variant only). |
| `voltage_actual` | Double | Measured LED supply voltage in V. |
| `voltage_set` | Double | Current voltage setpoint in V (7.9–16.5V). |
| `led_mask` | UInt64 | 13-bit mask for active LEDs (Bit N = LED N+1). |
| `duration` | Int32 | Flash duration in 0.1s units (0 = infinite). |
| `frequency_dividend` | Double | Timer base frequency in Hz (~244–10660 Hz). |
| `frequency_divider` | Int32 | Frequency divider ratio (1–3000). |
| `width` | Int32 | Trigger output pulse width in 62.5ns units. |
| `faults` | Int64 | Bitmask: D0=Under-V, D1=Over-V, D2=Fiber1 Fail, D3=Fiber2 Fail. |
| `light_pulse` | Boolean | Status of the internal pulsing engine (Start/Stop). |
| `central_current` | Double | SPE center LED current in mA (0–12 mA). |
| `device_connection_uptime`| Double | Seconds since the device last came online. |
| `device_connection_downtime`| Double | Seconds since the last successful poll (0.0 while connected). |

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

**Common Arguments:**
- `--product`: **Required.** Choose `FF` (Flat Field), `AIVFF` (V4.5), or `SPE` (Single Photon).
- `--address`: IP of the calibration box.
- `--auto-reconnect`: Enables exponential back-off reconnection logic.
- `--opcua-user`: Secure the server (e.g., `--opcua-user admin:password`).

### 3. Local Testing (Emulator)
You can test the entire stack on your local machine:
```bash
# Terminal 1: Start Emulator
python nc_lightsource_emulator.py --product-code 0xA5

# Terminal 2: Start Server
python nc_lightsource_asyncua_server.py --product FF --address localhost --auto-reconnect

# Terminal 3: Launch GUI
python nc_lightsource_asyncua_gui.py --endpoint opc.tcp://localhost:4840/nectarcam/
```
