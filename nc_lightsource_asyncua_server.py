"""
nc_lightsource_asyncua_server.py
NectarCAM Calibration Light Source OPC UA Server

Three-layer architecture
========================

1. Dialect layer  (FFBoxDialect / AIVFFBoxDialect / SPEBoxDialect)
   Pure data classes – no networking, no asyncio.  Each dialect knows only
   how to convert between a BoxState and the raw bytes that travel over the
   wire for its device variant.  Frames are built by concatenating bytes
   objects with +; frames are decoded by progressively chomping a memoryview.

   FFBoxDialect    – Flat Field source, protocol V6+ (software_release >= 6).
                     32-byte status frame; SHT25 temperature sensor; humidity field.
   AIVFFBoxDialect – AIV Flat Field source, protocol V4.5 (software_release == 2).
                     29-byte status frame; no humidity; different temperature sensor
                     (12-bit signed, LSB = 0.0625 °C).
   SPEBoxDialect   – Single Photon Electron source, protocol V6+.
                     33-byte status frame; SHT25 temperature; centre-LED current.

2. Connection layer  (CalBoxConnection)
   Manages the async TCP socket: connect, authenticate, reconnect, flow
   control, timeout.  Reads fixed-size responses (size known from the dialect)
   and verifies the CR LF trailer.

3. OPC UA layer  (CalibrationBoxServer)
   Builds the OPC UA address-space, polls the connection layer, and exposes
   methods that forward to it.

OPC UA node hierarchy
---------------------
    Objects/
      CalibrationLightSource/
        Monitoring/   -- read-only polled variables
        Methods:      Start, Stop, Reboot, Reconnect, GetStatus,
                      SetLeds, SetVoltage, SetDuration, SetFrequency,
                      SetCurrent, SetWidth, SetControl, Configure

NodeId convention: ns=2;s=CalibrationLightSource.<name>
  e.g.  ns=2;s=CalibrationLightSource.GetStatus
        ns=2;s=CalibrationLightSource.Monitoring.temperature

ICD references:
  MST-CAM-ICD-0328-LUPM Ed.1 Rev.3 (June 2024)  – protocol V6 (FF / SPE)
  Définition protocole d'échange sur Ethernet V4-5 (2017-11-16) – protocol V4.5 (AIVFF)
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import functools
import logging
import signal
import socket
import sys
import time
from abc import ABC, abstractmethod
from enum import IntEnum
from typing import Callable, ClassVar, Optional

from asyncua import Server, ua
from asyncua.server.user_managers import UserManager, User, UserRole


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
# Logging is configured at startup in _main() once CLI arguments are parsed,
# so --log-file and --log-level can both take effect before any log output.
# A module-level logger is defined here for use at import time if needed.
log = logging.getLogger("cta.nectarcam.cls.server")

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def _configure_logging(level: str, log_file: str | None) -> None:
    """Configure the root logger.

    Always emits to stdout (preferred for Docker / container log drivers).
    If --log-file is given, messages are also written to that file.
    Timestamps are always included so the output is self-contained whether
    running under Docker, systemd, or interactively.
    """
    formatter = logging.Formatter(_LOG_FORMAT)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)

    handlers = [stdout_handler]
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    logging.basicConfig(level=level, handlers=handlers, force=True)


class _SuppressUaStatusCodeTracebacks(logging.Filter):
    """Downgrade OPC UA method-failure log records; strip their tracebacks."""
    def filter(self, record: logging.LogRecord) -> bool:
        if record.exc_info and isinstance(record.exc_info[1], ua.UaStatusCodeError):
            log.warning("OPC UA method returned Bad status: %s", record.exc_info[1])
            return False
        return True



# ---------------------------------------------------------------------------
# Shared enumerations
# ---------------------------------------------------------------------------
class DeviceState(IntEnum):
    Offline  = 0   # OPC UA server not connected to the source
    Disabled = 1   # Connected, light_pulse is False
    Enabled  = 2   # Connected, light_pulse is True


class Cmd(IntEnum):
    GET_STATUS = 0xE0
    START      = 0xFC
    STOP       = 0xF0
    RESET      = 0xF3


# ---------------------------------------------------------------------------
# BoxState – the common decoded view of both device variants
# ---------------------------------------------------------------------------
@dataclasses.dataclass
class BoxState:
    """
    All device parameters as natural Python values.

    Produced by dialect.decode(), consumed by the OPC UA layer and by
    dialect.encode_configure() when building a ConfigSet command frame.
    Fields that are not meaningful for a particular device (e.g.
    central_current on FF, humidity on SPE) carry a neutral default and are
    simply ignored by the relevant dialect when encoding.
    """

    # LED on/off bitmap – bit N corresponds to LED N+1 (0-based, 13 LEDs)
    led_mask: int = 0x1FFF

    # LED supply voltage to set (V), range [7.9, 16.5]
    voltage_set: float = 10.0

    # Measured LED supply voltage (V) – populated from the status response
    voltage_actual: float = 10.0

    # Flash duration after Start (x 0.1 s; 0 = infinite)
    duration: int = 0

    # Internal timer frequency dividend (Hz), range approx [244.16, 10 659.56]
    frequency_dividend: float = 10000.0

    # Frequency divider ratio, range [1, 3000]
    frequency_divider: int = 1

    # Trigger output pulse width (x 62.5 ns), range [1, 1000]
    width: int = 4

    # Internal temperature (degC)
    temperature: float = 20.0

    # Internal humidity (%RH) – FF only; always 50.0 for SPE
    humidity: float = 50.0

    # Fault flags  D0=under-voltage  D1=over-voltage
    #              D2=optical-tx-1   D3=optical-tx-2
    faults: int = 0

    # Control / status flags
    light_pulse: bool = False   # D0 – pulsing active
    lemo_out:    bool = False   # D1 – LVDS trigger output on
    fiber1_out:  bool = False   # D2 – optical TX 1 on
    fiber2_out:  bool = False   # D3 – optical TX 2 on
    lemo_in:     bool = False   # D4 – external trigger input active

    # SPE only: centre LED (No.7) drive current (mA), range [0, approx 12]
    central_current: float = 0.0

    # Firmware / hardware metadata – populated from the status response
    product_code:     int = 0
    serial_number:    int = 0
    software_release: int = 6

    def as_dict(self) -> dict:
        """Flat dict of all fields – used to populate OPC UA variables."""
        return dataclasses.asdict(self)

    @property
    def device_state(self) -> DeviceState:
        return DeviceState.Enabled if self.light_pulse else DeviceState.Disabled


# ---------------------------------------------------------------------------
# Codec helpers
# ---------------------------------------------------------------------------
# Every multi-byte field in the protocol uses 5-bit payloads packed into bytes
# whose upper 3 bits carry a type tag.  The helpers below operate on plain
# Python ints and return/accept individual byte values or bytes objects, so
# they compose naturally with the + operator.

def _b(value: int, pos: int) -> int:
    """Extract a single bit."""
    return (value >> pos) & 1


def _clamp(name: str, value, lo, hi):
    if not (lo <= value <= hi):
        log.warning("Clamping %s=%s to [%s, %s]", name, value, lo, hi)
        return max(lo, min(hi, value))
    return value


# Type tags embedded in the upper bits of each protocol byte
_TAG_LED  = 0x20   # 0b001x_xxxx
_TAG_VOLT = 0x40   # 0b010x_xxxx
_TAG_TIME = 0x60   # 0b011x_xxxx
_TAG_SENS = 0x80   # 0b100x_xxxx  (temperature / humidity)
_TAG_CTRL = 0xA0   # 0b101x_xxxx
_TAG_META = 0xC0   # 0b110x_xxxx  (serial / release)

_MASK5 = 0x1F


# ---------------------------------------------------------------------------
# Encode helpers  (value → bytes, compose with +)
# ---------------------------------------------------------------------------
# Every multi-byte field uses 5-bit payloads packed into bytes whose upper
# 3 bits carry a type tag.

def _enc10(tag: int, code: int) -> bytes:
    return bytes([tag | ((code >> 5) & _MASK5),
                  tag | ( code       & _MASK5)])

def _enc15(tag: int, code: int) -> bytes:
    return bytes([tag | ((code >> 10) & _MASK5),
                  tag | ((code >>  5) & _MASK5),
                  tag | ( code        & _MASK5)])

def _enc20(tag: int, code: int) -> bytes:
    return bytes([tag | ((code >> 15) & _MASK5),
                  tag | ((code >> 10) & _MASK5),
                  tag | ((code >>  5) & _MASK5),
                  tag | ( code        & _MASK5)])


def _enc_voltage_set(volts: float) -> bytes:
    volts = _clamp("voltage_set", volts, 7.9, 16.5)
    return _enc10(_TAG_VOLT, int((volts - 7.9) * 950 / 8.44))

def _enc_duration(n: int) -> bytes:
    return _enc10(_TAG_TIME, _clamp("duration", n, 0, 1023))

_FREQ_SCALE    = 1e10 / 625          # = 16 000 000
_FREQ_CODE_MIN = 1500
_FREQ_CODE_MAX = 65530
_FREQ_HZ_MIN   = _FREQ_SCALE / (_FREQ_CODE_MAX + 1)
_FREQ_HZ_MAX   = _FREQ_SCALE / (_FREQ_CODE_MIN + 1)

def _enc_frequency(hz: float) -> bytes:
    hz = _clamp("frequency_dividend", hz, _FREQ_HZ_MIN, _FREQ_HZ_MAX)
    return _enc20(_TAG_TIME, int(round(_FREQ_SCALE / hz - 1)))

def _enc_divider(n: int) -> bytes:
    return _enc15(_TAG_TIME, _clamp("frequency_divider", n, 1, 3000) - 1)

def _enc_width(n: int) -> bytes:
    return _enc10(_TAG_TIME, _clamp("width", n, 1, 1000))

def _enc_control(state: BoxState) -> bytes:
    return bytes([_TAG_CTRL
                  | int(state.light_pulse)
                  | (int(state.lemo_out)   << 1)
                  | (int(state.fiber1_out) << 2)
                  | (int(state.fiber2_out) << 3)
                  | (int(state.lemo_in)    << 4)])

_CURRENT_STEP_MA = 0.0003814697 # 12.0 / 31457

def _enc_central_current(mA: float) -> bytes:
    mA = _clamp("central_current", mA, 0.0, 31457 * _CURRENT_STEP_MA)
    return _enc20(_TAG_VOLT, int(mA / _CURRENT_STEP_MA))


# ---------------------------------------------------------------------------
# Decode helpers  (memoryview → (value, remaining_memoryview))
# ---------------------------------------------------------------------------
# Each function consumes exactly the bytes it needs from the front of mv and
# returns the decoded value paired with the remaining view.  Callers chain
# them as:  value, mv = _dec_*(mv)

def _dec_voltage_set(mv: memoryview) -> tuple[float, memoryview]:
    code = ((mv[0] & _MASK5) << 5) | (mv[1] & _MASK5)
    return 7.9 + code * 8.44 / 950, mv[2:]

def _dec_voltage_actual(mv: memoryview) -> tuple[float, memoryview]:
    code = ((mv[0] & _MASK5) << 5) | (mv[1] & _MASK5)
    return 0.2 + code * 12.5 * 3.3 / 1023, mv[2:]

def _dec_duration(mv: memoryview) -> tuple[int, memoryview]:
    return ((mv[0] & _MASK5) << 5) | (mv[1] & _MASK5), mv[2:]

def _dec_frequency(mv: memoryview) -> tuple[float, memoryview]:
    code = (((mv[0] & _MASK5) << 15) | ((mv[1] & _MASK5) << 10)
           | ((mv[2] & _MASK5) <<  5) |  (mv[3] & _MASK5))
    return _FREQ_SCALE / (code + 1), mv[4:]

def _dec_divider(mv: memoryview) -> tuple[int, memoryview]:
    code = ((mv[0] & _MASK5) << 10) | ((mv[1] & _MASK5) << 5) | (mv[2] & _MASK5)
    return code + 1, mv[3:]

def _dec_width(mv: memoryview) -> tuple[int, memoryview]:
    return ((mv[0] & _MASK5) << 5) | (mv[1] & _MASK5), mv[2:]

def _dec_temperature(mv: memoryview) -> tuple[float, memoryview]:
    # SHT25 (protocol V6+): unsigned 12-bit code, negative temps emerge naturally
    # from the linear formula. No sign bit.
    code = ((mv[0] & 0x03) << 10) | ((mv[1] & _MASK5) << 5) | (mv[2] & _MASK5)
    return code * 175.72 / 4096 - 46.85, mv[3:]

def _dec_temperature_v45(mv: memoryview) -> tuple[float, memoryview]:
    # Protocol V4.5 (AIVFF) – formula from CalBoxConfig.cpp L616:
    #   abs_value = 32.0    * (C18 & 0x03)   # C18 2 LSB bits → integer degrees x32
    #             +  1.0    * (C19 & 0x1F)   # C19 5 LSB bits → integer degrees
    #             +  0.0625 * (C20 & 0x1F)   # C20 5 LSB bits → fractional degrees
    # Sign is carried in bit 2 of C18 (same position as the SHT25 encoder).
    sign      = -1.0 if (mv[0] & 0x04) else 1.0
    abs_value = 32.0 * (mv[0] & 0x03) + 1.0 * (mv[1] & _MASK5) + 0.0625 * (mv[2] & _MASK5)
    return sign * abs_value, mv[3:]

def _dec_faults(mv: memoryview) -> tuple[int, memoryview]:
    return mv[0] & _MASK5, mv[1:]

def _dec_control(mv: memoryview) -> tuple[tuple[bool, bool, bool, bool, bool], memoryview]:
    """Return ((light_pulse, lemo_out, fiber1_out, fiber2_out, lemo_in), remaining)."""
    b = mv[0]
    return (bool(b & 0x01), bool(b & 0x02),
            bool(b & 0x04), bool(b & 0x08), bool(b & 0x10)), mv[1:]

def _dec_humidity(mv: memoryview) -> tuple[float, memoryview]:
    # ICD section 3.6: LSB = 0.04 %RH
    code = ((mv[0] & 0x03) << 10) | ((mv[1] & _MASK5) << 5) | (mv[2] & _MASK5)
    return code * 125 / 4096.0 - 6.0, mv[3:]

def _dec_central_current(mv: memoryview) -> tuple[float, memoryview]:
    code = (((mv[0] & _MASK5) << 15) | ((mv[1] & _MASK5) << 10)
           | ((mv[2] & _MASK5) <<  5) |  (mv[3] & _MASK5))
    return code * _CURRENT_STEP_MA, mv[4:]

def _dec_metadata(mv: memoryview) -> tuple[tuple[int, int, int], memoryview]:
    """Decode (product_code, serial_number, software_release); leave CR LF in remainder."""
    product_code     = mv[0]
    serial_number    = (mv[1] & _MASK5) | ((mv[2] & _MASK5) << 5)
    software_release = mv[3] & _MASK5
    return (product_code, serial_number, software_release), mv[4:]


# ---------------------------------------------------------------------------
# Dialect base class
# ---------------------------------------------------------------------------
class _BoxDialect(ABC):
    """
    Knows how to build command frames and decode status frames for one device
    variant.  Stateless – safe to share across connections.

    encode_simple_cmd(cmd) -> 4-byte frame (GetStatus / Start / Stop / Reset)
    encode_configure(state) -> ConfigSet frame assembled with bytes +
    decode(raw)             -> chomps a memoryview of the status bytes -> BoxState
    """

    NAME:         ClassVar[str]
    PRODUCT_CODE: ClassVar[int]
    CMD_SIZE:     ClassVar[int]    # total bytes in a ConfigSet command
    STATUS_SIZE:  ClassVar[int]    # total bytes in a status response (incl. CR LF)

    def encode_simple_cmd(self, cmd: Cmd) -> bytes:
        return bytes([int(cmd), self.PRODUCT_CODE, 0x0D, 0x0A])

    @abstractmethod
    def encode_configure(self, state: BoxState) -> bytes: ...

    @abstractmethod
    def decode(self, raw: bytes) -> BoxState: ...


# ---------------------------------------------------------------------------
# FF dialect  (product code 0xA5)
# ---------------------------------------------------------------------------
class FFBoxDialect(_BoxDialect):
    """
    Flat Field Calibration Light Source.

    Status response: 32 bytes total (including CR LF)
    ConfigSet command: 20 bytes total (including CR LF)

    LED wire layout (ICD sections 3.2 / 3.4) – C0-C2, bits 4-0:
      C0: LED6  LED2  LED4  LED7  LED12   (0-based LED indices)
      C1: LED10 LED5  LED3  LED0  LED1
      C2: --    --    LED9  LED11 LED8

    Status frame layout (32 bytes):
      C0-C2   LED on/off
      C3-C4   voltage set
      C5-C6   voltage measured
      C7-C8   duration
      C9-C12  frequency dividend
      C13-C15 frequency divider
      C16-C17 pulse width
      C18-C20 temperature
      C21     fault flags
      C22     control flags
      C23-C25 humidity
      C26     product code (0xA5)
      C27     serial number LSB
      C28     serial number MSB
      C29     software release
      C30     CR (0x0D)
      C31     LF (0x0A)

    ConfigSet command layout (20 bytes):
      C0-C2   LED on/off
      C3-C4   voltage set
      C5-C6   duration
      C7-C10  frequency dividend
      C11-C13 frequency divider
      C14-C15 pulse width
      C16     control flags
      C17     product code (0xA5)
      C18     CR (0x0D)
      C19     LF (0x0A)
    """

    NAME         = "FF"
    PRODUCT_CODE = 0xA5
    CMD_SIZE     = 20
    STATUS_SIZE  = 32

    def _enc_led_mask(self, mask: int) -> bytes:
        return bytes([
            _TAG_LED | (_b(mask,6)<<4) | (_b(mask,2)<<3) | (_b(mask,4)<<2) | (_b(mask,7)<<1) | _b(mask,12),
            _TAG_LED | (_b(mask,10)<<4) | (_b(mask,5)<<3) | (_b(mask,3)<<2) | (_b(mask,0)<<1) | _b(mask,1),
            _TAG_LED | (_b(mask,9)<<2) | (_b(mask,11)<<1) | _b(mask,8),
        ])

    def _dec_led_mask(self, mv: memoryview) -> tuple:
        mask  = _b(mv[0], 0) << 12
        mask |= _b(mv[2], 1) << 11
        mask |= _b(mv[1], 4) << 10
        mask |= _b(mv[2], 2) << 9
        mask |= _b(mv[2], 0) << 8
        mask |= _b(mv[0], 1) << 7
        mask |= _b(mv[0], 4) << 6
        mask |= _b(mv[1], 3) << 5
        mask |= _b(mv[0], 2) << 4
        mask |= _b(mv[1], 2) << 3
        mask |= _b(mv[0], 3) << 2
        mask |= _b(mv[1], 0) << 1
        mask |= _b(mv[1], 1)
        return mask, mv[3:]

    def encode_configure(self, state: BoxState) -> bytes:
        return (
            self._enc_led_mask(state.led_mask)             # 3 bytes  C0-C2
            + _enc_voltage_set(state.voltage_set)          # 2 bytes  C3-C4
            + _enc_duration(state.duration)                # 2 bytes  C5-C6
            + _enc_frequency(state.frequency_dividend)     # 4 bytes  C7-C10
            + _enc_divider(state.frequency_divider)        # 3 bytes  C11-C13
            + _enc_width(state.width)                      # 2 bytes  C14-C15
            + _enc_control(state)                          # 1 byte   C16
            + bytes([self.PRODUCT_CODE, 0x0D, 0x0A])       # 3 bytes  C17-C19
        )

    def decode(self, raw: bytes) -> BoxState:
        s = BoxState()
        mv = memoryview(raw)
        s.led_mask,           mv = self._dec_led_mask(mv)  # C0-C2
        s.voltage_set,        mv = _dec_voltage_set(mv)    # C3-C4
        s.voltage_actual,     mv = _dec_voltage_actual(mv) # C5-C6
        s.duration,           mv = _dec_duration(mv)       # C7-C8
        s.frequency_dividend, mv = _dec_frequency(mv)      # C9-C12
        s.frequency_divider,  mv = _dec_divider(mv)        # C13-C15
        s.width,              mv = _dec_width(mv)          # C16-C17
        s.temperature,        mv = _dec_temperature(mv)    # C18-C20
        s.faults,             mv = _dec_faults(mv)         # C21
        (s.light_pulse, s.lemo_out,
         s.fiber1_out, s.fiber2_out, s.lemo_in), mv = _dec_control(mv)  # C22
        s.humidity,           mv = _dec_humidity(mv)       # C23-C25
        (s.product_code, s.serial_number,
         s.software_release), mv = _dec_metadata(mv)       # C26-C29
        if bytes(mv) != b"\r\n":
            raise ValueError(f"FF frame: expected CR LF trailer, got {bytes(mv).hex().upper()!r}")
        return s


# ---------------------------------------------------------------------------
# AIVFF dialect  (product code 0xA5, protocol V4.5)
# ---------------------------------------------------------------------------
class AIVFFBoxDialect(FFBoxDialect):
    """
    AIV Flat Field Calibration Light Source – protocol V4.5.

    Identical wire layout to FFBoxDialect for commands and for the first 22
    bytes of the status response, with two differences:

    1. No humidity field: the 3 humidity bytes (C23-C25 in V6) are absent.
    2. Different temperature sensor: 12-bit signed integer, LSB = 0.0625 °C
       (see _dec_temperature_v45).  The SHT25 formula used by FFBoxDialect
       does NOT apply here.

    Status response: 29 bytes total (including CR LF)
      C0-C2   LED on/off
      C3-C4   voltage set
      C5-C6   voltage measured
      C7-C8   duration
      C9-C12  frequency dividend
      C13-C15 frequency divider
      C16-C17 pulse width
      C18-C20 temperature  (12-bit, LSB=0.0625°C)
      C21     fault flags
      C22     control flags
      C23     product code (0xA5)
      C24     serial number LSB
      C25     serial number MSB
      C26     software release  (== 2 for V4.5)
      C27     CR (0x0D)
      C28     LF (0x0A)

    ConfigSet command layout: identical to FFBoxDialect (20 bytes).
    """

    NAME        = "AIVFF"
    STATUS_SIZE = 29     # 3 bytes shorter than FF V6: no humidity

    def decode(self, raw: bytes) -> BoxState:
        s = BoxState()
        mv = memoryview(raw)
        s.led_mask,           mv = self._dec_led_mask(mv)     # C0-C2
        s.voltage_set,        mv = _dec_voltage_set(mv)       # C3-C4
        s.voltage_actual,     mv = _dec_voltage_actual(mv)    # C5-C6
        s.duration,           mv = _dec_duration(mv)          # C7-C8
        s.frequency_dividend, mv = _dec_frequency(mv)         # C9-C12
        s.frequency_divider,  mv = _dec_divider(mv)           # C13-C15
        s.width,              mv = _dec_width(mv)             # C16-C17
        s.temperature,        mv = _dec_temperature_v45(mv)   # C18-C20  (V4.5 sensor)
        s.faults,             mv = _dec_faults(mv)            # C21
        (s.light_pulse, s.lemo_out,
         s.fiber1_out, s.fiber2_out, s.lemo_in), mv = _dec_control(mv)  # C22
        # No humidity in V4.5 – leave s.humidity at its default (50.0)
        (s.product_code, s.serial_number,
         s.software_release), mv = _dec_metadata(mv)          # C23-C26
        if bytes(mv) != b"\r\n":
            raise ValueError(f"AIVFF frame: expected CR LF trailer, got {bytes(mv).hex().upper()!r}")
        if s.software_release != 2:
            log.warning(
                "AIVFF: expected software_release=2 (V4.5), got %d "
                "– wrong --product flag?", s.software_release,
            )
        return s


class SPEBoxDialect(_BoxDialect):
    """
    Single Photon Electron Calibration Light Source.

    Status response: 33 bytes total (including CR LF)
    ConfigSet command: 24 bytes total (including CR LF)

    LED wire layout (ICD sections 3.3 / 3.5) – C0-C2, bits 4-0:
      C0: LED9  LED8  LED12 LED11 LED10   (0-based LED indices)
      C1: LED4  LED3  LED7  LED6  LED5
      C2: --    --    LED2  LED1  LED0

    Status frame layout (33 bytes):
      C0-C2   LED on/off
      C3-C4   voltage set
      C5-C6   voltage measured
      C7-C8   duration
      C9-C12  frequency dividend
      C13-C15 frequency divider
      C16-C17 pulse width
      C18-C20 temperature
      C21     fault flags
      C22     control flags
      C23-C26 centre LED current
      C27     product code (0xAA)
      C28     serial number LSB  (note: ICD has a typo repeating C24/C25/C26/C27)
      C29     serial number MSB
      C30     software release
      C31     CR (0x0D)
      C32     LF (0x0A)

    ConfigSet command layout (24 bytes):
      C0-C2   LED on/off
      C3-C4   voltage set
      C5-C6   duration
      C7-C10  frequency dividend
      C11-C13 frequency divider
      C14-C15 pulse width
      C16     control flags
      C17-C20 centre LED current
      C21     product code (0xAA)
      C22     CR (0x0D)
      C23     LF (0x0A)
    """

    NAME         = "SPE"
    PRODUCT_CODE = 0xAA
    CMD_SIZE     = 24
    STATUS_SIZE  = 33

    def _enc_led_mask(self, mask: int) -> bytes:
        return bytes([
            _TAG_LED | (_b(mask,9)<<4) | (_b(mask,8)<<3) | (_b(mask,12)<<2) | (_b(mask,11)<<1) | _b(mask,10),
            _TAG_LED | (_b(mask,4)<<4) | (_b(mask,3)<<3) | (_b(mask,7)<<2)  | (_b(mask,6)<<1)  | _b(mask,5),
            _TAG_LED | (_b(mask,2)<<2) | (_b(mask,1)<<1) | _b(mask,0),
        ])

    def _dec_led_mask(self, mv: memoryview) -> tuple:
        mask  = _b(mv[0], 2) << 12
        mask |= _b(mv[0], 1) << 11
        mask |= _b(mv[0], 0) << 10
        mask |= _b(mv[0], 4) << 9
        mask |= _b(mv[0], 3) << 8
        mask |= _b(mv[1], 2) << 7
        mask |= _b(mv[1], 1) << 6
        mask |= _b(mv[1], 0) << 5
        mask |= _b(mv[1], 4) << 4
        mask |= _b(mv[1], 3) << 3
        mask |= _b(mv[2], 2) << 2
        mask |= _b(mv[2], 1) << 1
        mask |= _b(mv[2], 0)
        return mask, mv[3:]

    def encode_configure(self, state: BoxState) -> bytes:
        return (
            self._enc_led_mask(state.led_mask)             # 3 bytes  C0-C2
            + _enc_voltage_set(state.voltage_set)          # 2 bytes  C3-C4
            + _enc_duration(state.duration)                # 2 bytes  C5-C6
            + _enc_frequency(state.frequency_dividend)     # 4 bytes  C7-C10
            + _enc_divider(state.frequency_divider)        # 3 bytes  C11-C13
            + _enc_width(state.width)                      # 2 bytes  C14-C15
            + _enc_control(state)                          # 1 byte   C16
            + _enc_central_current(state.central_current)  # 4 bytes  C17-C20
            + bytes([self.PRODUCT_CODE, 0x0D, 0x0A])       # 3 bytes  C21-C23
        )

    def decode(self, raw: bytes) -> BoxState:
        s = BoxState()
        mv = memoryview(raw)
        s.led_mask,           mv = self._dec_led_mask(mv)  # C0-C2
        s.voltage_set,        mv = _dec_voltage_set(mv)    # C3-C4
        s.voltage_actual,     mv = _dec_voltage_actual(mv) # C5-C6
        s.duration,           mv = _dec_duration(mv)       # C7-C8
        s.frequency_dividend, mv = _dec_frequency(mv)      # C9-C12
        s.frequency_divider,  mv = _dec_divider(mv)        # C13-C15
        s.width,              mv = _dec_width(mv)          # C16-C17
        s.temperature,        mv = _dec_temperature_v45(mv)  # C18-C20 <- SPE seemingly uses same sensor as AIVFF, not FF
        s.faults,             mv = _dec_faults(mv)         # C21
        (s.light_pulse, s.lemo_out,
         s.fiber1_out, s.fiber2_out, s.lemo_in), mv = _dec_control(mv)  # C22
        s.central_current,    mv = _dec_central_current(mv) # C23-C26
        (s.product_code, s.serial_number,
         s.software_release), mv = _dec_metadata(mv)       # C27-C30
        if bytes(mv) != b"\r\n":
            raise ValueError(f"SPE frame: expected CR LF trailer, got {bytes(mv).hex().upper()!r}")
        return s


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------
def make_dialect(product: str) -> _BoxDialect:
    if product.upper() == "FF":
        return FFBoxDialect()
    if product.upper() == "AIVFF":
        return AIVFFBoxDialect()
    if product.upper() == "SPE":
        return SPEBoxDialect()
    raise ValueError(f"Unknown product {product!r} – expected 'FF', 'AIVFF', or 'SPE'")


# ---------------------------------------------------------------------------
# Connection layer
# ---------------------------------------------------------------------------
class CalBoxConnection:
    """
    Async TCP connection to a calibration box.

    Uses asyncio streams throughout – no blocking socket calls, no
    run_in_executor.  Concurrent callers are serialised by an asyncio.Lock.

    Responses are read as fixed-size chunks (dialect.STATUS_SIZE bytes), so
    there is no need to search for a delimiter.  The CR LF trailer is verified
    on arrival; any mismatch raises OSError and triggers the offline path.

    Public API
    ----------
    await connect()                                    -> bool
    await authenticate()                               -> bool
    await close()
    await get_status()                                 -> BoxState
    await start()                                      -> BoxState
    await stop()                                       -> BoxState
    await reboot()
    await configure(state: BoxState)                   -> BoxState
    await modify(fn: Callable[[BoxState], None])       -> BoxState
    is_connected                                       -> bool  (property)
    """

    CONNECT_TIMEOUT = 5.0    # seconds
    IO_TIMEOUT      = 2.0    # seconds
    POST_AUTH_DELAY = 0.1    # seconds to wait after auth before first command

    def __init__(
        self,
        host:             str,
        port:             int,
        dialect:          _BoxDialect,
        password:         str   = "",
        auto_reconnect:   bool  = False,
        min_cmd_interval: float = 0.0,
    ):
        self.host             = host
        self.port             = port
        self.dialect          = dialect
        self.password         = password
        self.auto_reconnect   = auto_reconnect
        self.min_cmd_interval = min_cmd_interval

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._lock               = asyncio.Lock()
        self._link_ok            = False
        self._earliest_cmd_time  = 0.0

    @property
    def is_connected(self) -> bool:
        return self._link_ok and self._writer is not None

    # ----------------------------------------------------------------
    # Connect / authenticate / close
    # ----------------------------------------------------------------

    async def connect(self) -> bool:
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=self.CONNECT_TIMEOUT,
            )
            sock = self._writer.transport.get_extra_info("socket")
            if sock:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            return True
        except (OSError, asyncio.TimeoutError) as exc:
            log.debug("Connection to %s:%d failed: %s", self.host, self.port, exc)
            self._reader = self._writer = None
            return False

    async def authenticate(self) -> bool:
        if not self._writer:
            return False
        try:
            if self.password:
                prompt = await self._recv_exactly(13)
                log.debug("Auth prompt: %r", prompt)
                self._writer.write((self.password + "\r\n").encode())
                await self._writer.drain()
                await self._recv_exactly(2)   # echo / ACK
            self._link_ok = True
            self._earliest_cmd_time = time.monotonic() + self.POST_AUTH_DELAY
            return True
        except (OSError, asyncio.TimeoutError) as exc:
            log.debug("Authentication failed: %s", exc)
            self._link_ok = False
            return False

    async def close(self):
        self._link_ok = False
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except OSError:
                pass
            self._reader = self._writer = None

    # ----------------------------------------------------------------
    # Public command API
    # ----------------------------------------------------------------

    async def get_status(self) -> BoxState:
        async with self._lock:
            await self._ensure_connected()
            await self._rate_limit()
            return await self._exchange(self.dialect.encode_simple_cmd(Cmd.GET_STATUS))

    async def start(self) -> BoxState:
        async with self._lock:
            await self._ensure_connected()
            await self._rate_limit()
            log.info("Starting light source")
            return await self._exchange(self.dialect.encode_simple_cmd(Cmd.START))

    async def stop(self) -> BoxState:
        async with self._lock:
            await self._ensure_connected()
            await self._rate_limit()
            log.info("Stopping light source")
            return await self._exchange(self.dialect.encode_simple_cmd(Cmd.STOP))

    async def reboot(self):
        async with self._lock:
            await self._ensure_connected()
            await self._rate_limit()
            log.info("Rebooting device")
            await self._send(self.dialect.encode_simple_cmd(Cmd.RESET))
            # Reset yields no response

    async def configure(self, state: BoxState) -> BoxState:
        """Send a full ConfigSet frame and return the resulting BoxState."""
        async with self._lock:
            await self._ensure_connected()
            await self._rate_limit()
            return await self._exchange(self.dialect.encode_configure(state))

    async def modify(self, fn: Callable[[BoxState], None]) -> BoxState:
        """
        Atomically fetch current state, apply fn(state) in-place, send the
        updated ConfigSet, and return the device's fresh response.
        """
        async with self._lock:
            await self._ensure_connected()
            await self._rate_limit()
            state = await self._exchange(self.dialect.encode_simple_cmd(Cmd.GET_STATUS))
            fn(state)
            return await self._exchange(self.dialect.encode_configure(state))

    # ----------------------------------------------------------------
    # Internal helpers
    # ----------------------------------------------------------------

    async def _ensure_connected(self):
        if not self.is_connected:
            raise OSError("Not connected")

    async def _rate_limit(self):
        wait = self._earliest_cmd_time - time.monotonic()
        if wait > 0:
            log.debug("Rate-limit: sleeping %.3f s", wait)
            await asyncio.sleep(wait)
        if self.min_cmd_interval > 0:
            self._earliest_cmd_time = time.monotonic() + self.min_cmd_interval

    async def _send(self, data: bytes):
        log.debug("-> %d B: %s", len(data), data.hex().upper())
        self._writer.write(data)
        await self._writer.drain()

    async def _recv_exactly(self, n: int) -> bytes:
        try:
            return await asyncio.wait_for(
                self._reader.readexactly(n), timeout=self.IO_TIMEOUT,
            )
        except asyncio.IncompleteReadError as exc:
            raise OSError("Connection closed mid-read") from exc
        except asyncio.TimeoutError as exc:
            raise OSError(f"Timeout reading {n} bytes") from exc

    async def _recv_status(self) -> bytes:
        """Read exactly STATUS_SIZE bytes and verify the CR LF trailer."""
        raw = await self._recv_exactly(self.dialect.STATUS_SIZE)
        if raw[-2:] != b"\r\n":
            raise OSError(
                f"Status frame has no CR LF trailer: "
                f"last 2 bytes are {raw[-2:].hex().upper()!r}"
            )
        log.debug("<- %d B: %s", len(raw), raw.hex().upper())
        return raw

    async def _exchange(self, cmd: bytes) -> BoxState:
        """Send cmd, receive a fixed-size status response, decode and return it."""
        await self._send(cmd)
        raw   = await self._recv_status()
        state = self.dialect.decode(raw)
        if state.product_code != self.dialect.PRODUCT_CODE:
            log.warning(
                "Product-code mismatch: configured 0x%02X but device reports "
                "0x%02X -- verify the --product flag",
                self.dialect.PRODUCT_CODE, state.product_code,
            )
        log.info("%s status: enabled=%s leds=0x%04x V=%.2f T=%.1f faults=0x%02x",
                 self.dialect.NAME,
                 state.light_pulse, state.led_mask, state.voltage_set, 
                 state.temperature, state.faults)
        return state


# ---------------------------------------------------------------------------
# OPC UA helper decorator
# ---------------------------------------------------------------------------
def _unwrap_variants(fn):
    """Unwrap ua.Variant arguments before calling the OPC UA method body."""
    @functools.wraps(fn)
    async def wrapper(self, parent, *args):
        return await fn(self, parent,
                        *(a.Value if isinstance(a, ua.Variant) else a for a in args))
    return wrapper


# ---------------------------------------------------------------------------
# OPC UA layer
# ---------------------------------------------------------------------------
class CalibrationBoxServer:
    """
    OPC UA server that wraps CalBoxConnection and exposes the full
    NectarCAM calibration light source interface.

    Node hierarchy (example with default paths):
        Objects/
          CalibrationLightSource/        (--opcua-root)
            Monitoring/                  (--monitoring-path)
              temperature, humidity, ...
            Start, Stop, GetStatus, ...  (methods)

    Multi-level roots are supported, e.g. --opcua-root Camera.CalibrationLightSource
    creates Objects/Camera/CalibrationLightSource/...

    NodeId convention: ns=2;s=<root>.<monitoring-path>.<var>
      e.g. (defaults) ns=2;s=CalibrationLightSource.Monitoring.temperature
                      ns=2;s=CalibrationLightSource.GetStatus
    """

    _DEFAULT_ROOT            = "CalibrationLightSource"
    _DEFAULT_MONITORING_PATH = "Monitoring"

    # (name, initial value, OPC UA variant type, description)
    # Each entry becomes a variable node at:
    #   ns=2;s=CalibrationLightSource.Monitoring.<n>
    # host, port, dialect are static connection-identity variables written
    # once at startup; the remainder are polled live from the device.
    # Descriptions sourced from ICD MST-CAM-ICD-0328-LUPM Ed.1 Rev.3 section 3.6.
    _MONITORING_VARS: ClassVar[list[tuple]] = [
        ("host",               "",      ua.VariantType.String,
         "IP address or hostname of the calibration light source device"),
        ("port",               0,       ua.VariantType.Int64,
         "TCP port of the calibration light source device (port 50001 is fixed by the device firmware)"),
        ("dialect",            "",      ua.VariantType.String,
         "Device protocol variant: FF (V6+), AIVFF (V4.5), or SPE"),
        ("led_mask",           8191,    ua.VariantType.Int64,
         "LED on/off bitmask: bit N = LED N+1 (bits 0-12, 13 LEDs total)"),
        ("voltage_set",        10.0,    ua.VariantType.Double,
         "LED supply voltage setpoint in V (range 7.9-16.5 V, controls brightness of LEDs 1-13)"),
        ("voltage_actual",     10.0,    ua.VariantType.Double,
         "LED supply voltage measured by the device in V"),
        ("duration",           0,       ua.VariantType.Int64,
         "Flash duration after Start in units of 0.1 s (range 0-1023; 0 = infinite until Stop)"),
        ("frequency_dividend", 10000.0, ua.VariantType.Double,
         "Flash frequency dividend in Hz (range 244.16-10659.56 Hz with divider=1)"),
        ("frequency_divider",  1,       ua.VariantType.Int64,
         "Flash frequency divider ratio (range 1-3000; use > 1 for frequencies below ~300 Hz, minimum 0.1 Hz)"),
        ("width",              1,       ua.VariantType.Int64,
         "Trigger output pulse width in units of 62.5 ns (range 1-1000, i.e. 62.5 ns to 62.5 us)"),
        ("temperature",        20.0,    ua.VariantType.Double,
         "Internal temperature of the calibration light source in degrees C"),
        ("humidity",           50.0,    ua.VariantType.Double,
         "Internal relative humidity in %RH (FF only; LSB = 0.04 %RH; always 50.0 for SPE/AIVFF)"),
        ("faults",             0,       ua.VariantType.Int64,
         "Fault flags bitmask: D0=power supply under-voltage (<8 V), D1=over-voltage (>30 V), "
         "D2=optical transmitter 1 fault, D3=optical transmitter 2 fault"),
        ("light_pulse",        False,   ua.VariantType.Boolean,
         "Light pulse flashing active (D0 of control byte; set by Start/Stop commands)"),
        ("lemo_out",           False,   ua.VariantType.Boolean,
         "LVDS trigger output active (D1 of control byte)"),
        ("fiber1_out",         False,   ua.VariantType.Boolean,
         "Optical transmitter No.1 output active (D2 of control byte)"),
        ("fiber2_out",         False,   ua.VariantType.Boolean,
         "Optical transmitter No.2 output active (D3 of control byte)"),
        ("lemo_in",            False,   ua.VariantType.Boolean,
         "External LEMO trigger input active (D4 of control byte)"),
        ("central_current",    0.0,     ua.VariantType.Double,
         "Centre LED No.7 drive current in mA (range 0-12 mA, LSB = 38.15 nA; SPE only, always 0.0 for FF/AIVFF)"),
        ("cls_state",          0,       ua.VariantType.Int64,
         "Server connection state: 0=Offline (not connected to device), "
         "1=Disabled (connected, light pulse off), 2=Enabled (connected, light pulse on)"),
    ]

    def __init__(
        self,
        host:             str   = "10.11.4.69",
        port:             int   = 50001,
        password:         str   = "",
        product:          str   = "FF",
        opcua_endpoint:   str   = "opc.tcp://0.0.0.0:4840/nectarcam/",
        opcua_namespace:  str   = None,
        opcua_root:       str   = None,
        monitoring_path:  str   = None,
        opcua_users:      dict  = None,
        poll_interval:    float = 1.0,
        auto_reconnect:   bool  = False,
        min_cmd_interval: float = 0.0,
    ):
        self.opcua_users      = opcua_users or {}
        self.poll_interval    = poll_interval
        self.auto_reconnect   = auto_reconnect
        # Default namespace encodes the product; caller may override via
        # --opcua-namespace to add telescope number, site, etc.
        self.opcua_namespace  = (opcua_namespace or
            f"http://cta-observatory.org/nectarcam/calibrationlightsource"
            f"/{product.lower()}")
        # Root path may be multi-level (e.g. "Camera.CalibrationLightSource").
        # Each dot-separated component becomes one browse level in Objects/.
        self.opcua_root       = opcua_root or self._DEFAULT_ROOT
        # Path of the monitoring object relative to the leaf root node.
        self.monitoring_path  = monitoring_path or self._DEFAULT_MONITORING_PATH

        self.connection = CalBoxConnection(
            host=host, port=port,
            dialect=make_dialect(product),
            password=password,
            auto_reconnect=auto_reconnect,
            min_cmd_interval=min_cmd_interval,
        )
        self.device_state = DeviceState.Offline
        self._vars: dict  = {}
        self.server       = self._build_server(opcua_endpoint)

    # ----------------------------------------------------------------
    # OPC UA server construction
    # ----------------------------------------------------------------

    def _build_server(self, endpoint: str) -> Server:
        user_mgr = None
        if self.opcua_users:
            creds = self.opcua_users
            class _Manager(UserManager):
                async def get_user(self, iserver, username=None, password=None,
                                   certificate=None):
                    return (User(role=UserRole.User)
                            if creds.get(username) == password else None)
            user_mgr = _Manager()

        srv = Server(user_manager=user_mgr)
        srv.set_endpoint(endpoint)
        srv.set_server_name("NectarCAM CalibrationLightSource")
        if self.opcua_users:
            srv.set_security_IDs(["Anonymous", "Username"])
        logging.getLogger("asyncua.server.address_space").addFilter(
            _SuppressUaStatusCodeTracebacks()
        )
        logging.getLogger("asyncua.server.address_space").setLevel(logging.WARNING)
        return srv

    async def _init_server(self):
        # shelf_file=None disables asyncua's address-space persistence cache.
        # Without this, nodes added in a previous run are restored from
        # server_nodes.xml and re-adding them raises BadNodeIdExists.
        await self.server.init(shelf_file=None)
        log.info("OPC UA namespace: %s", self.opcua_namespace)
        log.info("OPC UA root: %s  monitoring: %s",
                 self.opcua_root, self.monitoring_path)
        ns = await self.server.register_namespace(self.opcua_namespace)

        # Build the root object, creating one node per path component so that
        # multi-level roots like "Camera.CalibrationLightSource" produce a
        # proper browse hierarchy: Objects/Camera/CalibrationLightSource/...
        # The NodeId of each intermediate node uses the dotted prefix up to
        # that component, matching the convention used for children.
        components = self.opcua_root.split(".")
        node_id_prefix = ""
        parent = self.server.nodes.objects
        for component in components:
            node_id_prefix = f"{node_id_prefix}.{component}" if node_id_prefix else component
            parent = await parent.add_object(
                ua.NodeId(node_id_prefix, ns),
                ua.QualifiedName(component, ns),
            )
        device = parent

        # The leaf root node is the device node; methods hang directly off it.
        # The NodeId prefix for all children is the full root path.
        self._node_id_prefix     = self.opcua_root
        self._monitoring_node_id = f"{self.opcua_root}.{self.monitoring_path}"

        await self._build_monitoring(ns, device)
        await self._build_methods(ns, device)
        # Write static connection-identity variables once at startup
        await self._set_var("host",    self.connection.host)
        await self._set_var("port",    self.connection.port)
        await self._set_var("dialect", self.connection.dialect.NAME)

    # ----------------------------------------------------------------
    # Monitoring nodes
    # ----------------------------------------------------------------

    async def _build_monitoring(self, ns: int, device):
        mon_node_id = ua.NodeId(self._monitoring_node_id, ns)
        monitoring  = await device.add_object(
            mon_node_id, ua.QualifiedName(self.monitoring_path, ns))
        for name, default, vtype, description in self._MONITORING_VARS:
            node_id = ua.NodeId(f"{self._monitoring_node_id}.{name}", ns)
            var = await monitoring.add_variable(
                node_id, ua.QualifiedName(name, ns),
                ua.Variant(default, vtype),
            )
            await var.write_attribute(
                ua.AttributeIds.Description,
                ua.DataValue(ua.Variant(ua.LocalizedText(description),
                                        ua.VariantType.LocalizedText)),
            )
            await var.set_read_only()
            self._vars[name] = var

    async def _set_var(self, name: str, value):
        node = self._vars.get(name)
        if node is None:
            return
        try:
            await node.write_value(ua.Variant(value))
        except Exception as exc:
            log.error("Failed to update OPC UA variable %s: %s", name, exc)

    async def _apply_state(self, state: BoxState):
        """Push all BoxState fields into OPC UA monitoring nodes."""
        d = state.as_dict()
        for name in self._vars:
            if name != "cls_state" and name in d:
                await self._set_var(name, d[name])
        self.device_state = state.device_state
        await self._set_var("cls_state", int(self.device_state))

    # ----------------------------------------------------------------
    # OPC UA methods
    # ----------------------------------------------------------------

    # Argument descriptions sourced from ICD MST-CAM-ICD-0328-LUPM Ed.1 Rev.3
    # section 3.6 (Characters details).  Keyed by argument name so each
    # description is written once and shared between individual Set* methods
    # and the bulk Configure method.
    _ARG_DESCRIPTIONS: ClassVar[dict[str, str]] = {
        "led_mask":
            "LED on/off bitmask: bit N = LED N+1 (bits 0–12, 13 LEDs)",
        "voltage_set":
            "LED supply voltage in V (range 7.9–16.5 V, sets brightness of LEDs 1–13)",
        "voltage":
            "LED supply voltage in V (range 7.9–16.5 V, sets brightness of LEDs 1–13)",
        "duration":
            "Flash duration after Start in units of 0.1 s (range 0–1023; 0 = infinite, stop with Stop command)",
        "current_mA":
            "Centre LED No.7 drive current in mA (range 0–12 mA, LSB = 38.15 nA; SPE only, ignored for FF/AIVFF)",
        "central_current":
            "Centre LED No.7 drive current in mA (range 0–12 mA, LSB = 38.15 nA; SPE only, ignored for FF/AIVFF)",
        "width":
            "Trigger output pulse width in units of 62.5 ns (range 1–1000, i.e. 62.5 ns–62.5 µs; hardware power-on default is 4 = 250 ns)",
        "dividend":
            "Flash frequency dividend in Hz (range 244.16–10659.56 Hz with divider=1; sets 1/period of internal timer)",
        "frequency_dividend":
            "Flash frequency dividend in Hz (range 244.16–10659.56 Hz with divider=1; sets 1/period of internal timer)",
        "divider":
            "Flash frequency divider ratio (range 1–3000; use values > 1 for frequencies below ~300 Hz, minimum 0.1 Hz)",
        "frequency_divider":
            "Flash frequency divider ratio (range 1–3000; use values > 1 for frequencies below ~300 Hz, minimum 0.1 Hz)",
        "light_pulse":
            "Light pulse flashing On/Off (D0 of control byte; equivalent to Start/Stop)",
        "lemo_out":
            "LVDS trigger output On/Off (D1 of control byte)",
        "fiber1_out":
            "Optical transmitter No.1 output On/Off (D2 of control byte)",
        "fiber2_out":
            "Optical transmitter No.2 output On/Off (D3 of control byte)",
        "lemo_in":
            "External LEMO trigger input On/Off (D4 of control byte)",
    }

    @classmethod
    def _arg(cls, name: str, variant_type: ua.VariantType) -> ua.Argument:
        """Build a named, typed OPC UA method input argument descriptor.
        Description is looked up from _ARG_DESCRIPTIONS by name."""
        arg = ua.Argument()
        arg.Name             = name
        arg.DataType         = ua.NodeId(int(variant_type))
        arg.ValueRank        = -1
        arg.ArrayDimensions  = []
        arg.Description      = ua.LocalizedText(
            cls._ARG_DESCRIPTIONS.get(name, name))
        return arg

    async def _build_methods(self, ns: int, device):
        def node_id(name):
            return ua.NodeId(f"{self._node_id_prefix}.{name}", ns)

        a = self._arg  # shorthand

        async def add(name, cb, in_args=()):
            await device.add_method(
                node_id(name), ua.QualifiedName(name, ns),
                cb, list(in_args), [],
            )

        await add("Start",     self._m_start)
        await add("Stop",      self._m_stop)
        await add("Reboot",    self._m_reboot)
        await add("Reconnect", self._m_reconnect)
        await add("GetStatus", self._m_get_status)

        await add("SetLeds",     self._m_set_leds,
                  [a("led_mask",  ua.VariantType.Int32)])
        await add("SetVoltage",  self._m_set_voltage,
                  [a("voltage",   ua.VariantType.Float)])
        await add("SetDuration", self._m_set_duration,
                  [a("duration",  ua.VariantType.Int32)])
        await add("SetCurrent",  self._m_set_current,
                  [a("current_mA", ua.VariantType.Float)])
        await add("SetWidth",    self._m_set_width,
                  [a("width",     ua.VariantType.Int32)])

        await add("SetFrequency", self._m_set_frequency,
                  [a("dividend",  ua.VariantType.Float),
                   a("divider",   ua.VariantType.Int32)])

        await add("SetControl", self._m_set_control,
                  [a("lemo_out",    ua.VariantType.Boolean),
                   a("fiber1_out",  ua.VariantType.Boolean),
                   a("fiber2_out",  ua.VariantType.Boolean),
                   a("lemo_in",     ua.VariantType.Boolean),
                   a("light_pulse", ua.VariantType.Boolean)])

        await add("Configure", self._m_configure,
                  [a("led_mask",           ua.VariantType.Int32),
                   a("voltage_set",        ua.VariantType.Float),
                   a("duration",           ua.VariantType.Int32),
                   a("frequency_dividend", ua.VariantType.Float),
                   a("frequency_divider",  ua.VariantType.Int32),
                   a("width",              ua.VariantType.Int32),
                   a("light_pulse",        ua.VariantType.Boolean),
                   a("lemo_out",           ua.VariantType.Boolean),
                   a("fiber1_out",         ua.VariantType.Boolean),
                   a("fiber2_out",         ua.VariantType.Boolean),
                   a("lemo_in",            ua.VariantType.Boolean),
                   a("central_current",    ua.VariantType.Float)])

    # ----------------------------------------------------------------
    # Dispatch helper and post-command status refresh
    # ----------------------------------------------------------------

    async def _dispatch(self, coro) -> list:
        try:
            state = await coro
            if state is not None:
                await self._apply_state(state)
        except Exception as exc:
            log.warning("Command failed: %s", exc)
            raise ua.UaStatusCodeError(ua.StatusCodes.Bad)
        return []

    # ----------------------------------------------------------------
    # Method implementations
    # ----------------------------------------------------------------

    @_unwrap_variants
    async def _m_start(self, parent):
        return await self._dispatch(self.connection.start())

    @_unwrap_variants
    async def _m_stop(self, parent):
        return await self._dispatch(self.connection.stop())

    @_unwrap_variants
    async def _m_reboot(self, parent):
        try:
            await self.connection.reboot()
        except Exception as exc:
            log.warning("Reboot failed: %s", exc)
            raise ua.UaStatusCodeError(ua.StatusCodes.Bad)
        return []

    @_unwrap_variants
    async def _m_reconnect(self, parent):
        await self.connection.close()
        await asyncio.sleep(0.5)
        log.info("Attempting reconnection to %s device %s:%d ...",
                 self.connection.dialect.NAME, self.connection.host, self.connection.port)
        if await self.connection.connect() and await self.connection.authenticate():
            log.info("Reconnected to %s device %s:%d.",
                     self.connection.dialect.NAME, self.connection.host, self.connection.port)
            self.device_state = DeviceState.Disabled
            await self._set_var("cls_state", int(self.device_state))
            await self._apply_state(await self.connection.get_status())
            return []
        log.warning("Reconnect failed -- device still offline.")
        self.device_state = DeviceState.Offline
        await self._set_var("cls_state", int(self.device_state))
        raise ua.UaStatusCodeError(ua.StatusCodes.Bad)

    @_unwrap_variants
    async def _m_get_status(self, parent):
        return await self._dispatch(self.connection.get_status())

    @_unwrap_variants
    async def _m_set_leds(self, parent, mask: int):
        def _mod(s: BoxState): s.led_mask = mask
        return await self._dispatch(self.connection.modify(_mod))

    @_unwrap_variants
    async def _m_set_voltage(self, parent, volts: float):
        def _mod(s: BoxState): s.voltage_set = volts
        return await self._dispatch(self.connection.modify(_mod))

    @_unwrap_variants
    async def _m_set_duration(self, parent, duration: int):
        def _mod(s: BoxState): s.duration = duration
        return await self._dispatch(self.connection.modify(_mod))

    @_unwrap_variants
    async def _m_set_frequency(self, parent, dividend: float, divider: int):
        def _mod(s: BoxState):
            s.frequency_dividend = dividend
            s.frequency_divider  = divider
        return await self._dispatch(self.connection.modify(_mod))

    @_unwrap_variants
    async def _m_set_current(self, parent, mA: float):
        def _mod(s: BoxState): s.central_current = mA
        return await self._dispatch(self.connection.modify(_mod))

    @_unwrap_variants
    async def _m_set_width(self, parent, width: int):
        def _mod(s: BoxState): s.width = width
        return await self._dispatch(self.connection.modify(_mod))

    @_unwrap_variants
    async def _m_set_control(self, parent,
                              lemo_out: bool, fiber1_out: bool, fiber2_out: bool,
                              lemo_in: bool, light_pulse: bool):
        def _mod(s: BoxState):
            s.lemo_out = lemo_out;  s.fiber1_out = fiber1_out
            s.fiber2_out = fiber2_out;  s.lemo_in = lemo_in
            s.light_pulse = light_pulse
        return await self._dispatch(self.connection.modify(_mod))

    @_unwrap_variants
    async def _m_configure(self, parent,
                            led_mask: int, voltage_set: float, duration: int,
                            frequency_dividend: float, frequency_divider: int,
                            width: int, light_pulse: bool, lemo_out: bool,
                            fiber1_out: bool, fiber2_out: bool,
                            lemo_in: bool, central_current: float):
        state = BoxState(
            led_mask=led_mask, voltage_set=voltage_set, duration=duration,
            frequency_dividend=frequency_dividend, frequency_divider=frequency_divider,
            width=width, light_pulse=light_pulse, lemo_out=lemo_out,
            fiber1_out=fiber1_out, fiber2_out=fiber2_out,
            lemo_in=lemo_in, central_current=central_current,
        )
        return await self._dispatch(self.connection.configure(state))

    # ----------------------------------------------------------------
    # Poll task
    # ----------------------------------------------------------------

    async def _poll_loop(self):
        # Phase-locked loop: track the ideal next-tick time and always sleep
        # to the next future tick, skipping any ticks that have already passed
        # (e.g. after a blocking reconnect or a slow command).  This keeps the
        # poll period stable without ever firing multiple back-to-back calls
        # when the loop is unblocked after a long pause.
        next_tick = time.monotonic() + self.poll_interval
        while True:
            try:
                state = await self.connection.get_status()
                if self.device_state == DeviceState.Offline:
                    log.info("Device back online.")
                await self._apply_state(state)
            except Exception as exc:
                if self.device_state != DeviceState.Offline:
                    log.error("Poll failed, marking Offline: %s", exc)
                    self.device_state = DeviceState.Offline
                    await self._set_var("cls_state", int(self.device_state))
                    await self.connection.close()
                elif self.auto_reconnect:
                    log.info("Attempting reconnection to %s device %s:%d ...",
                              self.connection.dialect.NAME, self.connection.host, self.connection.port)
                    await self.connection.close()
                    if (await self.connection.connect()
                            and await self.connection.authenticate()):
                        log.info("Reconnected to %s device %s:%d.",
                                 self.connection.dialect.NAME, self.connection.host, self.connection.port)
                    else:
                        log.debug("Reconnect failed, will retry.")
                else:
                    log.debug("Device offline: %s", exc)

            # Advance next_tick by the smallest number of whole intervals that
            # puts it strictly in the future, then sleep until it arrives.
            now = time.monotonic()
            if next_tick <= now:
                skipped = int((now - next_tick) / self.poll_interval) + 1
                if skipped > 1:
                    log.debug("Poll loop blocked for %.2f s; skipping %d tick(s).",
                              skipped * self.poll_interval, skipped)
                next_tick += skipped * self.poll_interval
            await asyncio.sleep(next_tick - now)

    # ----------------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------------

    async def start(self):
        await self._init_server()

        log.info("Connecting to %s:%d ...", self.connection.host, self.connection.port)
        connected = (await self.connection.connect()
                     and await self.connection.authenticate())
        if connected:
            self.device_state = DeviceState.Disabled
            log.info("Connected to calibration box.")
        else:
            self.device_state = DeviceState.Offline
            log.warning(
                "Could not connect at startup -- starting in Offline mode.%s",
                " Auto-reconnect enabled." if self.auto_reconnect
                else " Use Reconnect method or restart with --auto-reconnect.",
            )
        await self._set_var("cls_state", int(self.device_state))

        async with self.server:
            log.info("OPC UA server started: %s", self.server.endpoint)
            poll_task = asyncio.create_task(self._poll_loop())

            # Resolve on SIGTERM (systemd stop) or SIGINT (Ctrl-C)
            loop     = asyncio.get_running_loop()
            shutdown = loop.create_future()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, shutdown.set_result, sig)

            log.info("Polling task started.  Waiting for SIGTERM or SIGINT.")
            try:
                sig = await shutdown
                log.info("Received %s, shutting down.", sig.name)
            finally:
                poll_task.cancel()
                try:
                    await poll_task
                except asyncio.CancelledError:
                    pass
                log.info("Shutdown complete.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args():
    p = argparse.ArgumentParser(
        description="NectarCAM Calibration Light Source OPC UA Server")
    p.add_argument("--address",          default="10.11.4.69")
    p.add_argument("--port",             type=int, default=50001)
    p.add_argument("--passwd",           default="", metavar="PASSWORD")
    p.add_argument("--product",          required=True, choices=["SPE", "FF", "AIVFF"],
                   help="Device variant: FF (Flat Field V6+), AIVFF (AIV FF V4.5), SPE (Single Photon)")
    p.add_argument("--opcua-endpoint",   default="opc.tcp://0.0.0.0:4840/nectarcam/")
    p.add_argument("--opcua-namespace",  default=None,
                   metavar="URI",
                   help="OPC UA namespace URI (default: "
                        "http://cta-observatory.org/nectarcam/calibrationlightsource/<product>). "
                        "Include telescope number for multi-telescope deployments, e.g. "
                        "http://cta-observatory.org/nectarcam/1/calibrationlightsource/FF")
    p.add_argument("--opcua-root",       default=None,
                   metavar="PATH",
                   help="Root object path in the OPC UA address space "
                        "(default: CalibrationLightSource). "
                        "Dot-separated components create nested browse levels, e.g. "
                        "Camera.CalibrationLightSource creates Objects/Camera/CalibrationLightSource/")
    p.add_argument("--monitoring-path",  default=None,
                   metavar="NAME",
                   help="Name of the monitoring object node under the root "
                        "(default: Monitoring)")
    p.add_argument("--opcua-user",       action="append", metavar="USER:PASS")
    p.add_argument("--log-level",        default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--log-file",         default=None, metavar="PATH",
                   help="Write log output to this file in addition to stdout "
                        "(stdout is always used so container log drivers capture it)")
    p.add_argument("--poll-interval",    type=float, default=1.0, metavar="SECONDS")
    p.add_argument("--auto-reconnect",   action="store_true")
    p.add_argument("--min-cmd-interval", type=float, default=0.0, metavar="SECONDS")
    return p.parse_args()


async def _main() -> int:
    args = _parse_args()
    _configure_logging(args.log_level, args.log_file)

    opcua_users = {}
    for pair in args.opcua_user or []:
        if ":" not in pair:
            log.error("Invalid --opcua-user (expected USER:PASS): %r", pair)
            return 1
        user, _, pw = pair.partition(":")
        opcua_users[user] = pw

    srv = CalibrationBoxServer(
        host             = args.address,
        port             = args.port,
        password         = args.passwd,
        product          = args.product,
        opcua_endpoint   = args.opcua_endpoint,
        opcua_namespace  = args.opcua_namespace,
        opcua_root       = args.opcua_root,
        monitoring_path  = args.monitoring_path,
        opcua_users      = opcua_users,
        poll_interval    = args.poll_interval,
        auto_reconnect   = args.auto_reconnect,
        min_cmd_interval = args.min_cmd_interval,
    )
    try:
        await srv.start()
    except Exception as exc:
        log.critical("Fatal error: %s", exc, exc_info=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
