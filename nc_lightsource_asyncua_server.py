"""
nc_lightsource_opcua_server.py
NectarCAM Calibration Box OPC UA Server

Implements the full binary TCP protocol from CalBoxClient.cpp /
CalBoxConfig.cpp in idiomatic Python, then exposes everything over
OPC UA using the asyncua async-native library.
"""

import argparse
import asyncio
import logging
import math
import socket
import struct
from enum import IntEnum
from typing import Optional
import functools
import time

from asyncua import Server, ua
from asyncua.server.users import User, UserRole
from asyncua.server.user_managers import UserManager


def unwrap_variants(fn):
    """
    Decorator for OPC UA method callbacks.

    asyncua passes arguments as ``ua.Variant`` objects.  This decorator
    unwraps every argument (after ``parent``) before forwarding to the real
    implementation, so none of the method bodies need to call ``.Value``
    themselves.
    """
    @functools.wraps(fn)
    async def wrapper(self, parent, *args):
        unwrapped = tuple(
            a.Value if isinstance(a, ua.Variant) else a for a in args
        )
        return await fn(self, parent, *unwrapped)
    return wrapper


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("cta.nectarcam.cls.server")


class _SuppressUaStatusCodeTracebacks(logging.Filter):
    """
    Attached to the ``asyncua.server.address_space`` logger.

    asyncua logs a full traceback whenever a method callback raises any
    exception, including the intentional ``ua.UaStatusCodeError`` that we
    raise to return a clean OPC-UA-level failure (Bad status) to the client.
    This filter downgrades those records to WARNING and strips the exc_info
    so no traceback appears.  All other records pass through unchanged.
    """
    def filter(self, record: logging.LogRecord) -> bool:
        if record.exc_info:
            exc = record.exc_info[1]
            if isinstance(exc, ua.UaStatusCodeError):
                log.warning("OPC UA method returned Bad status: %s", exc)
                return False
        return True


# ==========================================================
# FSM States
# ==========================================================
class DeviceState(IntEnum):
    Disabled = 0
    Enabled  = 1
    Fault    = 2
    Offline  = 3


# ==========================================================
# Product codes
# ==========================================================
class ProductCode(IntEnum):
    SPE = 0xAA   # SPE box  (33-byte status)
    FF  = 0xA5   # FF  box  (29 or 32-byte status depending on release)


# ==========================================================
# Wire-level command codes  (from CalBoxClient.h)
# ==========================================================
class Cmd(IntEnum):
    GET_STATUS = 0xE0
    START      = 0xFC
    STOP       = 0xF0
    RESET      = 0xF3


# Minimum valid status payload size (bytes, excluding CRLF)
MINIMAL_STATUS_SIZE = 24

# Status mask templates  (hex strings initialising the internal buffer)
_SPE_STATUS_MASK   = bytes.fromhex("202020404040406060606F737F6060606060808080A0A040404040AAC0C0C00D0A")
_AIVFF_STATUS_MASK = bytes.fromhex("202020404040406060606F737F6060606060808080A0A0A5C0C0C00D0A")
_FF_STATUS_MASK    = bytes.fromhex("242425515E495A6060606F737F606060606181939EA0AE819788A5CEC1C60D0A")

# Central current step for SPE box (mA per LSB)
_CENTRAL_CURRENT_STEP_MA = 12.0/31457


# ==========================================================
# CalBoxConfig  -  encode / decode the binary status frame
# ==========================================================
class CalBoxConfig:
    """
    Encodes and decodes the binary status / command frame.
    Mirrors CalBoxConfig.cpp entirely, translated to Python.

    The internal buffer ``s`` holds the raw status bytes and is
    the single source of truth for all field accessors.
    """

    @staticmethod
    def status_size(product: int, release: int) -> int:
        if product == ProductCode.FF:
            return 32 if release >= 6 else 29
        return 33   # SPE

    @staticmethod
    def command_size(product: int) -> int:
        return 20 if product == ProductCode.FF else 24

    def __init__(self, product: int, release: int):
        self.product  = product
        self.release  = release
        n = self.status_size(product, release)
        if product == ProductCode.FF:
            template = _FF_STATUS_MASK if release >= 6 else _AIVFF_STATUS_MASK
        else:
            template = _SPE_STATUS_MASK
        self.s = bytearray(template[:n].ljust(n, b'\x00'))

    @staticmethod
    def _bit(value: int, pos: int) -> int:
        return (value >> pos) & 1

    # ----------------------------------------------------------
    # LED mask  (13 LEDs packed into 3 status bytes)
    # ----------------------------------------------------------
    def set_leds(self, mask: int):
        b = self._bit
        if self.product == ProductCode.FF:
            self.s[0] = 0x20|(b(mask,6)<<4)|(b(mask,2)<<3)|(b(mask,4)<<2)|(b(mask,7)<<1)|b(mask,12)
            self.s[1] = 0x20|(b(mask,10)<<4)|(b(mask,5)<<3)|(b(mask,3)<<2)|(b(mask,0)<<1)|b(mask,1)
            self.s[2] = 0x20|(b(mask,9)<<2)|(b(mask,11)<<1)|b(mask,8)
        else:   # SPE
            self.s[0] = 0x20|(b(mask,9)<<4)|(b(mask,8)<<3)|(b(mask,12)<<2)|(b(mask,11)<<1)|b(mask,10)
            self.s[1] = 0x20|(b(mask,4)<<4)|(b(mask,3)<<3)|(b(mask,7)<<2)|(b(mask,6)<<1)|b(mask,5)
            self.s[2] = 0x20|(b(mask,2)<<2)|(b(mask,1)<<1)|b(mask,0)

    def leds(self) -> int:
        b = self._bit
        mask = 0
        if self.product == ProductCode.FF:
            mask |= b(self.s[0],0)<<12
            mask |= b(self.s[2],1)<<11
            mask |= b(self.s[1],4)<<10
            mask |= b(self.s[2],2)<<9
            mask |= b(self.s[2],0)<<8
            mask |= b(self.s[0],1)<<7
            mask |= b(self.s[0],4)<<6
            mask |= b(self.s[1],3)<<5
            mask |= b(self.s[0],2)<<4
            mask |= b(self.s[1],2)<<3
            mask |= b(self.s[0],3)<<2
            mask |= b(self.s[1],0)<<1
            mask |=  b(self.s[1],1)
        else:
            mask |= b(self.s[0],2)<<12
            mask |= b(self.s[0],1)<<11
            mask |= b(self.s[0],0)<<10
            mask |= b(self.s[0],4)<<9
            mask |= b(self.s[0],3)<<8
            mask |= b(self.s[1],2)<<7
            mask |= b(self.s[1],1)<<6
            mask |= b(self.s[1],0)<<5
            mask |= b(self.s[1],4)<<4
            mask |= b(self.s[1],3)<<3
            mask |= b(self.s[2],2)<<2
            mask |= b(self.s[2],1)<<1
            mask |=  b(self.s[2],0)
        return mask

    # ----------------------------------------------------------
    # Voltage (set)  -  10-bit code in s[3:5]
    # ----------------------------------------------------------
    @staticmethod
    def _voltage_to_code(volts: float) -> int:
        volts = min(16.5, max(7.9, volts))
        return int((volts - 7.9) * 950 / 8.44)

    def set_voltage(self, volts: float):
        code = self._voltage_to_code(volts)
        self.s[3] = 0x40 | ((code >> 5) & 0x1F)
        self.s[4] = 0x40 | (code & 0x1F)

    def voltage(self) -> float:
        code = ((self.s[3] & 0x1F) << 5) | (self.s[4] & 0x1F)
        return 7.9 + code * 8.44 / 950

    # ----------------------------------------------------------
    # Measured voltage  -  10-bit code in s[5:7]
    # ----------------------------------------------------------
    @staticmethod
    def _measured_code_to_voltage(code: int) -> float:
        return 0.2 + code * 12.5 * 3.3 / 1023

    def measured_voltage(self) -> float:
        code = ((self.s[5] & 0x1F) << 5) | (self.s[6] & 0x1F)
        return self._measured_code_to_voltage(code)

    # ----------------------------------------------------------
    # Duration  -  10-bit in s[7:9]
    # ----------------------------------------------------------
    def set_duration(self, duration: int):
        self.s[7] = 0x60 | ((duration >> 5) & 0x1F)
        self.s[8] = 0x60 | (duration & 0x1F)

    def duration(self) -> int:
        return ((self.s[7] & 0x1F) << 5) | (self.s[8] & 0x1F)

    # ----------------------------------------------------------
    # Frequency dividend  -  20-bit in s[9:13]
    # ----------------------------------------------------------
    def set_dividend(self, dividend: float):
        code = int(math.floor(1e10 / dividend / 625 - 1))
        self.s[9]  = 0x60 | (0x1F & (code >> 15))
        self.s[10] = 0x60 | (0x1F & (code >> 10))
        self.s[11] = 0x60 | (0x1F & (code >>  5))
        self.s[12] = 0x60 | (0x1F &  code)

    def dividend(self) -> float:
        code  = (self.s[9]  & 0x1F) << 15
        code |= (self.s[10] & 0x1F) << 10
        code |= (self.s[11] & 0x1F) << 5
        code |= (self.s[12] & 0x1F)
        return 1e10 / 625 / (code + 1)

    # ----------------------------------------------------------
    # Frequency divider  -  15-bit in s[13:16]
    # ----------------------------------------------------------
    def set_divider(self, divider: int):
        code = divider - 1
        self.s[13] = 0x60 | (0x1F & (code >> 10))
        self.s[14] = 0x60 | (0x1F & (code >>  5))
        self.s[15] = 0x60 | (0x1F &  code)

    def divider(self) -> int:
        code  = (self.s[13] & 0x1F) << 10
        code |= (self.s[14] & 0x1F) << 5
        code |= (self.s[15] & 0x1F)
        return 1 + code

    # ----------------------------------------------------------
    # Pulse width  -  10-bit in s[16:18]
    # ----------------------------------------------------------
    def set_width(self, width: int):
        self.s[16] = 0x60 | (0x1F & (width >> 5))
        self.s[17] = 0x60 | (0x1F &  width)

    def width(self) -> int:
        return ((self.s[16] & 0x1F) << 5) | (self.s[17] & 0x1F)

    # ----------------------------------------------------------
    # Temperature  -  13-bit signed in s[18:21]
    # ----------------------------------------------------------
    def temperature(self) -> float:
        sign = -1.0 if (self.s[18] & 0x04) else 1.0
        if self.software_version() < 6:
            abs_val = (32.0    * (self.s[18] & 0x03)
                     +  1.0    * (self.s[19] & 0x1F)
                     +  0.0625 * (self.s[20] & 0x1F))
        else:
            code = ((self.s[18] & 0x3) << 10) | ((self.s[19] & 0x1F) << 5) | (self.s[20] & 0x1F)
            abs_val = code * 175.72 / 4096 - 46.85
        return sign * abs_val

    # ----------------------------------------------------------
    # Humidity  -  12-bit in s[23:26], FF release>=6 only
    # ----------------------------------------------------------
    def humidity(self) -> float:
        if self.product != ProductCode.FF or self.software_version() < 6:
            return 50.0
        code = ((self.s[23] & 0x3) << 10) | ((self.s[24] & 0x1F) << 5) | (self.s[25] & 0x1F)
        return code * 125 / 4096.0 - 6.0

    # ----------------------------------------------------------
    # Fault / control masks  -  in s[21] / s[22]
    # ----------------------------------------------------------
    def error_mask(self) -> int:
        return self.s[21] & 0x1F

    def control_mask(self) -> int:
        return self.s[22] & 0x1F

    def is_enabled(self)  -> bool: return bool(self.control_mask() & 0x01)
    def lemo_out(self)    -> bool: return bool((self.control_mask() >> 1) & 0x01)
    def fiber1_out(self)  -> bool: return bool((self.control_mask() >> 2) & 0x01)
    def fiber2_out(self)  -> bool: return bool((self.control_mask() >> 3) & 0x01)
    def lemo_in(self)     -> bool: return bool((self.control_mask() >> 4) & 0x01)

    def set_control(self, lemo: bool, fiber1: bool, fiber2: bool,
                    external: bool, enabled: bool):
        self.s[22] = (0xA0
                      | int(enabled)
                      | (int(lemo)     << 1)
                      | (int(fiber1)   << 2)
                      | (int(fiber2)   << 3)
                      | (int(external) << 4))

    # ----------------------------------------------------------
    # Central current  -  20-bit in s[23:27], SPE only
    # ----------------------------------------------------------
    def set_central_current(self, mA: float):
        if self.product != ProductCode.SPE:
            return
        code = int(mA / _CENTRAL_CURRENT_STEP_MA)
        code = max(min(code, 31457), 0)
        for i in range(4):
            self.s[23 + i] = 0x40 | (0x1F & (code >> ((3 - i) * 5)))

    def central_current(self) -> float:
        if self.product != ProductCode.SPE:
            return 0.0
        code = 0
        for i in range(4):
            code |= (self.s[23 + i] & 0x1F) << ((3 - i) * 5)
        return code * _CENTRAL_CURRENT_STEP_MA

    # ----------------------------------------------------------
    # Product / firmware metadata (from tail of frame)
    # ----------------------------------------------------------
    def product_code(self) -> int:
        return self.s[-6]

    def product_id(self) -> int:
        return (self.s[-5] & 0x1F) | ((self.s[-4] & 0x1F) << 4)

    def software_version(self) -> int:
        return self.s[-3] & 0x1F

    @staticmethod
    def software_version_from_buf(buf: bytes) -> int:
        return buf[-3] & 0x1F

    # ----------------------------------------------------------
    # Serialise to a Configure command frame
    # ----------------------------------------------------------
    def to_cmd(self) -> bytes:
        n = self.command_size(self.product)
        c = bytearray(n)
        c[0:5]  = self.s[0:5]
        c[5:16] = self.s[7:18]
        c[16]   = self.s[22]
        if self.product == ProductCode.SPE:
            c[17:21] = self.s[23:27]
        c[-3] = self.s[-6]
        c[-2] = 0x0D
        c[-1] = 0x0A
        return bytes(c)

    # ----------------------------------------------------------
    # Deserialise from a raw status frame
    # ----------------------------------------------------------
    def from_status(self, raw: bytes):
        n = len(raw)
        if n < MINIMAL_STATUS_SIZE:
            raise ValueError(f"Status too short: {n} < {MINIMAL_STATUS_SIZE}")
        if n == len(self.s):
            self.s[:] = raw[:len(self.s)]
        else:
            self.s[0:3]   = raw[0:3]
            self.s[5:24]  = raw[3:22]
            self.s[26:28] = raw[22:24]
            self.set_voltage(self.measured_voltage())
        self.release = self.software_version()
        log.debug("CalBoxConfig decoded: leds=0x%x voltage=%.2f "
                  "temp=%.1f faults=0x%x",
                  self.leds(), self.voltage(),
                  self.temperature(), self.error_mask())

    def as_status_dict(self) -> dict:
        """Return all decoded fields as a flat dict for OPC UA."""
        return {
            "led_mask"           : self.leds(),
            "voltage_set"        : float(self.voltage()),
            "voltage_actual"     : float(self.measured_voltage()),
            "duration"           : self.duration(),
            "frequency_dividend" : float(self.dividend()),
            "frequency_divider"  : self.divider(),
            "width"              : self.width(),
            "temperature"        : float(self.temperature()),
            "humidity"           : float(self.humidity()),
            "faults"             : self.error_mask(),
            "light_pulse"        : self.is_enabled(),
            "lemo_out"           : self.lemo_out(),
            "fiber1_out"         : self.fiber1_out(),
            "fiber2_out"         : self.fiber2_out(),
            "lemo_in"            : self.lemo_in(),
            "central_current"    : float(self.central_current()),
        }

    def __repr__(self) -> str:
        return (f"CalBoxConfig(leds=0x{self.leds():04x} "
                f"voltage={self.voltage():.2f}V "
                f"temp={self.temperature():.1f}C "
                f"faults=0x{self.error_mask():02x} "
                f"enabled={self.is_enabled()} "
                f"release={self.release})")


# ==========================================================
# CalBoxHardware  -  TCP client
# ==========================================================
class CalBoxHardware:
    """
    Manages the TCP connection to the physical calibration box and
    translates high-level commands into binary wire frames.

    Protocol summary:
      1. TCP connect
      2. Authenticate: if a password is set, read 13-byte prompt,
         send password + CRLF, read 2-byte echo
      3. Commands: send a 4-byte simple command  OR  a 20/24-byte
         Configure frame; receive a CRLF-terminated status frame

    All blocking socket I/O is dispatched to a thread-pool executor so
    it never stalls the asyncio event loop.  The asyncio Lock serialises
    concurrent callers (OPC UA method callbacks + poll task).
    """

    CONNECT_TIMEOUT = 5.0   # seconds
    IO_TIMEOUT      = 2.0   # seconds

    def __init__(self, address: str, port: int, password: str,
                 product_code: int = 0, min_cmd_interval: float = 0.0):
        self.address          = address
        self.port             = port
        self.password         = (password + "\r\n") if password else ""
        self.product_code     = product_code
        self.release          = 6
        self.min_cmd_interval = min_cmd_interval

        self._sock: Optional[socket.socket] = None
        self._lock = asyncio.Lock()
        self._link_ok = False
        self._last_cfg: Optional[CalBoxConfig] = None
        self._last_cmd_time: float = 0.0

    # ----------------------------------------------------------
    # Blocking helpers – only called from the executor
    # ----------------------------------------------------------
    def _blocking_connect(self) -> bool:
        log.info("Connecting to %s:%d ...", self.address, self.port)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.CONNECT_TIMEOUT)
            sock.connect((self.address, self.port))
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(self.IO_TIMEOUT)
            self._sock = sock
            log.info("Connected.")
            return True
        except OSError as exc:
            log.error("Connection failed: %s", exc)
            self._sock = None
            return False

    def _blocking_authenticate(self) -> bool:
        if not self._sock:
            return False
        try:
            if self.password:
                prompt = self._recv_exactly(13)
                log.debug("Auth prompt: %r", prompt)
                self._send_all(self.password.encode())
                self._recv_exactly(2)
            log.info("Authenticated.")
            self._link_ok = True
            time.sleep(1)
            return True
        except OSError as exc:
            log.error("Authentication failed: %s", exc)
            self._link_ok = False
            return False

    def _blocking_close(self):
        self._link_ok = False
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
        log.info("Connection closed.")

    def _blocking_ensure_connected(self) -> bool:
        if self._sock is not None and self._link_ok:
            return True
        log.debug("Re-connecting ...")
        self._blocking_close()
        return self._blocking_connect() and self._blocking_authenticate()

    # ----------------------------------------------------------
    # Low-level I/O (blocking, runs in executor)
    # ----------------------------------------------------------
    def _send_all(self, data: bytes):
        total = 0
        while total < len(data):
            sent = self._sock.send(data[total:])
            if sent == 0:
                raise OSError("Socket closed during send")
            total += sent
        log.debug("Sent %d byte(s): %s", total, data.hex().upper())

    def _recv_exactly(self, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise OSError("Socket closed during recv")
            buf += chunk
        return buf

    def _recv_until_crlf(self) -> bytes:
        """Read bytes until CR LF; return payload excluding the delimiter."""
        buf = b""
        while True:
            ch = self._sock.recv(1)
            if not ch:
                raise OSError("Socket closed while reading response")
            buf += ch
            if buf.endswith(b"\r\n"):
                return buf[:-2]

    def _build_simple_cmd(self, code: int) -> bytes:
        word = code | (self.product_code << 8) | (0x0A0D << 16)
        return struct.pack("<I", word)

    def _rate_limit(self):
        """Sleep only as long as needed to honour min_cmd_interval,
        then record the send time.  Called from executor thread."""
        if self.min_cmd_interval > 0:
            elapsed = time.monotonic() - self._last_cmd_time
            wait = self.min_cmd_interval - elapsed
            if wait > 0:
                log.debug("Rate limiter: sleeping %.3f s", wait)
                time.sleep(wait)
        self._last_cmd_time = time.monotonic()

    def _exec_simple_cmd(self, code: int) -> Optional[CalBoxConfig]:
        self._rate_limit()
        frame = self._build_simple_cmd(code)
        log.debug("Sending cmd 0x%02X: %s", code, frame.hex().upper())
        self._send_all(frame)
        if code == Cmd.RESET:
            return None
        return self._wait_for_reply()

    def _wait_for_reply(self) -> CalBoxConfig:
        raw = self._recv_until_crlf()
        raw_with_crlf = raw + b"\r\n"
        if len(raw) < MINIMAL_STATUS_SIZE:
            raise OSError(
                f"Short reply: got {len(raw)} bytes, "
                f"expected >={MINIMAL_STATUS_SIZE}"
            )
        log.debug("Received %d byte(s): %s",
                  len(raw_with_crlf), raw_with_crlf.hex().upper())
        self.release = CalBoxConfig.software_version_from_buf(raw_with_crlf)
        cfg = CalBoxConfig(self.product_code, self.release)
        cfg.from_status(raw_with_crlf)
        self._last_cfg = cfg
        log.info("Status: %s", cfg)
        return cfg

    def _get_and_modify(self, modify_fn) -> CalBoxConfig:
        cfg = self._exec_simple_cmd(Cmd.GET_STATUS)
        modify_fn(cfg)
        return self._send_configure(cfg)

    def _send_configure(self, cfg: CalBoxConfig) -> CalBoxConfig:
        self._rate_limit()
        frame = cfg.to_cmd()
        log.debug("Sending Configure (%d B): %s", len(frame), frame.hex().upper())
        self._send_all(frame)
        return self._wait_for_reply()

    # ----------------------------------------------------------
    # Blocking command bodies – called via run_in_executor
    # ----------------------------------------------------------
    def _run_get_status(self) -> CalBoxConfig:
        if not self._blocking_ensure_connected():
            raise OSError("Not connected")
        return self._exec_simple_cmd(Cmd.GET_STATUS)

    def _run_start(self) -> CalBoxConfig:
        if not self._blocking_ensure_connected():
            raise OSError("Not connected")
        log.info("Starting light source")
        return self._exec_simple_cmd(Cmd.START)

    def _run_stop(self) -> CalBoxConfig:
        if not self._blocking_ensure_connected():
            raise OSError("Not connected")
        log.info("Stopping light source")
        return self._exec_simple_cmd(Cmd.STOP)

    def _run_reboot(self):
        if not self._blocking_ensure_connected():
            raise OSError("Not connected")
        log.info("Rebooting device")
        self._exec_simple_cmd(Cmd.RESET)

    def _run_modify(self, modify_fn) -> CalBoxConfig:
        if not self._blocking_ensure_connected():
            raise OSError("Not connected")
        return self._get_and_modify(modify_fn)

    def _run_configure(self, led_mask, voltage_set, duration,
                       frequency_dividend, frequency_divider, width,
                       enabled, lemo_out, fiber1_out, fiber2_out, external,
                       central_current) -> CalBoxConfig:
        if not self._blocking_ensure_connected():
            raise OSError("Not connected")
        cfg = CalBoxConfig(self.product_code, self.release)
        if self._last_cfg:
            cfg.s[:] = self._last_cfg.s[:]
        cfg.set_leds(led_mask)
        cfg.set_voltage(voltage_set)
        cfg.set_duration(duration)
        cfg.set_dividend(frequency_dividend)
        cfg.set_divider(frequency_divider)
        cfg.set_width(width)
        cfg.set_control(lemo_out, fiber1_out, fiber2_out, external, enabled)
        cfg.set_central_current(central_current)
        return self._send_configure(cfg)

    # ----------------------------------------------------------
    # Async public API
    # ----------------------------------------------------------
    async def connect(self) -> bool:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._blocking_connect)

    async def authenticate(self) -> bool:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._blocking_authenticate)

    async def close(self):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._blocking_close)

    async def get_status(self) -> CalBoxConfig:
        async with self._lock:
            return await asyncio.get_running_loop().run_in_executor(
                None, self._run_get_status)

    async def start(self) -> CalBoxConfig:
        async with self._lock:
            return await asyncio.get_running_loop().run_in_executor(
                None, self._run_start)

    async def stop(self) -> CalBoxConfig:
        async with self._lock:
            return await asyncio.get_running_loop().run_in_executor(
                None, self._run_stop)

    async def reboot(self):
        async with self._lock:
            await asyncio.get_running_loop().run_in_executor(
                None, self._run_reboot)

    async def set_leds(self, mask: int) -> CalBoxConfig:
        async with self._lock:
            return await asyncio.get_running_loop().run_in_executor(
                None, self._run_modify, lambda c: c.set_leds(mask))

    async def set_voltage(self, volts: float) -> CalBoxConfig:
        async with self._lock:
            return await asyncio.get_running_loop().run_in_executor(
                None, self._run_modify, lambda c: c.set_voltage(volts))

    async def set_duration(self, duration: int) -> CalBoxConfig:
        async with self._lock:
            return await asyncio.get_running_loop().run_in_executor(
                None, self._run_modify, lambda c: c.set_duration(duration))

    async def set_frequency(self, dividend: float, divider: int) -> CalBoxConfig:
        def _mod(c):
            c.set_dividend(dividend)
            c.set_divider(divider)
        async with self._lock:
            return await asyncio.get_running_loop().run_in_executor(
                None, self._run_modify, _mod)

    async def set_central_current(self, mA: float) -> CalBoxConfig:
        async with self._lock:
            return await asyncio.get_running_loop().run_in_executor(
                None, self._run_modify, lambda c: c.set_central_current(mA))

    async def set_width(self, width: int) -> CalBoxConfig:
        async with self._lock:
            return await asyncio.get_running_loop().run_in_executor(
                None, self._run_modify, lambda c: c.set_width(width))

    async def set_trigger_control(self, lemo: bool, fiber1: bool, fiber2: bool,
                                  external: bool, enabled: bool) -> CalBoxConfig:
        async with self._lock:
            return await asyncio.get_running_loop().run_in_executor(
                None, self._run_modify,
                lambda c: c.set_control(lemo, fiber1, fiber2, external, enabled))

    async def configure(self, led_mask: int, voltage_set: float, duration: int,
                        frequency_dividend: float, frequency_divider: int,
                        width: int, enabled: bool, lemo_out: bool,
                        fiber1_out: bool, fiber2_out: bool, external: bool,
                        central_current: float) -> CalBoxConfig:
        async with self._lock:
            return await asyncio.get_running_loop().run_in_executor(
                None, self._run_configure,
                led_mask, voltage_set, duration,
                frequency_dividend, frequency_divider, width,
                enabled, lemo_out, fiber1_out, fiber2_out, external,
                central_current)


# ==========================================================
# OPC UA Server
# ==========================================================
class CalibrationBoxServer:
    """
    Async OPC UA server that wraps CalBoxHardware and exposes the full
    NectarCAM calibration box interface.

    Node hierarchy (namespace index = self.idx):
        Objects/
          <device_root_name>/
            CalibrationBox/
              Monitoring/
                <n>/
                  <n>_v    (read-only, polled at poll_interval Hz)
              Methods: Start, Stop, Reboot, Reconnect, GetStatus,
                       SetLeds, SetVoltage, SetDuration, SetFrequency,
                       SetCurrent, SetWidth, SetControl, Configure
    """

    def __init__(
        self,
        address:          str   = "10.11.4.69",
        port:             int   = 50001,
        password:         str   = "",
        device_root_name: str   = "NectarCAM",
        product_code:     int   = 0,
        opcua_endpoint:   str   = "opc.tcp://0.0.0.0:4840/nectarcam/",
        opcua_users:      dict  = None,
        poll_interval:    float = 1.0,
        auto_reconnect:   bool  = False,
        min_cmd_interval: float = 0.0,
    ):
        self.device_root_name = device_root_name
        self.opcua_users      = opcua_users or {}
        self.poll_interval    = poll_interval
        self.auto_reconnect   = auto_reconnect

        self.hardware = CalBoxHardware(address, port, password, product_code,
                                       min_cmd_interval=min_cmd_interval)
        self.state    = DeviceState.Offline
        self.vars: dict = {}

        # Build user manager if credentials were supplied
        if self.opcua_users:
            opcua_users_ref = self.opcua_users
            class _UserManager(UserManager):
                async def get_user(self, iserver, username=None, password=None,
                                   certificate=None):
                    if (username in opcua_users_ref
                            and password == opcua_users_ref[username]):
                        return User(role=UserRole.User)
                    return None
            user_mgr = _UserManager()
        else:
            user_mgr = None

        self.server = Server(user_manager=user_mgr)
        self.server.set_endpoint(opcua_endpoint)
        self.server.set_server_name("NectarCAM CalibrationBox")
        if self.opcua_users:
            self.server.set_security_IDs(["Anonymous", "Username"])

        # Suppress traceback noise for expected UaStatusCodeError failures
        logging.getLogger("asyncua.server.address_space").addFilter(
            _SuppressUaStatusCodeTracebacks()
        )

    # ----------------------------------------------------------
    # Async initialisation
    # ----------------------------------------------------------
    async def _init_server(self):
        """Initialise OPC UA namespace and build the node tree."""
        await self.server.init()

        self.idx    = await self.server.register_namespace(
            "http://nectarcam.calibrationbox"
        )
        objects     = self.server.nodes.objects
        self.root   = await objects.add_object(self.idx, self.device_root_name)
        self.device = await self.root.add_object(self.idx, "CalibrationBox")

        await self._create_monitoring()
        await self._create_methods()

    # ----------------------------------------------------------
    # Monitoring nodes
    # ----------------------------------------------------------
    async def _add_var(self, name: str, default, vtype):
        container = await self.monitoring.add_object(self.idx, name)
        var = await container.add_variable(
            self.idx, name + "_v", ua.Variant(default, vtype)
        )
        self.vars[name] = var

    async def _create_monitoring(self):
        self.monitoring = await self.device.add_object(self.idx, "Monitoring")
        await self._add_var("led_mask",           8191,    ua.VariantType.Int32)
        await self._add_var("voltage_set",        10.0,    ua.VariantType.Float)
        await self._add_var("voltage_actual",     10.0,    ua.VariantType.Float)
        await self._add_var("duration",           0,       ua.VariantType.Int32)
        await self._add_var("frequency_dividend", 10000.0, ua.VariantType.Float)
        await self._add_var("frequency_divider",  1,       ua.VariantType.Int32)
        await self._add_var("width",              1,       ua.VariantType.Int32)
        await self._add_var("temperature",        20.0,    ua.VariantType.Float)
        await self._add_var("humidity",           50.0,    ua.VariantType.Float)
        await self._add_var("faults",             0,       ua.VariantType.Int16)
        await self._add_var("light_pulse",        False,   ua.VariantType.Boolean)
        await self._add_var("lemo_out",           True,    ua.VariantType.Boolean)
        await self._add_var("fiber1_out",         True,    ua.VariantType.Boolean)
        await self._add_var("fiber2_out",         True,    ua.VariantType.Boolean)
        await self._add_var("lemo_in",            False,   ua.VariantType.Boolean)
        await self._add_var("central_current",    0.0,     ua.VariantType.Float)
        await self._add_var("cls_state",          0,       ua.VariantType.Int32)

    # ----------------------------------------------------------
    # OPC UA Methods
    # ----------------------------------------------------------
    async def _create_methods(self):
        async def m(name, cb, in_types):
            await self.device.add_method(
                ua.NodeId(name, self.idx),
                ua.QualifiedName(name, self.idx),
                cb, in_types, []
            )

        await m("Start",     self._m_start,     [])
        await m("Stop",      self._m_stop,      [])
        await m("Reboot",    self._m_reboot,    [])
        await m("Reconnect", self._m_reconnect, [])
        await m("GetStatus", self._m_get_status,[])

        await m("SetLeds",     self._m_set_leds,     [ua.VariantType.Int32])
        await m("SetVoltage",  self._m_set_voltage,  [ua.VariantType.Float])
        await m("SetDuration", self._m_set_duration, [ua.VariantType.Int32])
        await m("SetCurrent",  self._m_set_current,  [ua.VariantType.Float])
        await m("SetWidth",    self._m_set_width,    [ua.VariantType.Int32])

        await m("SetFrequency", self._m_set_frequency,
                [ua.VariantType.Float, ua.VariantType.Int32])

        await m("SetControl", self._m_set_control,
                [ua.VariantType.Boolean,   # lemo_out (AKA trigger out)
                 ua.VariantType.Boolean,   # fiber1
                 ua.VariantType.Boolean,   # fiber2
                 ua.VariantType.Boolean,   # external (AKA lemo_in)
                 ua.VariantType.Boolean])  # enabled

        await m("Configure", self._m_configure,
                [ua.VariantType.Int32,     # led_mask
                 ua.VariantType.Float,     # voltage_set
                 ua.VariantType.Int32,     # duration
                 ua.VariantType.Float,     # frequency_dividend
                 ua.VariantType.Int32,     # frequency_divider
                 ua.VariantType.Int32,     # width
                 ua.VariantType.Boolean,   # enabled
                 ua.VariantType.Boolean,   # lemo_out
                 ua.VariantType.Boolean,   # fiber1_out
                 ua.VariantType.Boolean,   # fiber2_out
                 ua.VariantType.Boolean,   # external (AKA lemo_in)
                 ua.VariantType.Float])    # central_current

    # ----------------------------------------------------------
    # Post-command status refresh
    # ----------------------------------------------------------
    def _schedule_status_refresh(self, delay: float = 0.050):
        """Schedule a get_status after *delay* seconds as a background task."""
        async def _refresh():
            await asyncio.sleep(delay)
            try:
                cfg = await self.hardware.get_status()
                await self._apply_config(cfg)
            except Exception as exc:
                log.debug("Post-command status refresh failed: %s", exc)
        asyncio.create_task(_refresh())

    # ----------------------------------------------------------
    # Method implementations
    # ----------------------------------------------------------
    async def _dispatch(self, hw_coro, refresh: bool = True) -> list:
        """Await hw_coro, push the resulting CalBoxConfig to OPC UA nodes."""
        try:
            cfg = await hw_coro
            if cfg is not None:
                await self._apply_config(cfg)
        except Exception as exc:
            log.warning("Command failed: %s", exc)
            raise ua.UaStatusCodeError(ua.StatusCodes.Bad)
        if refresh:
            self._schedule_status_refresh()
        return []

    @unwrap_variants
    async def _m_start(self, parent):
        return await self._dispatch(self.hardware.start())

    @unwrap_variants
    async def _m_stop(self, parent):
        return await self._dispatch(self.hardware.stop())

    @unwrap_variants
    async def _m_reboot(self, parent):
        try:
            await self.hardware.reboot()
        except Exception as exc:
            log.warning("Reboot failed: %s", exc)
            raise ua.UaStatusCodeError(ua.StatusCodes.Bad)
        return []

    @unwrap_variants
    async def _m_reconnect(self, parent):
        await self.hardware.close()
        await asyncio.sleep(2)
        if await self.hardware.connect() and await self.hardware.authenticate():
            log.info("Reconnect succeeded.")
            self.state = DeviceState.Disabled
            await self._set_var("cls_state", int(self.state))
            self._schedule_status_refresh()
            return []
        else:
            log.warning("Reconnect failed - device still offline.")
            self.state = DeviceState.Offline
            await self._set_var("cls_state", int(self.state))
            raise ua.UaStatusCodeError(ua.StatusCodes.Bad)

    @unwrap_variants
    async def _m_get_status(self, parent):
        return await self._dispatch(self.hardware.get_status(), refresh=False)

    @unwrap_variants
    async def _m_set_leds(self, parent, mask: int):
        return await self._dispatch(self.hardware.set_leds(mask))

    @unwrap_variants
    async def _m_set_voltage(self, parent, volts: float):
        return await self._dispatch(self.hardware.set_voltage(volts))

    @unwrap_variants
    async def _m_set_duration(self, parent, duration: int):
        return await self._dispatch(self.hardware.set_duration(duration))

    @unwrap_variants
    async def _m_set_frequency(self, parent, dividend: float, divider: int):
        return await self._dispatch(self.hardware.set_frequency(dividend, divider))

    @unwrap_variants
    async def _m_set_current(self, parent, mA: float):
        return await self._dispatch(self.hardware.set_central_current(mA))

    @unwrap_variants
    async def _m_set_width(self, parent, width: int):
        return await self._dispatch(self.hardware.set_width(width))

    @unwrap_variants
    async def _m_set_control(self, parent, lemo_out, fiber1_out, fiber2_out,
                             external, enabled):
        return await self._dispatch(
            self.hardware.set_trigger_control(
                lemo_out, fiber1_out, fiber2_out, external, enabled))

    @unwrap_variants
    async def _m_configure(self, parent, led_mask, voltage_set, duration,
                           frequency_dividend, frequency_divider, width,
                           enabled, lemo_out, fiber1_out, fiber2_out, external,
                           central_current):
        return await self._dispatch(
            self.hardware.configure(
                led_mask, voltage_set, duration,
                frequency_dividend, frequency_divider, width,
                enabled, lemo_out, fiber1_out, fiber2_out, external,
                central_current))

    # ----------------------------------------------------------
    # OPC UA node write helper
    # ----------------------------------------------------------
    async def _set_var(self, name: str, value):
        node = self.vars.get(name)
        if node is None:
            return
        try:
            await node.write_value(ua.Variant(value))
        except Exception as exc:
            log.error("Failed to set OPC UA var %s: %s", name, exc)

    async def _apply_config(self, cfg: CalBoxConfig):
        """Push a decoded CalBoxConfig into all OPC UA monitoring nodes."""
        for key, value in cfg.as_status_dict().items():
            await self._set_var(key, value)
        if cfg.error_mask():
            self.state = DeviceState.Fault
        elif cfg.is_enabled():
            self.state = DeviceState.Enabled
        else:
            self.state = DeviceState.Disabled
        await self._set_var("cls_state", int(self.state))

    # ----------------------------------------------------------
    # Background poll task
    # ----------------------------------------------------------
    async def _poll_hardware(self):
        while True:
            try:
                cfg = await self.hardware.get_status()
                if self.state == DeviceState.Offline:
                    log.info("Hardware back online.")
                await self._apply_config(cfg)
            except Exception as exc:
                if self.state != DeviceState.Offline:
                    log.error("Poll failed, marking Offline: %s", exc)
                    self.state = DeviceState.Offline
                    await self._set_var("cls_state", int(self.state))
                elif self.auto_reconnect:
                    log.info("Auto-reconnect: attempting to reconnect ...")
                    await self.hardware.close()
                    if (await self.hardware.connect()
                            and await self.hardware.authenticate()):
                        log.info("Auto-reconnect succeeded.")
                        self._schedule_status_refresh()
                    else:
                        log.warning("Auto-reconnect failed, will retry.")
            await asyncio.sleep(self.poll_interval)

    # ----------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------
    async def start(self):
        await self._init_server()

        connected = (await self.hardware.connect()
                     and await self.hardware.authenticate())
        if connected:
            self.state = DeviceState.Disabled
            log.info("Connected to calibration box.")
        else:
            self.state = DeviceState.Offline
            log.warning(
                "Could not connect to calibration box at startup - "
                "starting in Offline mode.%s",
                " Auto-reconnect is enabled." if self.auto_reconnect else
                " Use the Reconnect method or restart with --auto-reconnect."
            )

        await self._set_var("cls_state", int(self.state))

        async with self.server:
            log.info("OPC UA server started: %s", self.server.endpoint)

            if connected:
                self._schedule_status_refresh()

            poll_task = asyncio.create_task(self._poll_hardware())
            log.info("Polling task started. Press Ctrl-C to stop.")

            try:
                await asyncio.Event().wait()   # sleep until cancelled
            except asyncio.CancelledError:
                pass
            finally:
                poll_task.cancel()
                try:
                    await poll_task
                except asyncio.CancelledError:
                    pass
                log.info("Shutting down ...")


# ==========================================================
# Entry point
# ==========================================================
def parse_args():
    p = argparse.ArgumentParser(
        description="NectarCAM Calibration Box OPC UA Server"
    )
    p.add_argument("--address",           default="10.11.4.69",
                   help="CalBox TCP address")
    p.add_argument("--port",              type=int, default=50001,
                   help="CalBox TCP port")
    p.add_argument("--passwd",            default="",
                   help="CalBox password (omit if none)")
    p.add_argument("--device-root-name",  default="NectarCAM",
                   help="Root OPC UA object name")
    p.add_argument("--product-code",
                   type=lambda x: int(x, 0), default=0,
                   help="Product code: 0xAA=SPE, 0xA5=FF")
    p.add_argument("--opcua-endpoint",
                   default="opc.tcp://0.0.0.0:4840/nectarcam/",
                   help="OPC UA server endpoint URL")
    p.add_argument("--opcua-user", action="append", metavar="USER:PASS",
                   help="OPC UA username:password pair (repeatable)")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--poll-interval", type=float, default=1.0, metavar="SECONDS",
                   help="Hardware polling interval in seconds (default: 1.0)")
    p.add_argument("--auto-reconnect", action="store_true",
                   help="Automatically attempt to reconnect when the device goes offline")
    p.add_argument("--min-cmd-interval", type=float, default=0.0, metavar="SECONDS",
                   help="Minimum time between commands sent to the device (default: 0, no limiting)")
    return p.parse_args()


async def main():
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    opcua_users = {}
    for pair in (args.opcua_user or []):
        if ":" not in pair:
            raise SystemExit(
                f"Invalid --opcua-user (expected USER:PASS): {pair}"
            )
        u, _, pw = pair.partition(":")
        opcua_users[u] = pw

    srv = CalibrationBoxServer(
        address          = args.address,
        port             = args.port,
        password         = args.passwd,
        device_root_name = args.device_root_name,
        product_code     = args.product_code,
        opcua_endpoint   = args.opcua_endpoint,
        opcua_users      = opcua_users,
        poll_interval    = args.poll_interval,
        auto_reconnect   = args.auto_reconnect,
        min_cmd_interval = args.min_cmd_interval,
    )

    try:
        await srv.start()
    except KeyboardInterrupt:
        log.info("Interrupted.")


if __name__ == "__main__":
    asyncio.run(main())
