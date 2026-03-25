"""
nc_lightsource_emulator.py
NectarCAM Calibration Box  –  TCP Emulator

Listens on a TCP port and speaks the same binary protocol as the
real calibration box, so that nc_lightsource_opcua_server.py can
connect to it without any hardware present.

Two threads run in parallel:
  • TCP thread  – accepts one connection at a time, decodes frames
                  from the server and sends back status replies
  • CLI thread  – reads commands from stdin so you can inspect and
                  override individual state variables at any time

Usage
-----
    python nc_lightsource_emulator.py [--port 50001] [--product-code 0xAA]
                                      [--password secret] [--log-level DEBUG]
"""

import argparse
import logging
import math
import socket
import struct
import threading
import time
from enum import IntEnum
from typing import Optional

# ==========================================================
# Logging
# ==========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("cta.nectarcam.cls.emulator")


# ==========================================================
# Product codes & command codes  (mirrors server code)
# ==========================================================
class ProductCode(IntEnum):
    SPE = 0xAA
    FF  = 0xA5


class Cmd(IntEnum):
    GET_STATUS = 0xE0
    START      = 0xFC
    STOP       = 0xF0
    RESET      = 0xF3


MINIMAL_STATUS_SIZE  = 24
_CENTRAL_CURRENT_STEP_MA = 12.0/31457


class _RebootRequested(Exception):
    """Raised by the RESET handler to force-close the client connection."""


# ==========================================================
# Device state
# ==========================================================
class DeviceState:
    """
    Single shared object that holds the full emulated device state.
    All mutations go through a lock so the CLI and TCP threads can
    both read / write safely.
    """

    def __init__(self, product: int, release: int = 6, _skip_threads: bool = False):
        self._lock   = threading.Lock()
        self.product = product
        self.release = release

        # Configurable parameters
        self.led_mask            = 8191
        self.voltage_set         = 9.98
        self.duration            = 0
        self.frequency_dividend  = 10000.0
        self.frequency_divider   = 1
        self.width               = 1
        self.lemo_out            = True
        self.fiber1_out          = True
        self.fiber2_out          = True
        self.external            = False
        self.enabled             = False   # light pulse on/off
        self.central_current     = 0.0

        # Sensor readings (overridable from CLI)
        self.voltage_actual      = 9.98
        self.temperature         = 22.3
        self.humidity            = 45.0
        self.lemo_in             = False
        self.error_mask          = 0       # fault bits

        # Informational
        self.product_id          = 1
        self.connected           = False

        # Internal timers / background tasks (not part of protocol state)
        self._duration_timer: Optional[threading.Timer] = None
        self._voltage_ramp_thread: Optional[threading.Thread] = None
        self._voltage_ramp_stop  = threading.Event()
        
        # Skip starting background threads for temporary instances used only for encoding
        if not _skip_threads:
            self._start_voltage_ramp()

    # ----------------------------------------------------------
    def reset(self):
        """Restore all configurable parameters to their power-on defaults."""
        self._cancel_duration_timer()
        with self._lock:
            self.led_mask            = 8191
            self.voltage_set         = 9.98
            self.duration            = 0
            self.frequency_dividend  = 10000.0
            self.frequency_divider   = 1
            self.width               = 1
            self.lemo_out            = True
            self.fiber1_out          = True
            self.fiber2_out          = True
            self.external            = False
            self.enabled             = False
            self.central_current     = 0.0
            self.connected           = False
        log.info("Device state reset to defaults.")

    # ----------------------------------------------------------
    def update(self, **kwargs):
        with self._lock:
            for k, v in kwargs.items():
                if hasattr(self, k):
                    setattr(self, k, v)
                    log.info("State updated: %s = %s", k, v)
                else:
                    log.warning("Unknown state field: %s", k)

    def snapshot(self) -> dict:
        with self._lock:
            return {k: v for k, v in self.__dict__.items()
                    if not k.startswith("_")}

    # ----------------------------------------------------------
    # Duration timer
    # ----------------------------------------------------------
    def arm_duration_timer(self):
        """
        If duration > 0, schedule an automatic Stop after
        duration * 0.1 seconds (per ICD: range × 0.1 s).
        Cancels any previously armed timer first.
        """
        self._cancel_duration_timer()
        with self._lock:
            d = self.duration
        if d <= 0:
            log.debug("Duration is 0 — light source runs indefinitely.")
            return
        delay = d * 0.1
        log.info("Duration timer armed: %.1f s (duration=%d)", delay, d)
        self._duration_timer = threading.Timer(delay, self._duration_expired)
        self._duration_timer.daemon = True
        self._duration_timer.start()

    def _cancel_duration_timer(self):
        if self._duration_timer is not None:
            self._duration_timer.cancel()
            self._duration_timer = None
            log.debug("Duration timer cancelled.")

    def _duration_expired(self):
        log.info("Duration expired — switching light source OFF.")
        with self._lock:
            self.enabled = False
        self._duration_timer = None

    # ----------------------------------------------------------
    # Voltage ramp  (1 V/s background thread)
    # ----------------------------------------------------------
    _RAMP_RATE     = 1.0   # V per second
    _RAMP_INTERVAL = 0.05  # update interval in seconds

    def _start_voltage_ramp(self):
        self._voltage_ramp_stop.clear()
        self._voltage_ramp_thread = threading.Thread(
            target=self._voltage_ramp_loop,
            name="voltage-ramp",
            daemon=True,
        )
        self._voltage_ramp_thread.start()

    def _voltage_ramp_loop(self):
        step = self._RAMP_RATE * self._RAMP_INTERVAL   # V per tick
        while not self._voltage_ramp_stop.is_set():
            with self._lock:
                target  = self.voltage_set
                current = self.voltage_actual
                diff    = target - current
                if abs(diff) <= step:
                    if current != target:
                        self.voltage_actual = target
                        log.debug("Voltage ramp complete: %.3f V", target)
                else:
                    self.voltage_actual = current + math.copysign(step, diff)
                    log.debug("Voltage ramping: actual=%.3f → set=%.3f",
                              self.voltage_actual, target)
            time.sleep(self._RAMP_INTERVAL)

    def stop_background_tasks(self):
        """Call on shutdown to clean up the ramp thread and any pending timers."""
        self._cancel_duration_timer()
        self._voltage_ramp_stop.set()
        if self._voltage_ramp_thread is not None:
            self._voltage_ramp_thread.join(timeout=2.0)


# ==========================================================
# Binary frame codec  (mirrors CalBoxConfig)
# ==========================================================
class FrameCodec:
    """
    Encodes the device state into a binary status reply frame and
    decodes incoming Configure command frames back into state fields.

    Byte layout documented in CalBoxConfig.cpp.
    """

    @staticmethod
    def _bit(value: int, pos: int) -> int:
        return (value >> pos) & 1

    # ----------------------------------------------------------
    # Status frame size
    # ----------------------------------------------------------
    @staticmethod
    def status_size(product: int, release: int) -> int:
        if product == ProductCode.FF:
            return 32 if release >= 6 else 29
        return 33

    @staticmethod
    def command_size(product: int) -> int:
        return 20 if product == ProductCode.FF else 24

    # ----------------------------------------------------------
    # LED mask packing
    # ----------------------------------------------------------
    @staticmethod
    def encode_leds(mask: int, product: int) -> tuple:
        """Return (s0, s1, s2) bytes for the given LED mask."""
        b = FrameCodec._bit
        if product == ProductCode.FF:
            s0 = 0x20|(b(mask,6)<<4)|(b(mask,2)<<3)|(b(mask,4)<<2)|(b(mask,7)<<1)|b(mask,12)
            s1 = 0x20|(b(mask,10)<<4)|(b(mask,5)<<3)|(b(mask,3)<<2)|(b(mask,0)<<1)|b(mask,1)
            s2 = 0x20|(b(mask,9)<<2)|(b(mask,11)<<1)|b(mask,8)
        else:
            s0 = 0x20|(b(mask,9)<<4)|(b(mask,8)<<3)|(b(mask,12)<<2)|(b(mask,11)<<1)|b(mask,10)
            s1 = 0x20|(b(mask,4)<<4)|(b(mask,3)<<3)|(b(mask,7)<<2)|(b(mask,6)<<1)|b(mask,5)
            s2 = 0x20|(b(mask,2)<<2)|(b(mask,1)<<1)|b(mask,0)
        return s0, s1, s2

    @staticmethod
    def decode_leds(s0: int, s1: int, s2: int, product: int) -> int:
        b = FrameCodec._bit
        mask = 0
        if product == ProductCode.FF:
            mask |= b(s0,0)<<12; mask |= b(s2,1)<<11; mask |= b(s1,4)<<10
            mask |= b(s2,2)<<9;  mask |= b(s2,0)<<8;  mask |= b(s0,1)<<7
            mask |= b(s0,4)<<6;  mask |= b(s1,3)<<5;  mask |= b(s0,2)<<4
            mask |= b(s1,2)<<3;  mask |= b(s0,3)<<2;  mask |= b(s1,0)<<1
            mask |= b(s1,1)
        else:
            mask |= b(s0,2)<<12; mask |= b(s0,1)<<11; mask |= b(s0,0)<<10
            mask |= b(s0,4)<<9;  mask |= b(s0,3)<<8;  mask |= b(s1,2)<<7
            mask |= b(s1,1)<<6;  mask |= b(s1,0)<<5;  mask |= b(s1,4)<<4
            mask |= b(s1,3)<<3;  mask |= b(s2,2)<<2;  mask |= b(s2,1)<<1
            mask |= b(s2,0)
        return mask

    # ----------------------------------------------------------
    # Voltage
    # ----------------------------------------------------------
    @staticmethod
    def encode_voltage_set(volts: float) -> tuple:
        volts = min(16.5, max(7.9, volts))
        code  = int((volts - 7.9) * 950 / 8.44)
        return 0x40 | ((code >> 5) & 0x1F), 0x40 | (code & 0x1F)

    @staticmethod
    def decode_voltage_set(s3: int, s4: int) -> float:
        code = ((s3 & 0x1F) << 5) | (s4 & 0x1F)
        return 7.9 + code * 8.44 / 950

    @staticmethod
    def encode_voltage_actual(volts: float) -> tuple:
        # inverse of: 0.2 + code*12.5*3.3/1023
        code = max(0, int((volts - 0.2) * 1023 / (12.5 * 3.3)))
        code = min(0x3FF, code)
        return 0x40 | ((code >> 5) & 0x1F), 0x40 | (code & 0x1F)

    # ----------------------------------------------------------
    # Duration  (10-bit)
    # ----------------------------------------------------------
    @staticmethod
    def encode_duration(d: int) -> tuple:
        return 0x60 | ((d >> 5) & 0x1F), 0x60 | (d & 0x1F)

    @staticmethod
    def decode_duration(s7: int, s8: int) -> int:
        return ((s7 & 0x1F) << 5) | (s8 & 0x1F)

    # ----------------------------------------------------------
    # Frequency dividend  (20-bit)
    # ----------------------------------------------------------
    @staticmethod
    def encode_dividend(dividend: float) -> tuple:
        code = int(math.floor(1e10 / dividend / 625 - 1))
        return (0x60|(0x1F&(code>>15)), 0x60|(0x1F&(code>>10)),
                0x60|(0x1F&(code>>5)),  0x60|(0x1F& code))

    @staticmethod
    def decode_dividend(s9, s10, s11, s12) -> float:
        code  = (s9  & 0x1F) << 15
        code |= (s10 & 0x1F) << 10
        code |= (s11 & 0x1F) << 5
        code |= (s12 & 0x1F)
        return 1e10 / 625 / (code + 1)

    # ----------------------------------------------------------
    # Frequency divider  (15-bit)
    # ----------------------------------------------------------
    @staticmethod
    def encode_divider(divider: int) -> tuple:
        code = divider - 1
        return (0x60|(0x1F&(code>>10)), 0x60|(0x1F&(code>>5)),
                0x60|(0x1F& code))

    @staticmethod
    def decode_divider(s13, s14, s15) -> int:
        code  = (s13 & 0x1F) << 10
        code |= (s14 & 0x1F) << 5
        code |= (s15 & 0x1F)
        return 1 + code

    # ----------------------------------------------------------
    # Width  (10-bit)
    # ----------------------------------------------------------
    @staticmethod
    def encode_width(w: int) -> tuple:
        return 0x60 | (0x1F & (w >> 5)), 0x60 | (0x1F & w)

    @staticmethod
    def decode_width(s16, s17) -> int:
        return ((s16 & 0x1F) << 5) | (s17 & 0x1F)

    # ----------------------------------------------------------
    # Temperature  (13-bit, two sub-protocols)
    # ----------------------------------------------------------
    @staticmethod
    def encode_temperature(temp: float, release: int) -> tuple:
        sign_bit = 1 if temp < 0 else 0
        if release < 6:
            # Protocol v4.5
            abs_t = abs(temp)
            msb   = int(abs_t / 32) & 0x03
            mid   = int(abs_t % 32) & 0x1F
            lsb   = int((abs_t - int(abs_t)) / 0.0625) & 0x1F
            s18 = 0x80 | (sign_bit << 2) | msb
            s19 = 0x80 | mid
            s20 = 0x80 | lsb
        else:
            # Protocol v4.6
            code = int((abs(temp) + 46.85) * 4096 / 175.72) & 0xFFF
            s18 = 0x80 | (sign_bit << 2) | ((code >> 10) & 0x03)
            s19 = 0x80 | ((code >> 5) & 0x1F)
            s20 = 0x80 | (code & 0x1F)
        return s18, s19, s20

    # ----------------------------------------------------------
    # Humidity  (12-bit, FF release>=6 only)
    # ----------------------------------------------------------
    @staticmethod
    def encode_humidity(rh: float) -> tuple:
        code = int((rh + 6.0) * 4096 / 125) & 0xFFF
        s23 = 0x80 | ((code >> 10) & 0x03)
        s24 = 0x80 | ((code >>  5) & 0x1F)
        s25 = 0x80 | (code & 0x1F)
        return s23, s24, s25

    # ----------------------------------------------------------
    # Control mask  (s22)
    # ----------------------------------------------------------
    @staticmethod
    def encode_control(enabled, lemo, fiber1, fiber2, external) -> int:
        return (0xA0 | int(enabled) | (int(lemo)<<1) |
                (int(fiber1)<<2) | (int(fiber2)<<3) | (int(external)<<4))

    @staticmethod
    def decode_control(s22: int) -> dict:
        ctrl = s22 & 0x1F
        return {
            "enabled" : bool(ctrl & 0x01),
            "lemo_out": bool((ctrl >> 1) & 0x01),
            "fiber1_out": bool((ctrl >> 2) & 0x01),
            "fiber2_out": bool((ctrl >> 3) & 0x01),
            "external": bool((ctrl >> 4) & 0x01),
        }

    # ----------------------------------------------------------
    # Central current  (20-bit, SPE only)
    # ----------------------------------------------------------
    @staticmethod
    def encode_central_current(mA: float) -> tuple:
        code = int(mA / _CENTRAL_CURRENT_STEP_MA)
        code = max(min(code, 31457), 0)
        return tuple(0x40 | (0x1F & (code >> ((3-i)*5))) for i in range(4))

    @staticmethod
    def decode_central_current(s23, s24, s25, s26) -> float:
        code = 0
        for i, b in enumerate([s23, s24, s25, s26]):
            code |= (b & 0x1F) << ((3-i)*5)
        return code * _CENTRAL_CURRENT_STEP_MA

    # ----------------------------------------------------------
    # Build full status frame from DeviceState
    # ----------------------------------------------------------
    @classmethod
    def build_status(cls, st: DeviceState) -> bytes:
        n = cls.status_size(st.product, st.release)
        s = bytearray(n)

        # S0-S2: LED mask
        s[0], s[1], s[2] = cls.encode_leds(st.led_mask, st.product)
        # S3-S4: voltage set
        s[3], s[4] = cls.encode_voltage_set(st.voltage_set)
        # S5-S6: voltage actual
        s[5], s[6] = cls.encode_voltage_actual(st.voltage_actual)
        # S7-S8: duration
        s[7], s[8] = cls.encode_duration(st.duration)
        # S9-S12: frequency dividend
        s[9], s[10], s[11], s[12] = cls.encode_dividend(st.frequency_dividend)
        # S13-S15: frequency divider
        s[13], s[14], s[15] = cls.encode_divider(st.frequency_divider)
        # S16-S17: pulse width
        s[16], s[17] = cls.encode_width(st.width)
        # S18-S20: temperature
        s[18], s[19], s[20] = cls.encode_temperature(st.temperature, st.release)
        # S21: error/fault mask
        s[21] = 0xA0 | (st.error_mask & 0x1F)
        # S22: control mask
        s[22] = cls.encode_control(st.enabled, st.lemo_out,
                                   st.fiber1_out, st.fiber2_out, st.external)
        # SPE-specific: S23-S26 central current; humidity not in SPE
        if st.product == ProductCode.SPE:
            s[23], s[24], s[25], s[26] = cls.encode_central_current(
                st.central_current
            )
            # Tail: product code, product id (LSB, MSB), release, CR, LF
            s[27] = st.product & 0xFF
            s[28] = st.product_id & 0x1F
            s[29] = (st.product_id >> 4) & 0x1F
            s[30] = st.release & 0x1F
            s[31] = 0x0D
            s[32] = 0x0A
        else:
            # FF: humidity in S23-S25 (release>=6), then tail
            if st.release >= 6:
                s[23], s[24], s[25] = cls.encode_humidity(st.humidity)
            tail_offset = n - 6
            s[tail_offset]   = st.product & 0xFF
            s[tail_offset+1] = st.product_id & 0x1F
            s[tail_offset+2] = (st.product_id >> 4) & 0x1F
            s[tail_offset+3] = st.release & 0x1F
            s[n-2] = 0x0D
            s[n-1] = 0x0A

        log.debug("Status frame (%d B): %s", n, s.hex().upper())
        return bytes(s)

    # ----------------------------------------------------------
    # Decode an incoming Configure command frame into a state dict
    # ----------------------------------------------------------
    @classmethod
    def decode_configure(cls, frame: bytes, product: int) -> dict:
        """
        Parse a Configure command frame and return a dict of field
        updates suitable for DeviceState.update().
        """
        if len(frame) < cls.command_size(product):
            raise ValueError(
                f"Configure frame too short: {len(frame)} < "
                f"{cls.command_size(product)}"
            )

        # C0-C2: LED mask
        led_mask = cls.decode_leds(frame[0], frame[1], frame[2], product)
        # C3-C4: voltage set
        voltage_set = cls.decode_voltage_set(frame[3], frame[4])
        # C5-C6: duration
        duration = cls.decode_duration(frame[5], frame[6])
        # C7-C10: frequency dividend
        frequency_dividend = cls.decode_dividend(
            frame[7], frame[8], frame[9], frame[10]
        )
        # C11-C13: frequency divider
        frequency_divider = cls.decode_divider(frame[11], frame[12], frame[13])
        # C14-C15: pulse width
        width = cls.decode_width(frame[14], frame[15])
        # C16: control mask
        ctrl = cls.decode_control(frame[16])

        result = dict(
            led_mask           = led_mask,
            voltage_set        = voltage_set,
            duration           = duration,
            frequency_dividend = frequency_dividend,
            frequency_divider  = frequency_divider,
            width              = width,
            **ctrl,
        )

        # C17-C20: central current (SPE only)
        if product == ProductCode.SPE and len(frame) >= 21:
            result["central_current"] = cls.decode_central_current(
                frame[17], frame[18], frame[19], frame[20]
            )

        return result


# ==========================================================
# TCP Server
# ==========================================================
class TcpEmulator:
    """
    Listens for a single TCP connection at a time and handles the
    CalBox binary protocol.
    """

    IO_TIMEOUT = 5.0

    def __init__(self, state: DeviceState, port: int, password: str = ""):
        self.state    = state
        self.port     = port
        self.password = password
        self._server_sock: Optional[socket.socket] = None

    # ----------------------------------------------------------
    def start(self):
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind(("0.0.0.0", self.port))
        self._server_sock.listen(1)
        log.info("TCP emulator listening on port %d", self.port)

        while True:
            try:
                conn, addr = self._server_sock.accept()
                log.info("Client connected from %s:%d", *addr)
                self.state.update(connected=True)
                try:
                    self._handle_connection(conn)
                except _RebootRequested:
                    log.info("Reboot: closing client connection.")
                except Exception as exc:
                    log.error("Connection error: %s", exc)
                finally:
                    conn.close()
                    self.state.update(connected=False)
                    log.info("Client disconnected.")
            except OSError:
                break   # server socket closed

    def stop(self):
        if self._server_sock:
            self._server_sock.close()

    # ----------------------------------------------------------
    def _send_all(self, conn: socket.socket, data: bytes):
        total = 0
        while total < len(data):
            sent = conn.send(data[total:])
            if sent == 0:
                raise OSError("Socket closed during send")
            total += sent
        log.debug("Sent %d B: %s", total, data.hex().upper())

    def _recv_exactly(self, conn: socket.socket, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = conn.recv(n - len(buf))
            if not chunk:
                raise OSError("Connection closed by client")
            buf += chunk
        return buf

    # ----------------------------------------------------------
    def _handle_connection(self, conn: socket.socket):
        conn.settimeout(self.IO_TIMEOUT)

        # Authentication
        if self.password:
            prompt = b"\n\nPassword ? "   # 12 chars + space = 13 bytes
            self._send_all(conn, prompt)
            received = b""
            while not received.endswith(b"\r\n"):
                received += conn.recv(1)
            supplied = received[:-2].decode(errors="replace")
            if supplied != self.password:
                log.warning("Authentication failed (wrong password)")
                return
            self._send_all(conn, b"\r\n")  # 2-byte echo
            log.info("Client authenticated.")
        else:
            log.info("No password required, skipping auth.")

        # Command loop
        while True:
            # Read first byte to determine frame type
            header = self._recv_exactly(conn, 1)
            cmd_byte = header[0]

            if cmd_byte in (Cmd.GET_STATUS, Cmd.START, Cmd.STOP, Cmd.RESET):
                # 4-byte simple command: read remaining 3 bytes
                rest = self._recv_exactly(conn, 3)
                frame = header + rest
                log.debug("Simple cmd 0x%02X frame: %s",
                          cmd_byte, frame.hex().upper())
                self._handle_simple_cmd(conn, cmd_byte)

            else:
                # Assume Configure frame: read until CRLF
                buf = header
                while not buf.endswith(b"\r\n"):
                    buf += self._recv_exactly(conn, 1)
                log.debug("Configure frame (%d B): %s",
                          len(buf), buf.hex().upper())
                self._handle_configure(conn, buf)

    # ----------------------------------------------------------
    def _send_status(self, conn: socket.socket):
        snap = self.state.snapshot()
        # Temporarily promote snapshot fields to DeviceState-like object.
        # Use _skip_threads=True to avoid spawning background threads for this
        # temporary encoder instance (threads only run when the real state object is created).
        st = DeviceState(self.state.product, self.state.release, _skip_threads=True)
        for k, v in snap.items():
            if hasattr(st, k):
                setattr(st, k, v)
        frame = FrameCodec.build_status(st)
        self._send_all(conn, frame)
        log.info("Sent status frame (%d B)", len(frame))

    def _handle_simple_cmd(self, conn: socket.socket, cmd: int):
        if cmd == Cmd.START:
            log.info("CMD: Start")
            self.state.update(enabled=True)
            self.state.arm_duration_timer()
            time.sleep(0.1)
            self._send_status(conn)

        elif cmd == Cmd.STOP:
            log.info("CMD: Stop")
            self.state._cancel_duration_timer()
            self.state.update(enabled=False)
            time.sleep(0.1)
            self._send_status(conn)

        elif cmd == Cmd.GET_STATUS:
            log.info("CMD: GetStatus")
            time.sleep(0.1)
            self._send_status(conn)

        elif cmd == Cmd.RESET:
            log.info("CMD: Reset — resetting state and dropping client connection")
            self.state.reset()
            raise _RebootRequested("Reboot requested by client")

    def _handle_configure(self, conn: socket.socket, frame: bytes):
        log.info("CMD: Configure")
        try:
            updates = FrameCodec.decode_configure(frame, self.state.product)
            log.info("Configure decoded: %s", updates)
            self.state.update(**updates)
        except Exception as exc:
            log.error("Failed to decode Configure frame: %s", exc)
        time.sleep(0.1)
        self._send_status(conn)


# ==========================================================
# CLI
# ==========================================================
HELP = """
╔══════════════════════════════════════════════════════════════╗
║         NectarCAM CalBox Emulator — CLI                      ║
╠══════════════════════════════════════════════════════════════╣
║  Query                                                       ║
║    status               Print all current state variables    ║
║                                                              ║
║  Override sensor readings                                    ║
║    set temperature <float>     Temperature in °C             ║
║    set humidity    <float>     Relative humidity in %RH      ║
║    set voltage_actual <float>  Measured voltage in V         ║
║    set lemo_in     <true|false>                              ║
║    set error_mask  <int>       Fault bitmask (0 = no fault)  ║
║                                                              ║
║  Override device parameters                                  ║
║    set led_mask            <int>                             ║
║    set voltage_set         <float>                           ║
║    set duration            <int>                             ║
║    set frequency_dividend  <float>                           ║
║    set frequency_divider   <int>                             ║
║    set width               <int>                             ║
║    set central_current     <float>                           ║
║    set enabled             <true|false>                      ║
║    set lemo_out            <true|false>                      ║
║    set fiber1_out          <true|false>                      ║
║    set fiber2_out          <true|false>                      ║
║    set external            <true|false>                      ║
║                                                              ║
║  Other                                                       ║
║    ?  /  help              Show this menu                    ║
║    exit  /  quit           Stop the emulator                 ║
╚══════════════════════════════════════════════════════════════╝
"""


def _parse_value(raw: str):
    """Auto-detect bool / int / float from a string."""
    if raw.lower() == "true":  return True
    if raw.lower() == "false": return False
    try:    return int(raw, 0)
    except ValueError: pass
    return float(raw)


def run_cli(state: DeviceState, stop_event: threading.Event):
    print(HELP)
    while not stop_event.is_set():
        try:
            raw = input("emulator>> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            stop_event.set()
            break

        if not raw:
            continue

        parts = raw.split()
        cmd   = parts[0].lower()

        try:
            if cmd in ("?", "help"):
                print(HELP)

            elif cmd == "status":
                snap = state.snapshot()
                print()
                for k, v in sorted(snap.items()):
                    print(f"  {k:<25} = {v}")
                print()

            elif cmd == "set":
                if len(parts) < 3:
                    print("  Usage: set <field> <value>")
                    continue
                field = parts[1]
                value = _parse_value(parts[2])
                state.update(**{field: value})
                print(f"  OK: {field} = {value}")

            elif cmd in ("exit", "quit"):
                stop_event.set()
                break

            else:
                print(f"  Unknown command '{cmd}'. Type '?' for help.")

        except (ValueError, IndexError) as exc:
            print(f"  Error: {exc}")


# ==========================================================
# Entry point
# ==========================================================
def parse_args():
    p = argparse.ArgumentParser(
        description="NectarCAM Calibration Box TCP Emulator"
    )
    p.add_argument("--port",         type=int,             default=50001,
                   help="TCP port to listen on (default: 50001)")
    p.add_argument("--product-code", type=lambda x: int(x, 0), default=0xAA,
                   help="Product code: 0xAA=SPE (default), 0xA5=FF")
    p.add_argument("--release",      type=int,             default=6,
                   help="Firmware release number (default: 6)")
    p.add_argument("--password",     default="",
                   help="Authentication password (omit for none)")
    p.add_argument("--log-level",    default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


def main():
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    state = DeviceState(product=args.product_code, release=args.release)
    emulator = TcpEmulator(state, port=args.port, password=args.password)
    stop_event = threading.Event()

    # TCP thread
    tcp_thread = threading.Thread(target=emulator.start, daemon=True,
                                  name="tcp-emulator")
    tcp_thread.start()

    # CLI runs on the main thread so readline / input() work normally
    try:
        run_cli(state, stop_event)
    finally:
        log.info("Stopping emulator …")
        state.stop_background_tasks()
        emulator.stop()
        tcp_thread.join(timeout=2)
        log.info("Done.")


if __name__ == "__main__":
    main()
