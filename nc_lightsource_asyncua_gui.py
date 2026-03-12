"""
nc_lightsource_asyncua_gui.py
NectarCAM Calibration Box — OPC UA GUI Client (asyncua edition)

Modern tkinter GUI replicating the functionality of the legacy
Ethernet-based client, talking to the OPC UA bridge server via asyncua.

Architecture
------------
* One persistent asyncio event loop runs in a background daemon thread.
* All OPC UA coroutines are dispatched to that loop with
  asyncio.run_coroutine_threadsafe(), which returns a concurrent.futures.Future.
* tkinter mainloop stays on the main thread; GUI updates are posted back via
  widget.after(0, callback).

Usage:
    python nc_lightsource_asyncua_gui.py [--endpoint opc.tcp://host:4840/nectarcam/]

Requires:
    pip install asyncua
"""

import argparse
import asyncio
import concurrent.futures
import math
import threading
import time
import tkinter as tk
from tkinter import messagebox

# ---------------------------------------------------------------------------
# Optional asyncua import — GUI works in "demo / offline" mode without it
# ---------------------------------------------------------------------------
try:
    from asyncua import Client, ua
    ASYNCUA_AVAILABLE = True
except ImportError:
    ASYNCUA_AVAILABLE = False

DEFAULT_ENDPOINT = "opc.tcp://localhost:4840/nectarcam/"

# ---------------------------------------------------------------------------
# Colour palette
# ---------------------------------------------------------------------------
BG       = "#1e2130"
BG2      = "#252a3d"
BG3      = "#2d3354"
ACCENT   = "#4f8ef7"
ACCENT2  = "#3ecf8e"
RED      = "#e05c5c"
YELLOW   = "#f0c040"
TEXT     = "#dde3f5"
TEXT_DIM = "#7880a0"
BORDER   = "#3a4060"

LED_ON   = "#4488ff"
LED_OFF  = "#1a2040"
LED_RING = "#2255aa"


def _darken(hex_color: str, factor=0.75) -> str:
    """Return a slightly darker shade of a hex colour for hover/press."""
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i:i+2], 16) for i in (0, 2, 4))
    return "#{:02x}{:02x}{:02x}".format(int(r*factor), int(g*factor), int(b*factor))


class FlatButton(tk.Label):
    """
    Label-based button that is immune to macOS Aqua white-washing.
    tk.Button on macOS always renders as a native white button when the
    window is active, regardless of bg/highlightbackground. tk.Label has
    no such restriction, so we simulate button behaviour here.
    """
    def __init__(self, parent, text, command, bg=BG3, fg="white",
                 font=("Helvetica", 9), padx=8, pady=4, width=None, **kw):
        self._bg       = bg
        self._fg       = fg
        self._command  = command
        self._enabled  = True
        self._hover_bg = _darken(bg)
        # Drop kwargs that apply to tk.Button but not tk.Label
        for k in ("relief", "highlightbackground", "activebackground",
                  "activeforeground", "cursor"):
            kw.pop(k, None)
        label_kw = dict(bg=bg, fg=fg, font=font,
                        padx=padx, pady=pady)
        if width is not None:
            label_kw["width"] = width
        label_kw.update(kw)
        super().__init__(parent, text=text, **label_kw)
        self.bind("<ButtonPress-1>",   self._on_press)
        self.bind("<ButtonRelease-1>", self._on_release)
        self.bind("<Enter>",           self._on_enter)
        self.bind("<Leave>",           self._on_leave)

    def _on_press(self, _=None):
        if self._enabled:
            super().config(bg=self._hover_bg)

    def _on_release(self, _=None):
        if self._enabled:
            super().config(bg=self._bg)
            if self._command:
                self._command()

    def _on_enter(self, _=None):
        if self._enabled:
            super().config(bg=self._hover_bg)

    def _on_leave(self, _=None):
        super().config(bg=self._bg)

    def config(self, **kw):
        state = kw.pop("state", None)
        if state == "disabled":
            self._enabled = False
            super().config(fg=TEXT_DIM, cursor="")
        elif state == "normal":
            self._enabled = True
            super().config(fg=self._fg)
        if "bg" in kw:
            self._bg       = kw["bg"]
            self._hover_bg = _darken(self._bg)
        if "fg" in kw:
            self._fg = kw["fg"]
        if kw:
            super().config(**kw)

    configure = config


def btn(parent, text, command, bg=BG3, fg="white",
        font=("Helvetica", 9), padx=6, pady=3, **kw):
    """Convenience factory — returns a FlatButton."""
    return FlatButton(parent, text=text, command=command,
                      bg=bg, fg=fg, font=font, padx=padx, pady=pady, **kw)




# ===========================================================================
# Asyncio event-loop thread
# ===========================================================================
class AsyncLoopThread:
    """Owns a running asyncio event loop in a daemon thread."""

    def __init__(self):
        self._loop   = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="asyncua-loop"
        )
        self._thread.start()

    def _run(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def submit(self, coro) -> concurrent.futures.Future:
        """Schedule a coroutine and return a Future (thread-safe)."""
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def stop(self):
        self._loop.call_soon_threadsafe(self._loop.stop)


# ===========================================================================
# OPC UA client wrapper  (async — mirrors nc_lightsource_asyncua_test_cli.py)
# ===========================================================================
class CalibrationBoxClient:

    def __init__(self, endpoint: str, username=None, password=None):
        self._endpoint = endpoint
        self._username = username
        self._password = password
        self.client     = None
        self.device     = None
        self.monitoring = None
        self._sub       = None
        self.on_data_change = None   # callable(name, value) — called from loop thread

    # ------------------------------------------------------------------
    async def connect(self):
        self.client = Client(self._endpoint, watchdog_intervall=30.0)
        if self._username:
            self.client.set_user(self._username)
            self.client.set_password(self._password or "")
        await self.client.connect()
        root = self.client.get_root_node()
        self.device = await root.get_child(
            ["0:Objects", "2:CalibrationLightSource"]
        )
        self.monitoring = await self.device.get_child(["2:Monitoring"])

    async def disconnect(self):
        await self._stop_subscription()
        if self.client:
            try:
                await self.client.disconnect()
            except Exception:
                pass
            self.client = None

    # ------------------------------------------------------------------
    async def call_method(self, name: str, *args):
        method = await self.device.get_child([f"2:{name}"])
        if args:
            await self.device.call_method(method, *args)
        else:
            await self.device.call_method(method)

    # ------------------------------------------------------------------
    async def read_all(self) -> dict:
        result = {}
        for container in await self.monitoring.get_children():
            name = (await container.read_browse_name()).Name
            try:
                leaf  = await container.get_child([f"2:{name}_v"])
                value = await leaf.get_value()
            except Exception:
                try:
                    value = await container.get_value()
                except Exception:
                    value = None
            result[name] = value
        return result

    # ------------------------------------------------------------------
    class _SubHandler:
        def __init__(self, names, callback):
            self._names    = names
            self._callback = callback

        def datachange_notification(self, node, val, data):
            name = self._names.get(node.nodeid, str(node.nodeid))
            if self._callback:
                self._callback(name, val)

        def event_notification(self, event):
            pass

    async def start_subscription(self):
        await self._stop_subscription()
        names   = {}
        handler = self._SubHandler(names, self.on_data_change)
        self._sub = await self.client.create_subscription(500, handler)
        for container in await self.monitoring.get_children():
            cname = (await container.read_browse_name()).Name
            try:
                leaf = await container.get_child([f"2:{cname}_v"])
            except Exception:
                leaf = container
            await self._sub.subscribe_data_change(leaf)
            names[leaf.nodeid] = cname

    async def _stop_subscription(self):
        if self._sub:
            try:
                await self._sub.delete()
            except Exception:
                pass
            self._sub = None


# ===========================================================================
# Gauge widget — analogue arc needle
# ===========================================================================
# Gauge widget — owns a Canvas internally (avoids tk.Canvas subclass issues)
# ===========================================================================
class GaugeWidget(tk.Frame):

    def __init__(self, parent, min_val=-16.5, max_val=-7.5,
                 width=220, height=130, **kw):
        kw.setdefault("bg", BG2)
        super().__init__(parent, **kw)
        self._min    = min_val
        self._max    = max_val
        self._value  = min_val
        self._width  = width
        self._height = height
        self._canvas = tk.Canvas(self, width=width, height=height,
                                 bg=BG2, highlightthickness=0)
        self._canvas.pack()
        self._canvas.bind("<Map>", lambda e: self._draw())

    def set_value(self, v):
        self._value = max(self._min, min(self._max, v))
        self._draw()

    def _draw(self):
        c = self._canvas
        c.delete("all")
        cx, cy = self._width // 2, self._height - 20
        r = min(cx, cy + 20) - 14

        zones = [
            (self._min, self._min + (self._max - self._min) * 0.35, ACCENT2),
            (self._min + (self._max - self._min) * 0.35,
             self._min + (self._max - self._min) * 0.65, YELLOW),
            (self._min + (self._max - self._min) * 0.65, self._max, RED),
        ]
        for z_min, z_max, col in zones:
            a0 = self._angle(z_min)
            a1 = self._angle(z_max)
            c.create_arc(cx - r, cy - r, cx + r, cy + r,
                         start=a0, extent=(a1 - a0),
                         style="arc", outline=col, width=10)

        for v in [self._min,
                  self._min + (self._max - self._min) * 0.25,
                  self._min + (self._max - self._min) * 0.5,
                  self._min + (self._max - self._min) * 0.75,
                  self._max]:
            ang = math.radians(self._angle(v))
            tx  = cx + (r + 12) * math.cos(ang)
            ty  = cy - (r + 12) * math.sin(ang)
            c.create_text(tx, ty, text=f"{v:.0f}",
                          fill=TEXT_DIM, font=("Helvetica", 7))

        ang = math.radians(self._angle(self._value))
        nx  = cx + r * 0.82 * math.cos(ang)
        ny  = cy - r * 0.82 * math.sin(ang)
        c.create_line(cx, cy, nx, ny, fill=TEXT, width=2)
        c.create_oval(cx - 5, cy - 5, cx + 5, cy + 5,
                      fill=TEXT, outline="")
        c.create_text(cx, cy - 10, text=f"{self._value:.4f}",
                      fill=TEXT, font=("Helvetica", 9, "bold"))

    def _angle(self, v):
        frac = (v - self._min) / (self._max - self._min)
        return 210 - frac * 180


# ===========================================================================
# LED array widget
# ===========================================================================
LED_ON_ENABLED  = "#3ecf8e"   # green — selected AND light source enabled
LED_ON_DISABLED = "#4488ff"   # blue  — selected, light source off

class LEDArrayWidget(tk.Frame):
    """
    Two independent masks:
      _sel_mask   — user selection (what will be sent on Apply)
      _dev_mask   — last reported mask from the device (shown as 'x' overlay)

    LED fill colour tracks _enabled:
      True  → selected LEDs shown in green (LED_ON_ENABLED)
      False → selected LEDs shown in blue  (LED_ON_DISABLED)
    """

    LED_POSITIONS = [
        ("L11", 0, 0), ("L12", 0, 2), ("L13", 0, 4),
        ("L9",  1, 1), ("L10", 1, 3),
        ("L6",  2, 0), ("L7",  2, 2), ("L8",  2, 4),
        ("L4",  3, 1), ("L5",  3, 3),
        ("L1",  4, 0), ("L2",  4, 2), ("L3",  4, 4),
    ]
    LED_NUMBERS = [11, 12, 13, 9, 10, 6, 7, 8, 4, 5, 1, 2, 3]

    def __init__(self, parent, on_mask_change=None, **kw):
        super().__init__(parent, bg=BG2, **kw)
        self._sel_mask       = 0      # user-selected (pending Apply)
        self._dev_mask       = 0      # reported by device
        self._enabled        = False  # light source running?
        self._initialised    = False  # selector seeded from device yet?
        self._on_mask_change = on_mask_change
        self._ovals          = {}
        self._x_texts        = {}     # num -> canvas text id for 'x' marker
        self._canvases       = {}
        self._build()

    def _build(self):
        R = 22; PAD = 6; SIZE = R * 2 + PAD
        # Big oval: (PAD//2, PAD//2) -> (SIZE-PAD//2, SIZE-PAD//2)
        # so its centre is exactly (SIZE//2, SIZE//2)
        cx = SIZE // 2          # = 25
        cy = SIZE // 2          # = 25
        r_dot = R // 3          # one-third the LED radius (≈7 px)
        for (label, row, col), num in zip(self.LED_POSITIONS, self.LED_NUMBERS):
            c = tk.Canvas(self, width=SIZE, height=SIZE + 14,
                          bg=BG2, highlightthickness=0, cursor="hand2")
            c.grid(row=row, column=col, padx=4, pady=4)
            oval = c.create_oval(PAD//2, PAD//2, SIZE - PAD//2, SIZE - PAD//2,
                                 fill=LED_OFF, outline=LED_RING, width=2)
            # Small white circle — device-reported mask indicator, centred on LED
            dot = c.create_oval(cx - r_dot, cy - r_dot,
                                cx + r_dot, cy + r_dot,
                                fill="white", outline="", state="hidden")
            c.create_text(SIZE // 2, SIZE + 6, text=label,
                          fill=TEXT_DIM, font=("Helvetica", 8))
            self._ovals[num]    = oval
            self._x_texts[num]  = dot
            self._canvases[num] = c
            c.bind("<Button-1>", lambda e, n=num: self._toggle(n))

    def _toggle(self, num):
        self._sel_mask ^= (1 << (num - 1))
        self._refresh()
        if self._on_mask_change:
            self._on_mask_change(self._sel_mask)

    # ---- public setters ------------------------------------------------
    def set_mask(self, mask: int):
        """Set the user-selection mask (e.g. from bitmask entry)."""
        self._sel_mask = mask
        self._refresh()

    def set_device_mask(self, mask: int):
        """Update the device-reported mask (shown as dot overlay).
        On the first call the selector is also initialised to match."""
        self._dev_mask = mask
        if not self._initialised:
            self._sel_mask = mask
            self._initialised = True
            if self._on_mask_change:
                self._on_mask_change(self._sel_mask)
        self._refresh()

    def set_enabled(self, enabled: bool):
        """Set whether the light source is currently running."""
        self._enabled = enabled
        self._refresh()

    def get_mask(self) -> int:
        return self._sel_mask

    # ---- rendering --------------------------------------------------------
    def _refresh(self):
        on_col = LED_ON_ENABLED if self._enabled else LED_ON_DISABLED
        for num, oval in self._ovals.items():
            bit = 1 << (num - 1)
            selected = bool(self._sel_mask & bit)
            on_device = bool(self._dev_mask & bit)
            self._canvases[num].itemconfig(oval,
                fill=on_col if selected else LED_OFF)
            self._canvases[num].itemconfig(self._x_texts[num],
                state="normal" if on_device else "hidden")


# ===========================================================================
# Small indicator LED
# ===========================================================================
class Indicator(tk.Canvas):
    def __init__(self, parent, size=12, color_on=ACCENT2,
                 color_off="#2a3040", **kw):
        kw.setdefault("bg", BG2)
        super().__init__(parent, width=size, height=size,
                         highlightthickness=0, **kw)
        self._on = color_on; self._off = color_off
        self._size = size; self._state = False
        self._draw()

    def set_state(self, on: bool):
        self._state = on; self._draw()

    def _draw(self):
        self.delete("all")
        s = self._size
        self.create_oval(1, 1, s - 1, s - 1,
                         fill=self._on if self._state else self._off,
                         outline="")


# ===========================================================================
# Main application window
# ===========================================================================
class CalibBoxGUI(tk.Tk):

    def __init__(self, endpoint: str, username=None, password=None):
        super().__init__()
        self.title("NectarCAM Calibration Box — OPC UA Control")
        self.configure(bg=BG)
        self.resizable(True, True)

        self._endpoint  = endpoint
        self._username  = username
        self._password  = password
        self._client    = None
        self._connected = False
        self._poll_job  = None
        self._info_host_var    = None
        self._info_port_var    = None
        self._info_dialect_var = None
        self._info_state_var   = None

        # Single asyncio loop for all OPC UA work
        self._async = AsyncLoopThread()

        self._build_ui()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    # -----------------------------------------------------------------------
    # Dispatch helper — run a coroutine on the asyncua loop, callback on GUI
    # -----------------------------------------------------------------------
    def _dispatch(self, coro, on_done=None, on_error=None):
        """
        Schedule `coro` on the asyncua event loop.
        on_done(result) and on_error(exc) are called back on the tkinter thread
        via self.after(0, ...).
        """
        fut = self._async.submit(coro)

        def _cb(f):
            exc = f.exception()
            if exc:
                if on_error:
                    self.after(0, on_error, exc)
            else:
                if on_done:
                    self.after(0, on_done, f.result())
        fut.add_done_callback(_cb)
        return fut

    def _run_method(self, name, *args, success_msg=None):
        """Convenience: call an OPC UA device method and log result."""
        if not self._connected:
            self._log(f"[demo] {name}({', '.join(str(a) for a in args)})",
                      TEXT_DIM)
            return
        def ok(_):
            msg = success_msg or f"{name}() OK"
            self._set_status(msg)
            self._log(msg, ACCENT2)
        def err(exc):
            msg = f"{name}() error: {exc}"
            self._set_status(msg)
            self._log(msg, RED)
        self._dispatch(self._client.call_method(name, *args),
                       on_done=ok, on_error=err)

    # -----------------------------------------------------------------------
    # UI construction  (unchanged from opcua version)
    # -----------------------------------------------------------------------
    def _section(self, parent, title):
        return tk.LabelFrame(parent, text=f"  {title}  ",
                             bg=BG2, fg=TEXT_DIM,
                             font=("Helvetica", 9),
                             highlightthickness=1,
                             bd=0, labelanchor="nw")

    def _build_ui(self):
        # Top bar
        top = tk.Frame(self, bg=BG3, pady=6)
        top.pack(fill="x")

        tk.Label(top, text="NectarCAM  ·  Calibration Box Control",
                 bg=BG3, fg=TEXT,
                 font=("Helvetica", 14, "bold")).pack(side="left", padx=16)

        conn_frame = tk.Frame(top, bg=BG3)
        conn_frame.pack(side="right", padx=14)

        tk.Label(conn_frame, text="OPC UA Endpoint:", bg=BG3,
                 fg=TEXT_DIM, font=("Helvetica", 9)).pack(side="left")
        self._ep_var = tk.StringVar(value=self._endpoint)
        tk.Entry(conn_frame, textvariable=self._ep_var, width=36,
                 bg=BG2, fg=TEXT, insertbackground=TEXT,
                 font=("Helvetica", 9)).pack(side="left", padx=(4, 8))

        self._conn_ind = Indicator(conn_frame, size=14, bg=BG3)
        self._conn_ind.pack(side="left", padx=(0, 6))

        self._conn_btn = btn(
            conn_frame, text="Connect", command=self._toggle_connect,
            bg=ACCENT, fg="white",
            font=("Helvetica", 9, "bold"), padx=10, pady=3)
        self._conn_btn.pack(side="left")

        # Status bar
        self._status_var = tk.StringVar(
            value="Disconnected — not connected to server")
        tk.Label(self, textvariable=self._status_var,
                 bg=BG3, fg=TEXT_DIM, font=("Helvetica", 8),
                 anchor="w", padx=10, pady=3).pack(side="bottom", fill="x")

        # Content
        content = tk.Frame(self, bg=BG)
        content.pack(fill="both", expand=True, padx=12, pady=8)

        left = tk.Frame(content, bg=BG)
        left.pack(side="left", fill="y", padx=(0, 12))
        self._build_led_panel(left)
        self._build_quick_actions(left)

        right = tk.Frame(content, bg=BG)
        right.pack(side="left", fill="both", expand=True)
        self._build_info_panel(right)
        self._build_voltage_panel(right)
        self._build_params_panel(right)
        self._build_monitoring_panel(right)

    def _build_led_panel(self, parent):
        sec = self._section(parent, "LED Array")
        sec.pack(fill="x", pady=(0, 8))
        inner = tk.Frame(sec, bg=BG2, padx=10, pady=8)
        inner.pack()

        self._led_array = LEDArrayWidget(
            inner, on_mask_change=self._on_led_mask_change)
        self._led_array.pack()

        mask_row = tk.Frame(inner, bg=BG2)
        mask_row.pack(fill="x", pady=(6, 0))
        tk.Label(mask_row, text="Bitmask:", bg=BG2, fg=TEXT_DIM,
                 font=("Helvetica", 8)).pack(side="left")
        self._led_mask_var = tk.IntVar(value=0)
        e = tk.Entry(mask_row, textvariable=self._led_mask_var, width=6,
                     bg=BG, fg=TEXT, insertbackground=TEXT,
                     font=("Helvetica", 9))
        e.pack(side="left", padx=4)
        e.bind("<Return>", self._mask_entry_changed)
        for lbl, cmd in [("Apply", self._apply_leds),
                         ("All On",  lambda: self._set_mask(8191)),
                         ("All Off", lambda: self._set_mask(0))]:
            btn(mask_row, text=lbl, command=cmd,
                      bg=ACCENT if lbl == "Apply" else BG3,
                      fg="white",
                      font=("Helvetica", 8), padx=6).pack(side="left", padx=1)

    def _build_quick_actions(self, parent):
        sec = self._section(parent, "Device Actions")
        sec.pack(fill="x", pady=(0, 8))
        inner = tk.Frame(sec, bg=BG2, padx=10, pady=10)
        inner.pack(fill="x")

        pr = tk.Frame(inner, bg=BG2)
        pr.pack(fill="x", pady=(0, 6))
        tk.Label(pr, text="Light Pulse", bg=BG2, fg=TEXT_DIM,
                 font=("Helvetica", 9)).pack(side="left")
        self._pulse_ind = Indicator(pr, size=16, color_on=ACCENT2, bg=BG2)
        self._pulse_ind.pack(side="left", padx=8)
        btn(pr, text="  Start  ", command=self._cmd_start,
                  bg=ACCENT2, fg="white",
                  font=("Helvetica", 9, "bold"), padx=6).pack(side="left", padx=(0, 6))
        btn(pr, text="  Stop  ", command=self._cmd_stop,
                  bg=RED, fg="white",
                  font=("Helvetica", 9, "bold"), padx=6).pack(side="left")

        for lbl, cmd, col in [
            ("Get Status", self._cmd_getstatus, BG3),
            ("Configure",  self._cmd_configure, ACCENT),
            ("Reconnect",  self._cmd_reconnect, BG3),
            ("Reboot",     self._cmd_reboot,    RED),
        ]:
            btn(inner, text=lbl, command=cmd, bg=col, fg="white", font=("Helvetica", 9), pady=4, width=14).pack(fill="x", pady=2)

    def _build_info_panel(self, parent):
        sec = self._section(parent, "Connection Info")
        sec.pack(fill="x", pady=(0, 8))
        inner = tk.Frame(sec, bg=BG2, padx=10, pady=6)
        inner.pack(fill="x")

        self._info_host_var    = tk.StringVar(value="—")
        self._info_port_var    = tk.StringVar(value="—")
        self._info_dialect_var = tk.StringVar(value="—")
        self._info_state_var   = tk.StringVar(value="—")

        _STATE_LABELS = {0: "Disabled", 1: "Enabled", 2: "Fault", 3: "Offline"}

        def lbl_row(fr, label, var, color=TEXT):
            tk.Label(fr, text=label, bg=BG2, fg=TEXT_DIM,
                     font=("Helvetica", 8), width=8, anchor="w").pack(side="left")
            tk.Label(fr, textvariable=var, bg=BG2, fg=color,
                     font=("Helvetica", 9, "bold")).pack(side="left", padx=(2, 14))

        row = tk.Frame(inner, bg=BG2)
        row.pack(fill="x")
        lbl_row(row, "Host:",    self._info_host_var,    ACCENT)
        lbl_row(row, "Port:",    self._info_port_var,    ACCENT)
        lbl_row(row, "Dialect:", self._info_dialect_var, ACCENT)

        # State indicator with colour-coded label
        tk.Label(row, text="State:", bg=BG2, fg=TEXT_DIM,
                 font=("Helvetica", 8), width=6, anchor="w").pack(side="left")
        self._state_label = tk.Label(row, textvariable=self._info_state_var,
                                     bg=BG2, fg=TEXT_DIM,
                                     font=("Helvetica", 9, "bold"))
        self._state_label.pack(side="left")

    def _build_voltage_panel(self, parent):
        sec = self._section(parent, "LED Voltage")
        sec.pack(fill="x", pady=(0, 8))
        inner = tk.Frame(sec, bg=BG2, padx=10, pady=8)
        inner.pack(fill="x")
        row = tk.Frame(inner, bg=BG2)
        row.pack(fill="x")

        self._gauge = GaugeWidget(row, min_val=7.9, max_val=16.5,
                                  width=220, height=120)
        self._gauge.pack(side="left", padx=(0, 12))

        vr = tk.Frame(row, bg=BG2)
        vr.pack(side="left", fill="y")

        uv = tk.Frame(vr, bg=BG2)
        uv.pack(anchor="w", pady=(0, 4))
        tk.Label(uv, text="Under", bg=BG2, fg=TEXT_DIM,
                 font=("Helvetica", 8)).pack(side="left")
        self._uv_ind = Indicator(uv, color_on=ACCENT2, bg=BG2)
        self._uv_ind.pack(side="left", padx=4)
        tk.Label(uv, text="Voltage", bg=BG2, fg=TEXT_DIM,
                 font=("Helvetica", 8)).pack(side="left", padx=(0, 6))
        self._ov_ind = Indicator(uv, color_on=RED, bg=BG2)
        self._ov_ind.pack(side="left", padx=4)
        tk.Label(uv, text="Over", bg=BG2, fg=TEXT_DIM,
                 font=("Helvetica", 8)).pack(side="left")

        tr = tk.Frame(vr, bg=BG2)
        tr.pack(anchor="w", pady=4)
        tk.Label(tr, text="Inside Temp:", bg=BG2, fg=TEXT_DIM,
                 font=("Helvetica", 8)).pack(side="left")
        self._temp_var = tk.StringVar(value="—")
        tk.Label(tr, textvariable=self._temp_var, bg=BG2, fg=ACCENT,
                 font=("Helvetica", 11, "bold")).pack(side="left", padx=4)
        tk.Label(tr, text="°C", bg=BG2, fg=TEXT_DIM,
                 font=("Helvetica", 8)).pack(side="left")

        vsr = tk.Frame(vr, bg=BG2)
        vsr.pack(anchor="w", pady=4)
        tk.Label(vsr, text="Set Voltage:", bg=BG2, fg=TEXT_DIM,
                 font=("Helvetica", 8)).pack(side="left")
        self._volt_set_var = tk.DoubleVar(value=16.0)
        tk.Entry(vsr, textvariable=self._volt_set_var, width=7,
                 bg=BG, fg=TEXT, insertbackground=TEXT,
                 font=("Helvetica", 9)).pack(side="left", padx=4)
        tk.Label(vsr, text="V", bg=BG2, fg=TEXT_DIM,
                 font=("Helvetica", 8)).pack(side="left")
        btn(vsr, text="Set", command=self._apply_voltage,
                  bg=ACCENT, fg="white",
                  font=("Helvetica", 8), padx=6).pack(side="left", padx=(4, 0))

    def _build_params_panel(self, parent):
        sec = self._section(parent, "Pulse Parameters")
        sec.pack(fill="x", pady=(0, 8))
        inner = tk.Frame(sec, bg=BG2, padx=10, pady=8)
        inner.pack(fill="x")

        def rlabel(fr, text):
            tk.Label(fr, text=text, bg=BG2, fg=TEXT_DIM,
                     font=("Helvetica", 9), width=14,
                     anchor="w").pack(side="left")

        def entry(fr, var, width=7):
            e = tk.Entry(fr, textvariable=var, width=width,
                         bg=BG, fg=TEXT, insertbackground=TEXT, font=("Helvetica", 9))
            e.pack(side="left", padx=2)

        def setbtn(fr, cmd):
            btn(fr, text="Set", command=cmd, bg=ACCENT, fg="white", font=("Helvetica", 8), padx=6).pack(side="left", padx=4)

        # Frequency
        r = tk.Frame(inner, bg=BG2); r.pack(fill="x", pady=2)
        rlabel(r, "Frequency:")
        self._freq_div_var = tk.DoubleVar(value=5000.0)
        self._freq_dvr_var = tk.IntVar(value=1)
        entry(r, self._freq_div_var)
        tk.Label(r, text="/", bg=BG2, fg=TEXT_DIM,
                 font=("Helvetica", 10)).pack(side="left")
        entry(r, self._freq_dvr_var, width=5)
        tk.Label(r, text="Hz", bg=BG2, fg=TEXT_DIM,
                 font=("Helvetica", 9)).pack(side="left", padx=2)
        setbtn(r, self._apply_frequency)

        # Pulse Width
        r = tk.Frame(inner, bg=BG2); r.pack(fill="x", pady=2)
        rlabel(r, "Pulse Width:")
        self._width_var = tk.IntVar(value=4)
        entry(r, self._width_var)
        tk.Label(r, text="× 62.5 ns", bg=BG2, fg=TEXT_DIM,
                 font=("Helvetica", 9)).pack(side="left", padx=2)
        self._pw_result = tk.Label(r, text="250 ns", bg=BG2, fg=ACCENT,
                                   font=("Helvetica", 9, "bold"))
        self._pw_result.pack(side="left", padx=4)
        setbtn(r, self._apply_width)
        self._width_var.trace_add("write", self._update_pw_label)

        # Flash Duration
        r = tk.Frame(inner, bg=BG2); r.pack(fill="x", pady=2)
        rlabel(r, "Flash Duration:")
        self._duration_var = tk.IntVar(value=0)
        entry(r, self._duration_var)
        tk.Label(r, text="μs", bg=BG2, fg=TEXT_DIM,
                 font=("Helvetica", 9)).pack(side="left", padx=2)
        setbtn(r, self._apply_duration)

        # Central LED Current
        r = tk.Frame(inner, bg=BG2); r.pack(fill="x", pady=2)
        rlabel(r, "LED 7 Current:")
        self._current_var = tk.DoubleVar(value=1.0)
        entry(r, self._current_var)
        tk.Label(r, text="mA", bg=BG2, fg=TEXT_DIM,
                 font=("Helvetica", 9)).pack(side="left", padx=2)
        setbtn(r, self._apply_current)

        # Control flags
        r = tk.Frame(inner, bg=BG2); r.pack(fill="x", pady=(8, 2))
        tk.Label(r, text="Trigger Outputs:", bg=BG2, fg=TEXT_DIM,
                 font=("Helvetica", 9), width=14,
                 anchor="w").pack(side="left")
        self._lemo_var    = tk.BooleanVar(value=False)
        self._fiber1_var  = tk.BooleanVar(value=True)
        self._fiber2_var  = tk.BooleanVar(value=True)
        self._lemo_in_var     = tk.BooleanVar(value=False)
        self._enabled_var = tk.BooleanVar(value=True)
        for lbl, var in [("LEMO", self._lemo_var),
                         ("Fiber 1", self._fiber1_var),
                         ("Fiber 2", self._fiber2_var),
                         ("Ext Trig In", self._lemo_in_var),
                         ("Enabled", self._enabled_var)]:
            tk.Checkbutton(r, text=lbl, variable=var, bg=BG2, fg=TEXT,
                           selectcolor=BG,
                           font=("Helvetica", 8)).pack(side="left", padx=4)
        btn(r, text="Set Control", command=self._apply_control,
                  bg=ACCENT, fg="white",
                  font=("Helvetica", 8), padx=6).pack(side="left", padx=(6, 0))

    def _build_monitoring_panel(self, parent):
        sec = self._section(parent, "Live Monitoring")
        sec.pack(fill="both", expand=True)
        inner = tk.Frame(sec, bg=BG2, padx=6, pady=6)
        inner.pack(fill="both", expand=True)

        br = tk.Frame(inner, bg=BG2)
        br.pack(fill="x", pady=(0, 4))
        self._mon_ind = Indicator(br, size=12, bg=BG2)
        self._mon_ind.pack(side="left", padx=(0, 6))
        for lbl, cmd, col in [
            ("Subscribe (live)", self._start_monitoring, ACCENT2),
            ("Unsubscribe",      self._stop_monitoring,  BG3),
            ("Read All Once",    self._read_all_once,    BG3),
        ]:
            btn(br, text=lbl, command=cmd, bg=col, fg="white", font=("Helvetica", 8), padx=6).pack(side="left", padx=(0, 4))

        tf = tk.Frame(inner, bg=BG2)
        tf.pack(fill="both", expand=True)
        sb = tk.Scrollbar(tf); sb.pack(side="right", fill="y")
        self._mon_text = tk.Text(tf, bg=BG, fg=TEXT_DIM,
                                 insertbackground=TEXT,
                                 font=("Courier", 8),
                                 wrap="none", yscrollcommand=sb.set,
                                 height=8)
        self._mon_text.pack(fill="both", expand=True)
        sb.config(command=self._mon_text.yview)
        self._mon_text.config(state="disabled")

    # -----------------------------------------------------------------------
    # Logging / status
    # -----------------------------------------------------------------------
    def _log(self, msg: str, color=None):
        self._mon_text.config(state="normal")
        ts  = time.strftime("%H:%M:%S")
        tag = None
        if color:
            tag = f"c_{color.replace('#','')}"
            self._mon_text.tag_configure(tag, foreground=color)
        self._mon_text.insert("end", f"[{ts}] {msg}\n", tag or "")
        self._mon_text.see("end")
        self._mon_text.config(state="disabled")

    def _set_status(self, msg: str):
        self._status_var.set(msg)

    # -----------------------------------------------------------------------
    # Connection
    # -----------------------------------------------------------------------
    def _toggle_connect(self):
        if self._connected:
            self._do_disconnect()
        else:
            self._do_connect()

    def _do_connect(self):
        endpoint = self._ep_var.get().strip()
        self._set_status(f"Connecting to {endpoint} …")
        self._conn_btn.config(state="disabled")

        if not ASYNCUA_AVAILABLE:
            self._on_connect_failed(
                RuntimeError("asyncua not installed — running in demo mode"))
            return

        client = CalibrationBoxClient(endpoint, self._username, self._password)
        client.on_data_change = self._on_data_change

        def ok(_):
            self._client = client
            self._on_connected()

        def err(exc):
            self._on_connect_failed(exc)

        self._dispatch(client.connect(), on_done=ok, on_error=err)

    def _on_connected(self):
        self._connected = True
        self._conn_ind.set_state(True)
        self._conn_btn.config(state="normal", text="Disconnect", bg=RED)
        ep = self._ep_var.get()
        self._set_status(f"Connected — {ep}")
        self._log(f"Connected to {ep}", ACCENT2)
        # Read all variables immediately so the UI reflects current state
        # before the first subscription notification or poll arrives
        def on_initial_read(data):
            self._apply_monitoring_data(data)
        self._dispatch(self._client.read_all(), on_done=on_initial_read)
        self._schedule_poll()
        # Auto-subscribe to live data changes
        self._start_monitoring()

    def _on_connect_failed(self, exc):
        self._conn_ind.set_state(False)
        self._conn_btn.config(state="normal", text="Connect", bg=ACCENT)
        msg = f"Connection failed: {exc}"
        self._set_status(msg)
        self._log(msg, RED)

    def _do_disconnect(self):
        self._cancel_poll()
        self._connected = False
        self._conn_ind.set_state(False)
        self._mon_ind.set_state(False)
        self._led_array._initialised = False
        self._conn_btn.config(text="Connect", bg=ACCENT)
        self._set_status("Disconnecting…")
        if self._client:
            def done(_):
                self.after(0, self._set_status, "Disconnected")
                self.after(0, self._log, "Disconnected", TEXT_DIM)
            self._dispatch(self._client.disconnect(), on_done=done)
            self._client = None
        else:
            self._set_status("Disconnected")

    # -----------------------------------------------------------------------
    # Periodic poll
    # -----------------------------------------------------------------------
    def _schedule_poll(self, interval_ms=5000):
        self._cancel_poll()
        self._poll_job = self.after(interval_ms, self._do_poll)

    def _cancel_poll(self):
        if self._poll_job:
            self.after_cancel(self._poll_job)
            self._poll_job = None

    def _do_poll(self):
        if not self._connected or not self._client:
            return
        def ok(data):
            self._apply_monitoring_data(data)
            self._schedule_poll()
        def err(_):
            self._schedule_poll()
        self._dispatch(self._client.read_all(), on_done=ok, on_error=err)

    # -----------------------------------------------------------------------
    # Data change callback — called from the asyncua loop thread
    # -----------------------------------------------------------------------
    def _on_data_change(self, name: str, value):
        self.after(0, self._update_var, name, value)
        self.after(0, self._log, f"{name} = {value}")

    def _apply_monitoring_data(self, data: dict):
        for name, value in data.items():
            self._update_var(name, value)

    # Map cls_state int to (label, colour)
    _STATE_MAP = {0: ("Disabled", TEXT_DIM), 1: ("Enabled", ACCENT2),
                  2: ("Fault",    RED),       3: ("Offline", YELLOW)}

    def _update_var(self, name: str, value):
        n = name.lower()
        try:
            if n == "temperature":
                self._temp_var.set(f"{float(value):.2f}")
            elif n == "voltage_actual":
                self._gauge.set_value(float(value))
            elif n == "faults":
                faults = int(value)
                self._uv_ind.set_state(bool(faults & 0x01))  # D0 under-voltage
                self._ov_ind.set_state(bool(faults & 0x02))  # D1 over-voltage
            elif n == "light_pulse":
                enabled = bool(value)
                self._pulse_ind.set_state(enabled)
                self._led_array.set_enabled(enabled)
            elif n == "led_mask":
                self._led_array.set_device_mask(int(value))
            elif n == "cls_state":
                label, col = self._STATE_MAP.get(int(value), ("Unknown", TEXT_DIM))
                if self._info_state_var:
                    self._info_state_var.set(label)
                if hasattr(self, "_state_label"):
                    self._state_label.config(fg=col)
            elif n == "host" and self._info_host_var:
                self._info_host_var.set(str(value))
            elif n == "port" and self._info_port_var:
                self._info_port_var.set(str(value))
            elif n == "dialect" and self._info_dialect_var:
                self._info_dialect_var.set(str(value))
        except Exception:
            pass

    # -----------------------------------------------------------------------
    # LED controls
    # -----------------------------------------------------------------------
    def _on_led_mask_change(self, mask: int):
        self._led_mask_var.set(mask)

    def _mask_entry_changed(self, event=None):
        try:
            self._led_array.set_mask(int(self._led_mask_var.get()) & 0x1FFF)
        except Exception:
            pass

    def _set_mask(self, mask: int):
        self._led_mask_var.set(mask)
        self._led_array.set_mask(mask)
        self._apply_leds()

    def _apply_leds(self):
        mask = self._led_array.get_mask()
        self._run_method("SetLeds", mask, success_msg=f"SetLeds({mask}) OK")

    # -----------------------------------------------------------------------
    # Parameter setters
    # -----------------------------------------------------------------------
    def _update_pw_label(self, *_):
        try:
            self._pw_result.config(
                text=f"{int(self._width_var.get()) * 62.5:.0f} ns")
        except Exception:
            pass

    def _apply_voltage(self):
        self._run_method("SetVoltage", self._volt_set_var.get(),
                         success_msg=f"SetVoltage({self._volt_set_var.get()}) OK")

    def _apply_frequency(self):
        d, r = self._freq_div_var.get(), self._freq_dvr_var.get()
        self._run_method("SetFrequency", d, r,
                         success_msg=f"SetFrequency({d}/{r}) OK")

    def _apply_width(self):
        w = self._width_var.get()
        self._run_method("SetWidth", w, success_msg=f"SetWidth({w}) OK")

    def _apply_duration(self):
        d = self._duration_var.get()
        self._run_method("SetDuration", d, success_msg=f"SetDuration({d}) OK")

    def _apply_current(self):
        c = self._current_var.get()
        self._run_method("SetCurrent", c, success_msg=f"SetCurrent({c}) OK")

    def _apply_control(self):
        flags = [self._lemo_var.get(), self._fiber1_var.get(),
                 self._fiber2_var.get(), self._lemo_in_var.get(),
                 self._enabled_var.get()]
        self._run_method("SetControl", *flags, success_msg="SetControl OK")

    # -----------------------------------------------------------------------
    # Full Configure dialog
    # -----------------------------------------------------------------------
    def _cmd_configure(self):
        dlg = tk.Toplevel(self)
        dlg.title("Full Configure")
        dlg.configure(bg=BG)
        dlg.resizable(False, False)

        fields = [
            ("LED Mask (0–8191)",    self._led_array.get_mask(),       int),
            ("Voltage Set (V)",      self._volt_set_var.get(),         float),
            ("Duration (μs)",        self._duration_var.get(),         int),
            ("Freq Dividend (Hz)",   self._freq_div_var.get(),         float),
            ("Freq Divider",         self._freq_dvr_var.get(),         int),
            ("Pulse Width",          self._width_var.get(),            int),
            ("Light Pulse (0/1)",    int(self._enabled_var.get()),     int),
            ("LEMO Out (0/1)",       int(self._lemo_var.get()),        int),
            ("Fiber 1 Out (0/1)",    int(self._fiber1_var.get()),      int),
            ("Fiber 2 Out (0/1)",    int(self._fiber2_var.get()),      int),
            ("Ext Trig In (0/1)",    int(self._lemo_in_var.get()),     int),
            ("Central Current (mA)", self._current_var.get(),         float),
        ]
        entries = []
        for i, (lbl, default, _) in enumerate(fields):
            tk.Label(dlg, text=lbl, bg=BG, fg=TEXT_DIM,
                     font=("Helvetica", 9), anchor="w",
                     width=22).grid(row=i, column=0, padx=10, pady=3, sticky="w")
            var = tk.StringVar(value=str(default))
            tk.Entry(dlg, textvariable=var, width=12, bg=BG2, fg=TEXT,
                     insertbackground=TEXT,
                     font=("Helvetica", 9)).grid(row=i, column=1,
                                                 padx=10, pady=3)
            entries.append((var, fields[i][2]))

        def do_configure():
            try:
                args = [cast(var.get()) for var, cast in entries]
                for i in range(6, 11):
                    args[i] = bool(args[i])
                if not self._connected:
                    self._log(f"[demo] Configure({args})", TEXT_DIM)
                    dlg.destroy(); return

                def ok(_):
                    self._set_status("Configure OK")
                    self._log("Configure OK", ACCENT2)
                def err(exc):
                    self._log(f"Configure error: {exc}", RED)

                self._dispatch(
                    self._client.call_method("Configure", *args),
                    on_done=ok, on_error=err)
                dlg.destroy()
            except Exception as exc:
                messagebox.showerror("Configure error", str(exc), parent=dlg)

        br = tk.Frame(dlg, bg=BG)
        br.grid(row=len(fields), column=0, columnspan=2, pady=10)
        btn(br, text="Apply", command=do_configure,
                  bg=ACCENT, fg="white",
                  font=("Helvetica", 9), padx=16).pack(side="left", padx=8)
        btn(br, text="Cancel", command=dlg.destroy,
                  bg=BG3, fg=TEXT,
                  font=("Helvetica", 9), padx=16).pack(side="left")

    # -----------------------------------------------------------------------
    # Device commands
    # -----------------------------------------------------------------------
    def _cmd_start(self):
        self._run_method("Start",
                         success_msg="Start OK — light pulses enabled")
        if not self._connected:
            self._pulse_ind.set_state(True)

    def _cmd_stop(self):
        self._run_method("Stop",
                         success_msg="Stop OK — light pulses disabled")
        if not self._connected:
            self._pulse_ind.set_state(False)

    def _cmd_getstatus(self):
        self._run_method("GetStatus", success_msg="GetStatus OK")
        self._read_all_once()

    def _cmd_reconnect(self):
        self._run_method("Reconnect", success_msg="Reconnect OK")

    def _cmd_reboot(self):
        if not messagebox.askyesno(
                "Reboot", "Reboot the calibration box?", parent=self):
            return
        self._run_method("Reboot", success_msg="Reboot command sent")

    # -----------------------------------------------------------------------
    # Monitoring
    # -----------------------------------------------------------------------
    def _start_monitoring(self):
        if not self._connected:
            self._log("Not connected", RED); return

        def ok(_):
            self._mon_ind.set_state(True)
            self._log("Live subscription active", ACCENT2)
        def err(exc):
            self._log(f"Subscription error: {exc}", RED)

        self._dispatch(self._client.start_subscription(),
                       on_done=ok, on_error=err)

    def _stop_monitoring(self):
        if not self._connected: return

        def ok(_):
            self._mon_ind.set_state(False)
            self._log("Subscription stopped", TEXT_DIM)
        def err(exc):
            self._log(f"Unsubscribe error: {exc}", RED)

        self._dispatch(self._client._stop_subscription(),
                       on_done=ok, on_error=err)

    def _read_all_once(self):
        if not self._connected:
            self._log("[demo] read_all()", TEXT_DIM); return

        def ok(data):
            self._apply_monitoring_data(data)
            for k, v in data.items():
                self._log(f"{k} = {v}")
        def err(exc):
            self._log(f"Read error: {exc}", RED)

        self._dispatch(self._client.read_all(), on_done=ok, on_error=err)

    # -----------------------------------------------------------------------
    def _on_close(self):
        self._cancel_poll()
        if self._client:
            # Best-effort async disconnect; don't wait
            self._async.submit(self._client.disconnect())
        self._async.stop()
        self.destroy()


# ===========================================================================
# Entry point
# ===========================================================================
def parse_args():
    p = argparse.ArgumentParser(
        description="NectarCAM CalibrationBox OPC UA GUI (asyncua)"
    )
    p.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    p.add_argument("--user",     default=None)
    p.add_argument("--password", default=None)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    app  = CalibBoxGUI(args.endpoint, args.user, args.password)
    app.mainloop()
