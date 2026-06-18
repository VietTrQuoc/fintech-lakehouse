"""Render a structured Vietnamese progress report (report_data.json) to a multi-page PDF.

Uses matplotlib only (DejaVu Sans supports Vietnamese diacritics) so it runs offline
with the libs already in .venv. A small flow-layout engine paginates paragraphs,
bullet lists, sub-headings and tables, repeating table headers across page breaks.
"""
import json
import re
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

plt.rcParams["font.family"] = "DejaVu Sans"

# ---- page geometry (A4 portrait, fractions of the page) -------------------
PAGE_W_IN, PAGE_H_IN = 8.27, 11.69
DPI = 100
PT_PER_PAGE = PAGE_H_IN * 72.0  # points down the page

LEFT, RIGHT = 0.075, 0.925
TOP, BOTTOM = 0.935, 0.06
CONTENT_W = RIGHT - LEFT

INK = "#1a1a1a"
ACCENT = "#0b4f6c"
ACCENT2 = "#1b7a8c"
MUTED = "#666666"
RULE = "#cfd8dc"
HEAD_BG = "#0b4f6c"
ZEBRA = "#eef3f5"


def pt2frac(pt):
    return pt / PT_PER_PAGE


# dedicated hidden figure used only to measure text widths
_mfig = plt.figure(figsize=(PAGE_W_IN, PAGE_H_IN), dpi=DPI)
_mfig.canvas.draw()
_mrend = _mfig.canvas.get_renderer()


def text_w(s, fs, weight="normal"):
    s = _sub(s)
    if s == "":
        return 0.0
    t = _mfig.text(0, 0, s, fontsize=fs, fontweight=weight)
    bb = t.get_window_extent(renderer=_mrend)
    t.remove()
    return bb.width / _mfig.bbox.width


# glyphs absent from DejaVu Sans -> supported substitutes
_SUBST = {"✅": "✓", "⏳": "○", "✔": "✓", "❌": "✗"}


def _sub(s):
    s = str(s)
    for k, v in _SUBST.items():
        s = s.replace(k, v)
    return s


def wrap(s, fs, weight, max_w):
    s = _sub(s).replace("\n", " ").strip()
    if not s:
        return [""]
    words = s.split(" ")
    lines, cur = [], ""
    for w in words:
        trial = w if not cur else cur + " " + w
        if text_w(trial, fs, weight) <= max_w:
            cur = trial
            continue
        if cur:
            lines.append(cur)
        if text_w(w, fs, weight) > max_w:  # token longer than a line -> hard break
            piece = ""
            for ch in w:
                if text_w(piece + ch, fs, weight) <= max_w or not piece:
                    piece += ch
                else:
                    lines.append(piece)
                    piece = ch
            cur = piece
        else:
            cur = w
    if cur:
        lines.append(cur)
    return lines or [""]


class Report:
    def __init__(self, title, subtitle):
        self.title = title
        self.subtitle = subtitle
        self.pdf = None
        self.fig = None
        self.ax = None
        self.y = TOP
        self.page = 0
        self.sec_no = 0

    # --- page lifecycle ---
    def open(self, path):
        self.pdf = PdfPages(path)

    def close(self):
        if self.fig is not None:
            self._finish_page()
        self.pdf.close()

    def _new_page(self):
        if self.fig is not None:
            self._finish_page()
        self.page += 1
        self.fig = plt.figure(figsize=(PAGE_W_IN, PAGE_H_IN), dpi=DPI)
        self.ax = self.fig.add_axes([0, 0, 1, 1])
        self.ax.axis("off")
        self.ax.set_xlim(0, 1)
        self.ax.set_ylim(0, 1)
        # running header
        self.ax.text(LEFT, 0.972, "Financial Transaction Data Lakehouse",
                     fontsize=7.5, color=MUTED, va="center")
        self.ax.text(RIGHT, 0.972, "Báo cáo tiến độ", fontsize=7.5, color=MUTED,
                     va="center", ha="right")
        self.ax.plot([LEFT, RIGHT], [0.963, 0.963], color=RULE, lw=0.6)
        self.y = TOP

    def _finish_page(self):
        self.ax.text(0.5, 0.032, f"— {self.page} —", fontsize=8, color=MUTED,
                     ha="center", va="center")
        self.pdf.savefig(self.fig)
        plt.close(self.fig)

    def _ensure(self, h):
        if self.fig is None or self.y - h < BOTTOM:
            self._new_page()

    # --- primitives ---
    def gap(self, pt):
        self.y -= pt2frac(pt)

    def _draw_lines(self, lines, fs, weight, x, color, gap=1.42):
        lh = pt2frac(fs * gap)
        for ln in lines:
            self._ensure(lh)
            self.ax.text(x, self.y, ln, fontsize=fs, fontweight=weight,
                         color=color, va="top")
            self.y -= lh

    # --- cover ---
    def cover(self):
        self._new_page()
        self.y = 0.74
        self.ax.add_patch(plt.Rectangle((LEFT, self.y - 0.004), CONTENT_W, 0.006,
                                        color=ACCENT, transform=self.ax.transData))
        self.y -= 0.03
        for ln in wrap(self.title, 24, "bold", CONTENT_W):
            self.ax.text(LEFT, self.y, ln, fontsize=24, fontweight="bold",
                         color=ACCENT, va="top")
            self.y -= pt2frac(24 * 1.3)
        self.gap(8)
        for ln in wrap(self.subtitle, 12, "normal", CONTENT_W):
            self.ax.text(LEFT, self.y, ln, fontsize=12, color=MUTED, va="top")
            self.y -= pt2frac(12 * 1.4)
        self.gap(20)
        self.ax.plot([LEFT, RIGHT], [self.y, self.y], color=RULE, lw=0.8)

    # --- section / heading ---
    def section(self, sid, heading):
        self._ensure(pt2frac(46))
        self.gap(14)
        self.sec_no += 1
        # strip any leading "N." already in the heading to avoid double numbering
        heading = re.sub(r"^\s*\d+[\.\)]\s*", "", str(heading))
        chip = str(self.sec_no)
        self.ax.text(LEFT + 0.012, self.y - pt2frac(7), chip, fontsize=15,
                     fontweight="bold", color="white", va="center", ha="center",
                     bbox=dict(boxstyle="circle,pad=0.34", fc=ACCENT, ec="none"))
        self._draw_lines(wrap(heading, 15, "bold", CONTENT_W - 0.06), 15, "bold",
                         LEFT + 0.045, ACCENT)
        self.gap(2)
        self.ax.plot([LEFT, RIGHT], [self.y, self.y], color=ACCENT2, lw=1.0)
        self.gap(8)

    def subheading(self, text):
        self._ensure(pt2frac(22))
        self.gap(6)
        self._draw_lines(wrap(text, 11.5, "bold", CONTENT_W), 11.5, "bold", LEFT, ACCENT2)
        self.gap(2)

    def para(self, text):
        self._draw_lines(wrap(text, 10, "normal", CONTENT_W), 10, "normal", LEFT, INK)
        self.gap(4)

    def bullets(self, items):
        for it in items:
            lines = wrap(it, 10, "normal", CONTENT_W - 0.022)
            lh = pt2frac(10 * 1.42)
            self._ensure(lh)
            self.ax.text(LEFT + 0.004, self.y, "•", fontsize=10, color=ACCENT2, va="top")
            for i, ln in enumerate(lines):
                self._ensure(lh)
                self.ax.text(LEFT + 0.022, self.y, ln, fontsize=10, color=INK, va="top")
                self.y -= lh
        self.gap(4)

    # --- table ---
    def table(self, columns, rows, fs=8.5):
        ncol = len(columns)
        pad = 0.008
        # column weights from max content width (capped) -> proportional fill
        weights = []
        for c in range(ncol):
            w = text_w(str(columns[c]), fs, "bold")
            for r in rows:
                if c < len(r):
                    w = max(w, min(text_w(str(r[c]), fs, "normal"), 0.30))
            weights.append(max(w, 0.04))
        tot = sum(weights)
        col_w = [CONTENT_W * wv / tot for wv in weights]
        xs = [LEFT]
        for w in col_w[:-1]:
            xs.append(xs[-1] + w)

        lh = pt2frac(fs * 1.4)

        def row_height(cells, weight):
            mx = 1
            for c in range(ncol):
                cell = str(cells[c]) if c < len(cells) else ""
                mx = max(mx, len(wrap(cell, fs, weight, col_w[c] - 2 * pad)))
            return mx * lh + pt2frac(6)

        def draw_header():
            h = row_height(columns, "bold")
            self._ensure(h)
            self.ax.add_patch(plt.Rectangle((LEFT, self.y - h), CONTENT_W, h,
                                            color=HEAD_BG, ec="none"))
            top = self.y
            for c in range(ncol):
                lines = wrap(str(columns[c]), fs, "bold", col_w[c] - 2 * pad)
                yy = top - pt2frac(3)
                for ln in lines:
                    self.ax.text(xs[c] + pad, yy, ln, fontsize=fs, fontweight="bold",
                                 color="white", va="top")
                    yy -= lh
            self.y -= h
            return

        draw_header()
        zebra = False
        for cells in rows:
            h = row_height(cells, "normal")
            if self.y - h < BOTTOM:  # new page -> repeat header
                self._new_page()
                draw_header()
            top = self.y
            if zebra:
                self.ax.add_patch(plt.Rectangle((LEFT, top - h), CONTENT_W, h,
                                                color=ZEBRA, ec="none"))
            for c in range(ncol):
                cell = str(cells[c]) if c < len(cells) else ""
                lines = wrap(cell, fs, "normal", col_w[c] - 2 * pad)
                yy = top - pt2frac(3)
                for ln in lines:
                    self.ax.text(xs[c] + pad, yy, ln, fontsize=fs, color=INK, va="top")
                    yy -= lh
            self.ax.plot([LEFT, RIGHT], [top - h, top - h], color=RULE, lw=0.4)
            self.y -= h
            zebra = not zebra
        self.gap(8)


def render(data, out_path):
    rep = Report(data["title"], data.get("subtitle", ""))
    rep.open(out_path)
    rep.cover()
    for sec in data["sections"]:
        rep.section(sec.get("id", ""), sec["heading"])
        for blk in sec["blocks"]:
            kind = blk.get("kind")
            if kind == "para":
                rep.para(blk.get("text", ""))
            elif kind == "subheading":
                rep.subheading(blk.get("text", ""))
            elif kind == "bullets":
                rep.bullets(blk.get("items", []))
            elif kind == "table":
                rep.table(blk.get("columns", []), blk.get("rows", []))
    rep.close()


if __name__ == "__main__":
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("report_data.json")
    out = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("BaoCao_TienDo_DataLakehouse.pdf")
    data = json.loads(src.read_text(encoding="utf-8"))
    render(data, str(out))
    print(f"PDF written: {out.resolve()}  ({out.stat().st_size:,} bytes)")
