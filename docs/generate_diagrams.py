#!/usr/bin/env python3
"""Render Visual walkthrough SVGs (t0..t10) on a fixed grid.

Every diagram shares the same box coordinates so the JobManager, TM1,
TM2, TM3, and Output boxes line up when scrolling between panes in
pr_description.md. Content inside the boxes changes per timestep; the
layout does not.

Regenerate:
    python3 docs/generate_diagrams.py
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional
from xml.sax.saxutils import escape

# ---------------------------------------------------------------------------
# Grid constants — change these in one place to rescale every pane.
# ---------------------------------------------------------------------------

CANVAS_W = 1120
CANVAS_H = 640

# JobManager box
JM_X, JM_Y, JM_W, JM_H = 30, 55, 1060, 115

# TM row
TM_Y, TM_H = 280, 170
TM_W = 340
TM1_X = 30
TM2_X = 390
TM3_X = 750

# Output box
OUT_X, OUT_Y, OUT_W, OUT_H = 30, 540, 1060, 90

# Zones between rows (used for arrows)
UPPER_ARROW_TOP = JM_Y + JM_H
UPPER_ARROW_BOTTOM = TM_Y
LOWER_ARROW_TOP = TM_Y + TM_H
LOWER_ARROW_BOTTOM = OUT_Y

# Stylesheet — rendered inside each SVG. GitHub serves SVGs referenced by
# `![](…)` standalone, so `prefers-color-scheme` inside the SVG works.
STYLE = """
<style>
  .box        { fill: #f7f7f9; stroke: #333;    stroke-width: 1.5; }
  .box-jm     { fill: #eef3fb; stroke: #333;    stroke-width: 1.5; }
  .box-out    { fill: #f3f0ea; stroke: #333;    stroke-width: 1.5; }
  .box-warn   { fill: #f7f7f9; stroke: #b23a48; stroke-width: 2; }
  .divider    { stroke: #aab;  stroke-width: 1; }
  .arrow      { stroke: #222;  stroke-width: 1.6; fill: none; }
  .arrow-dot  { stroke: #222;  stroke-width: 1.6; fill: none; stroke-dasharray: 5 3; }
  .arrow-head { fill: #222; }
  .text       { fill: #222; }
  .text-muted { fill: #555; }
  .text-accent{ fill: #b23a48; }
  .label-bg   { fill: #ffffff; stroke: none; }
  @media (prefers-color-scheme: dark) {
    .box      { fill: #1e1f24; stroke: #c8c9d0; }
    .box-jm   { fill: #20293a; stroke: #c8c9d0; }
    .box-out  { fill: #2b2620; stroke: #c8c9d0; }
    .box-warn { fill: #1e1f24; stroke: #ff7a8a; }
    .divider  { stroke: #6b6f7a; }
    .arrow,
    .arrow-dot{ stroke: #e6e6e6; }
    .arrow-head { fill: #e6e6e6; }
    .text     { fill: #e6e6e6; }
    .text-muted { fill: #a8abb3; }
    .text-accent{ fill: #ff7a8a; }
    .label-bg { fill: #1e1f24; }
  }
</style>
"""

# Lane x-centers
TM_LANES = {
    "TM1": TM1_X + TM_W // 2,
    "TM2": TM2_X + TM_W // 2,
    "TM3": TM3_X + TM_W // 2,
}


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class Arrow:
    src: str  # "JM", "TM1", "TM2", "TM3", "OUT"
    dst: str
    label: str
    dotted: bool = False


@dataclass
class TMState:
    name: str  # "TM1", "TM2", "TM3" — or a display override
    state: str
    detail: str
    warn: bool = False  # adds a subtle accent border
    extra: Optional[str] = None  # rare third detail line


@dataclass
class Step:
    ident: str  # "t5"
    title: str  # "### t5: Bug 1 first fire, Bug 2 seeds the queue"
    pool: str  # rendered as remainingSplits
    awaiting: str  # rendered as readersAwaitingQueue
    view: str  # rendered as pointOfView
    awaiting_alert: bool = False  # highlights the awaiting line
    view_alert: bool = False  # highlights a part of the view
    tms: List[TMState] = field(default_factory=list)
    output: str = ""
    output_note: Optional[str] = None  # second line, e.g. "💀 S1 missing"
    arrows: List[Arrow] = field(default_factory=list)


# ---------------------------------------------------------------------------
# SVG helpers
# ---------------------------------------------------------------------------


def svg_header() -> str:
    return f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {CANVAS_W} {CANVAS_H}" width="{CANVAS_W}" height="{CANVAS_H}" font-family="-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif">
  <defs>
    <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M 0 0 L 10 5 L 0 10 z" class="arrow-head"/>
    </marker>
  </defs>
{STYLE}
"""


def rect(x, y, w, h, cls="box", rx=6):
    return f'  <rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" class="{cls}"/>\n'


def text(x, y, content, font_size=14, bold=False, cls="text", anchor="start", mono=False):
    weight = ' font-weight="600"' if bold else ""
    family = ' font-family="ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace"' if mono else ""
    # Preserve runs of spaces for the monospace lines so padded labels stay aligned.
    preserve = ' xml:space="preserve"' if mono else ""
    return f'  <text x="{x}" y="{y}" font-size="{font_size}" class="{cls}"{weight}{family}{preserve} text-anchor="{anchor}">{escape(content)}</text>\n'


def divider(x1, y1, x2, y2):
    return f'  <line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" class="divider"/>\n'


def arrow_path(x1, y1, x2, y2, dotted=False):
    cls = "arrow-dot" if dotted else "arrow"
    return f'  <line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" class="{cls}" marker-end="url(#arrow)"/>\n'


def label_box(cx, cy, content):
    """Label overlaid on an arrow — opaque background so the arrow shows through only where
    we want it to.
    """
    # crude width estimation: ~6.5 px per char for 12px font
    width = max(40, int(len(content) * 6.8) + 12)
    x = cx - width / 2
    out = f'  <rect x="{x}" y="{cy - 10}" width="{width}" height="20" rx="3" class="label-bg"/>\n'
    out += text(cx, cy + 4, content, font_size=12, anchor="middle")
    return out


# ---------------------------------------------------------------------------
# Box drawing
# ---------------------------------------------------------------------------


def draw_jm(step: Step) -> str:
    out = rect(JM_X, JM_Y, JM_W, JM_H, cls="box-jm")
    out += text(JM_X + 16, JM_Y + 22, "JobManager", font_size=14, bold=True)
    out += divider(JM_X + 12, JM_Y + 32, JM_X + JM_W - 12, JM_Y + 32)
    # Widest label is "readersAwaitingQueue:" (21 chars); pad the others to match.
    out += text(JM_X + 20, JM_Y + 55,
                f"remainingSplits:      {step.pool}", font_size=13, mono=True)
    cls = "text-accent" if step.awaiting_alert else "text"
    out += text(JM_X + 20, JM_Y + 78,
                f"readersAwaitingQueue: {step.awaiting}", font_size=13, mono=True, cls=cls)
    cls = "text-accent" if step.view_alert else "text"
    out += text(JM_X + 20, JM_Y + 101,
                f"pointOfView:          {step.view}", font_size=13, mono=True, cls=cls)
    return out


def draw_tm(tm: TMState, x: int) -> str:
    out = rect(x, TM_Y, TM_W, TM_H, cls="box-warn" if tm.warn else "box")
    cx = x + TM_W // 2
    out += text(cx, TM_Y + 30, tm.name, font_size=17, bold=True, anchor="middle")
    out += divider(x + 16, TM_Y + 44, x + TM_W - 16, TM_Y + 44)
    out += text(cx, TM_Y + 75, tm.state, font_size=15, bold=True, anchor="middle")
    if tm.detail and tm.detail != "—":
        out += text(cx, TM_Y + 105, tm.detail, font_size=13, anchor="middle", cls="text-muted")
    if tm.extra and tm.extra != "—":
        out += text(cx, TM_Y + 130, tm.extra, font_size=13, anchor="middle", cls="text-muted")
    return out


def draw_output(step: Step) -> str:
    out = rect(OUT_X, OUT_Y, OUT_W, OUT_H, cls="box-out")
    out += text(OUT_X + 16, OUT_Y + 24, "Output", font_size=14, bold=True)
    out += divider(OUT_X + 12, OUT_Y + 34, OUT_X + OUT_W - 12, OUT_Y + 34)
    cx = OUT_X + OUT_W // 2
    if step.output:
        out += text(cx, OUT_Y + 62, step.output, font_size=15, bold=True, anchor="middle")
    if step.output_note:
        out += text(cx, OUT_Y + 85, step.output_note, font_size=13, anchor="middle", cls="text-accent")
    return out


# ---------------------------------------------------------------------------
# Arrow drawing
# ---------------------------------------------------------------------------


def endpoint(name: str, top_or_bottom: str) -> tuple:
    """Return (x, y) for an arrow endpoint.

    top_or_bottom: 'top' = edge facing away from JM; 'bottom' = edge facing away from OUT.
    """
    if name == "JM":
        # JM only has a bottom edge relevant for arrows
        return (0, JM_Y + JM_H)
    if name == "OUT":
        return (0, OUT_Y)
    if name in TM_LANES:
        lane_x = TM_LANES[name]
        if top_or_bottom == "top":
            return (lane_x, TM_Y)
        return (lane_x, TM_Y + TM_H)
    raise ValueError(name)


def draw_arrows(arrows: List[Arrow]) -> str:
    """Arrow routing.

    Upper zone (between JM and TM row):
        JM → TMi: vertical, at TMi's lane x, arrow pointing down
        TMi → JM: vertical, at TMi's lane x, arrow pointing up
    Lower zone (between TM row and Output):
        TMi → OUT / OUT → TMi: vertical at TMi's lane x

    When multiple arrows share a lane, stack them with x offsets so the
    arrowheads don't collide.
    """
    svg = ""

    # Group arrows by (zone, lane) so we can offset colliding ones.
    upper_by_lane: dict[str, List[Arrow]] = {}
    lower_by_lane: dict[str, List[Arrow]] = {}
    for a in arrows:
        if a.src == "JM" or a.dst == "JM":
            tm = a.dst if a.src == "JM" else a.src
            upper_by_lane.setdefault(tm, []).append(a)
        elif a.src == "OUT" or a.dst == "OUT":
            tm = a.dst if a.src == "OUT" else a.src
            lower_by_lane.setdefault(tm, []).append(a)

    # Upper zone
    for tm, lane_arrows in upper_by_lane.items():
        lane_x = TM_LANES[tm]
        n = len(lane_arrows)
        offsets = _offsets(n, spacing=30)
        y_slots = _y_slots(UPPER_ARROW_TOP, UPPER_ARROW_BOTTOM, n)
        for arrow, dx, y_label in zip(lane_arrows, offsets, y_slots):
            x = lane_x + dx
            y_top = UPPER_ARROW_TOP
            y_bot = UPPER_ARROW_BOTTOM
            if arrow.src == "JM":
                svg += arrow_path(x, y_top + 2, x, y_bot - 2, dotted=arrow.dotted)
            else:
                svg += arrow_path(x, y_bot - 2, x, y_top + 2, dotted=arrow.dotted)
            svg += label_box(x, y_label, arrow.label)

    # Lower zone
    for tm, lane_arrows in lower_by_lane.items():
        lane_x = TM_LANES[tm]
        n = len(lane_arrows)
        offsets = _offsets(n, spacing=30)
        y_slots = _y_slots(LOWER_ARROW_TOP, LOWER_ARROW_BOTTOM, n)
        for arrow, dx, y_label in zip(lane_arrows, offsets, y_slots):
            x = lane_x + dx
            y_top = LOWER_ARROW_TOP
            y_bot = LOWER_ARROW_BOTTOM
            if arrow.dst == "OUT":
                svg += arrow_path(x, y_top + 2, x, y_bot - 2, dotted=arrow.dotted)
            else:
                svg += arrow_path(x, y_bot - 2, x, y_top + 2, dotted=arrow.dotted)
            svg += label_box(x, y_label, arrow.label)

    return svg


def _offsets(n: int, spacing: int) -> List[int]:
    """Return n x-offsets symmetric around zero, spaced `spacing` px apart."""
    if n == 1:
        return [0]
    start = -(n - 1) * spacing / 2
    return [int(start + i * spacing) for i in range(n)]


def _y_slots(top: int, bottom: int, n: int) -> List[int]:
    """Return n y-positions distributed between top and bottom with ~28px separation."""
    if n == 1:
        return [int((top + bottom) / 2)]
    # Keep labels clear of the box edges (14px buffer each side).
    usable_top = top + 18
    usable_bottom = bottom - 18
    span = usable_bottom - usable_top
    step = span / (n - 1)
    return [int(usable_top + step * i) for i in range(n)]


# ---------------------------------------------------------------------------
# Full render
# ---------------------------------------------------------------------------


def render(step: Step) -> str:
    svg = svg_header()
    svg += text(CANVAS_W // 2, 32, step.title, font_size=17, bold=True, anchor="middle")
    svg += draw_jm(step)
    for tm, x in zip(step.tms, [TM1_X, TM2_X, TM3_X]):
        svg += draw_tm(tm, x)
    svg += draw_output(step)
    svg += draw_arrows(step.arrows)
    svg += "</svg>\n"
    return svg


# ---------------------------------------------------------------------------
# Timestep definitions — one Step per pane
# ---------------------------------------------------------------------------


def steps() -> List[Step]:
    return [
        Step(
            ident="t0",
            title="t0: Job starts, TM3 still provisioning",
            pool="[S1, S2, S3]",
            awaiting="[ ]",
            view="TM1=RUNNING | TM2=RUNNING",
            tms=[
                TMState("TM1", "REGISTERED", "(idle)"),
                TMState("TM2", "REGISTERED", "(idle)"),
                TMState("TM3", "PROVISIONING", "(not registered)"),
            ],
            output="",
        ),
        Step(
            ident="t1",
            title="t1: Requests queue up in `awaiting`",
            pool="[S1, S2, S3]",
            awaiting="[TM1, TM2]",
            view="TM1=RUNNING | TM2=RUNNING",
            tms=[
                TMState("TM1", "REGISTERED", "(idle)"),
                TMState("TM2", "REGISTERED", "(idle)"),
                TMState("TM3", "PROVISIONING", ""),
            ],
            output="",
            arrows=[
                Arrow("TM1", "JM", "1. sendSplitRequest", dotted=True),
                Arrow("TM2", "JM", "2. sendSplitRequest", dotted=True),
            ],
        ),
        Step(
            ident="t2",
            title="t2: Enumerator drains `awaiting`, assigns splits",
            pool="[S3]",
            awaiting="[ ]",
            view="TM1=RUNNING | TM2=RUNNING",
            tms=[
                TMState("TM1", "RUNNING", "S1"),
                TMState("TM2", "RUNNING", "S2"),
                TMState("TM3", "PROVISIONING", ""),
            ],
            output="",
            arrows=[
                Arrow("JM", "TM1", "1. assign S1"),
                Arrow("JM", "TM2", "2. assign S2"),
            ],
        ),
        Step(
            ident="t3",
            title="t3: TM1 (fast) finishes S1, requests queue up",
            pool="[S3]",
            awaiting="[TM1]",
            view="TM1=RUNNING | TM2=RUNNING",
            tms=[
                TMState("TM1", "RUNNING", "(idle, finished S1)"),
                TMState("TM2", "RUNNING", "S2"),
                TMState("TM3", "PROVISIONING", ""),
            ],
            output="S1",
            arrows=[
                Arrow("TM1", "OUT", "1. emit S1"),
                Arrow("TM1", "JM", "2. sendSplitRequest", dotted=True),
            ],
        ),
        Step(
            ident="t4",
            title="t4: Enumerator drains `awaiting`, assigns S3 to TM1",
            pool="[ ]",
            awaiting="[ ]",
            view="TM1=RUNNING | TM2=RUNNING",
            tms=[
                TMState("TM1", "RUNNING", "S3"),
                TMState("TM2", "RUNNING", "S2"),
                TMState("TM3", "PROVISIONING", ""),
            ],
            output="S1",
            arrows=[
                Arrow("JM", "TM1", "1. assign S3"),
            ],
        ),
        Step(
            ident="t5",
            title="t5: Bug 1 first fire, Bug 2 seeds the queue",
            pool="[ ]",
            awaiting="[TM1*]  ← Bug 2: stale",
            view="TM1=RUNNING | TM2=RUNNING",
            awaiting_alert=True,
            tms=[
                TMState("TM1", "FINISHING", "(processed NMS)"),
                TMState("TM2", "RUNNING", "S2 (noted NMS)"),
                TMState("TM3", "PROVISIONING", ""),
            ],
            output="S1, S3",
            arrows=[
                Arrow("TM1", "OUT", "1. emit S3"),
                Arrow("TM1", "JM", "2. sendSplitRequest", dotted=True),
                Arrow("JM", "TM1", "3. NoMoreSplits broadcast", dotted=True),
            ],
        ),
        Step(
            ident="t6",
            title="t6: TM3 late-registers — Bug 1 re-fires",
            pool="[ ]",
            awaiting="[TM1*, TM3]",
            view="TM1=RUNNING ⚠ | TM2=FINISHED | TM3=RUNNING",
            awaiting_alert=True,
            view_alert=True,
            tms=[
                TMState("TM1", "FINISHED on TM", "(JM: still RUNNING)", warn=True),
                TMState("TM2", "FINISHED", ""),
                TMState("TM3", "RUNNING", "(just registered)"),
            ],
            output="S1, S2, S3",
            arrows=[
                Arrow("TM2", "OUT", "1. emit S2"),
                Arrow("TM3", "JM", "2. register + sendSplitRequest", dotted=True),
                Arrow("JM", "TM1", "3. NoMoreSplits re-broadcast", dotted=True),
                Arrow("JM", "TM3", "3. NoMoreSplits re-broadcast", dotted=True),
            ],
        ),
        Step(
            ident="t7",
            title="t7: Delivery to TM1 fails — task failover",
            pool="[S1, S3]",
            awaiting="[TM1*, TM3]",
            view="TM1=FAILED | TM2=FINISHED | TM3=RUNNING",
            awaiting_alert=True,
            tms=[
                TMState("TM1", "FAILED", "outputs DISCARDED", warn=True),
                TMState("TM2", "FINISHED", ""),
                TMState("TM3", "RUNNING", "(idle)"),
            ],
            output="S2",
            arrows=[
                Arrow("TM1", "JM", "1. TaskNotRunningException → FAILED", dotted=True),
                Arrow("TM1", "OUT", "2. discard (S1, S3)", dotted=True),
                Arrow("TM1", "JM", "3. addSplitsBack(S1, S3)", dotted=True),
            ],
        ),
        Step(
            ident="t8",
            title="t8: Queue filled — assignSplits re-invoked",
            pool="[S1, S3]",
            awaiting="[TM1*, TM3]",
            view="TM1=FAILED | TM2=FINISHED | TM3=RUNNING",
            awaiting_alert=True,
            tms=[
                TMState("TM1", "FAILED", "(stale in queue)", warn=True),
                TMState("TM2", "FINISHED", ""),
                TMState("TM3", "RUNNING", "(idle)"),
            ],
            output="S2",
        ),
        Step(
            ident="t9",
            title="t9: Queue drains — Bug 2 bites, S1 orphaned",
            pool="[ ]",
            awaiting="[ ]",
            view="TM1=FAILED | TM2=FINISHED | TM3=RUNNING",
            tms=[
                TMState("TM1", "FAILED", "💀 S1 orphaned", warn=True),
                TMState("TM2", "FINISHED", ""),
                TMState("TM3", "RUNNING", "S3"),
            ],
            output="S2",
            arrows=[
                Arrow("JM", "TM1", "1. assign S1 (absorbed — ORPHANED)"),
                Arrow("JM", "TM3", "2. assign S3"),
            ],
        ),
        Step(
            ident="t10",
            title="t10: Final state — data loss",
            pool="[ ]",
            awaiting="[ ]",
            view="TM1=FINISHED | TM2=FINISHED | TM3=FINISHED",
            tms=[
                TMState("TM1 attempt_1", "FINISHED", "(no splits)"),
                TMState("TM2", "FINISHED", "S2 ✓"),
                TMState("TM3", "FINISHED", "S3 ✓"),
            ],
            output="S2, S3",
            output_note="💀 S1 missing",
            arrows=[
                Arrow("TM3", "OUT", "1. emit S3"),
                Arrow("TM1", "JM", "2. sendSplitRequest", dotted=True),
                Arrow("JM", "TM1", "3. NoMoreSplits", dotted=True),
            ],
        ),
    ]


def main() -> None:
    out_dir = Path(__file__).parent / "svg"
    out_dir.mkdir(parents=True, exist_ok=True)
    for s in steps():
        path = out_dir / f"{s.ident}.svg"
        path.write_text(render(s))
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
