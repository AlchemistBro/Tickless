"""Общие средства схем: измерение текста Arial, фигуры C4 в оформлении draw.io и запись .drawio.svg.

Файл .drawio.svg — обычный SVG для README и одновременно модель draw.io в атрибуте content:
его можно открыть в draw.io и поправить вручную. Источник истины — описания схем в c4.py и sequence.py.
"""

import html
import json
import math
import pathlib

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[1]
FONT = "Arial, Helvetica, sans-serif"

_widths = json.loads((HERE / "arial-widths.json").read_text(encoding="utf-8"))
_index = {c: i for i, c in enumerate(_widths["chars"])}

PALETTE = {
    "person": ("#08427B", "#06315C"),
    "person-ext": ("#8B8496", "#6E6879"),
    "system": ("#1168BD", "#0B4884"),
    "container": ("#23A2D9", "#0E7DAD"),
    "external": ("#8B8496", "#6E6879"),
}
EDGE = "#707070"
LABEL = "#404040"


def measure(text, size, bold=False):
    """Ширина строки в пикселях по метрикам Arial; неизвестный символ считается как «М»."""
    table = _widths["bold" if bold else "regular"]
    fallback = table[_index["М"]]
    return sum(table[_index[c]] if c in _index else fallback for c in text) * size / 1000


def wrap(text, size, width, bold=False):
    """Жадный перенос по словам; явный \\n начинает новую строку."""
    lines = []
    for paragraph in text.split("\n"):
        line = ""
        for word in paragraph.split(" "):
            candidate = f"{line} {word}".strip()
            if line and measure(candidate, size, bold) > width:
                lines.append(line)
                line = word
            else:
                line = candidate
        lines.append(line)
    return lines


def esc(text):
    return html.escape(text, quote=True)


def text_block(x, y, rows, anchor="middle"):
    """rows: [(текст, размер, жирный, цвет, прозрачность)], y — верх блока. Возвращает SVG и высоту."""
    out, cursor = [], y
    for content, size, bold, color, opacity in rows:
        cursor += size * 1.22
        if content:
            weight = ' font-weight="bold"' if bold else ""
            fade = f' fill-opacity="{opacity}"' if opacity < 1 else ""
            out.append(f'<text x="{x:.1f}" y="{cursor - size * 0.28:.1f}" font-size="{size}"{weight} fill="{color}"{fade} text-anchor="{anchor}">{esc(content)}</text>')
    return "".join(out), cursor - y


class Canvas:
    """Холст схемы: фигуры, границы, связи, легенда; выдаёт SVG и модель draw.io."""

    def __init__(self, name, width, height):
        self.name, self.width, self.height = name, width, height
        self.back, self.middle, self.front, self.top = [], [], [], []
        self.cells = []
        self.shapes = {}
        self.serial = 0

    # ---------- модель draw.io ----------

    def _id(self):
        self.serial += 1
        return f"c{self.serial}"

    def _vertex(self, ident, value, style, x, y, w, h):
        self.cells.append(f'<mxCell id="{ident}" value="{esc(value)}" style="{style}" vertex="1" parent="1"><mxGeometry x="{x:.0f}" y="{y:.0f}" width="{w:.0f}" height="{h:.0f}" as="geometry"/></mxCell>')

    @staticmethod
    def _html(name, tech, desc, size):
        parts = [f'<b><font style="font-size:{size}px">{esc(name)}</font></b>']
        if tech:
            parts.append(f'<div style="font-size:13px">[{esc(tech)}]</div>')
        if desc:
            parts.append(f'<br><div style="font-size:12px">{esc(desc)}</div>')
        return "".join(parts)

    # ---------- фигуры ----------

    def _rows(self, name, tech, desc, w, size, color="#FFFFFF"):
        rows = [(line, size, True, color, 1) for line in wrap(name, size, w - 24, True)]
        if tech:
            rows += [(line, 13, False, color, 0.9) for line in wrap(f"[{tech}]", 13, w - 24)]
        if desc:
            rows.append(("", 5, False, color, 1))
            rows += [(line, 12.5, False, color, 0.85) for line in wrap(desc, 12.5, w - 24)]
        return rows

    def _centered(self, x, y, w, h, rows, layer):
        _, height = text_block(0, 0, rows)
        body, _ = text_block(x + w / 2, y + (h - height) / 2, rows)
        layer.append(body)

    def box(self, ident, x, y, w, h, name, tech="", desc="", kind="container", size=17):
        fill, stroke = PALETTE[kind]
        self.shapes[ident] = (x, y, w, h)
        self.front.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>')
        self._centered(x, y, w, h, self._rows(name, tech, desc, w, size), self.front)
        self._vertex(ident, self._html(name, tech, desc, size), f"rounded=1;arcSize=10;whiteSpace=wrap;html=1;labelBackgroundColor=none;fillColor={fill};strokeColor={stroke};fontColor=#FFFFFF;fontSize=12;", x, y, w, h)

    def person(self, ident, x, y, w, h, name, tech="Пользователь", desc="", kind="person", size=17):
        fill, stroke = PALETTE[kind]
        r = w * 0.17
        top = y + r * 1.55
        self.shapes[ident] = (x, top, w, y + h - top)
        self.front.append(f'<rect x="{x}" y="{top:.1f}" width="{w}" height="{y + h - top:.1f}" rx="{min(42, (y + h - top) / 2.6):.1f}" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>')
        self.front.append(f'<circle cx="{x + w / 2:.1f}" cy="{y + r:.1f}" r="{r:.1f}" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>')
        self._centered(x, top + 6, w, y + h - top - 6, self._rows(name, tech, desc, w, size), self.front)
        self._vertex(ident, self._html(name, tech, desc, size), f"shape=mxgraph.c4.person2;whiteSpace=wrap;html=1;align=center;labelBackgroundColor=none;fillColor={fill};strokeColor={stroke};fontColor=#FFFFFF;fontSize=12;", x, y, w, h)

    def database(self, ident, x, y, w, h, name, tech="", desc="", kind="container", size=17):
        fill, stroke = PALETTE[kind]
        ry = 12
        self.shapes[ident] = (x, y, w, h)
        self.front.append(f'<path d="M{x},{y + ry} a{w / 2},{ry} 0 0 0 {w},0 v{h - 2 * ry} a{w / 2},{ry} 0 0 1 {-w},0 z" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>')
        self.front.append(f'<ellipse cx="{x + w / 2}" cy="{y + ry}" rx="{w / 2}" ry="{ry}" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>')
        self._centered(x, y + 2 * ry, w, h - 2 * ry, self._rows(name, tech, desc, w, size), self.front)
        self._vertex(ident, self._html(name, tech, desc, size), f"shape=cylinder3;size=12;boundedLbl=1;whiteSpace=wrap;html=1;labelBackgroundColor=none;fillColor={fill};strokeColor={stroke};fontColor=#FFFFFF;fontSize=12;", x, y, w, h)

    def queue(self, ident, x, y, w, h, name, tech="", desc="", kind="container", size=17):
        fill, stroke = PALETTE[kind]
        rx = 14
        self.shapes[ident] = (x, y, w, h)
        self.front.append(f'<path d="M{x + rx},{y} h{w - 2 * rx} a{rx},{h / 2} 0 0 1 0,{h} h{-(w - 2 * rx)} a{rx},{h / 2} 0 0 1 0,{-h} z" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>')
        self.front.append(f'<ellipse cx="{x + w - rx}" cy="{y + h / 2}" rx="{rx}" ry="{h / 2}" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>')
        self._centered(x, y, w - 2 * rx, h, self._rows(name, tech, desc, w - 2 * rx, size), self.front)
        self._vertex(ident, self._html(name, tech, desc, size), f"shape=cylinder3;direction=south;size=14;boundedLbl=1;whiteSpace=wrap;html=1;labelBackgroundColor=none;fillColor={fill};strokeColor={stroke};fontColor=#FFFFFF;fontSize=12;", x, y, w, h)

    def boundary(self, x, y, w, h, name, kind, ident=None):
        """Граница C4: пунктир, подпись снизу слева, как в схемах для слайдов."""
        ident = ident or self._id()
        self.shapes[ident] = (x, y, w, h)
        self.back.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="16" fill="none" stroke="#8A8A8A" stroke-width="1.5" stroke-dasharray="8 4"/>')
        body, _ = text_block(x + 12, y + h - 46, [(name, 15, True, "#333333", 1), (f"[{kind}]", 14, False, "#333333", 1)], anchor="start")
        self.back.append(body)
        self._vertex(ident, f"<b>{esc(name)}</b><div>[{esc(kind)}]</div>", "rounded=1;arcSize=6;whiteSpace=wrap;html=1;dashed=1;dashPattern=8 4;fillColor=none;strokeColor=#8A8A8A;fontColor=#333333;fontSize=14;align=left;verticalAlign=bottom;spacing=10;labelBackgroundColor=none;container=0;", x, y, w, h)

    def node(self, x, y, w, h, name, kind, fill="#FFFFFF", ident=None):
        """Узел развёртывания C4: сплошная рамка, подпись сверху слева."""
        ident = ident or self._id()
        self.shapes[ident] = (x, y, w, h)
        self.back.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="8" fill="{fill}" stroke="#666666" stroke-width="1.5"/>')
        body, _ = text_block(x + 12, y + 4, [(name, 15, True, "#222222", 1), (f"[{kind}]", 13, False, "#444444", 1)], anchor="start")
        self.back.append(body)
        self._vertex(ident, f"<b>{esc(name)}</b><div>[{esc(kind)}]</div>", f"rounded=1;arcSize=3;whiteSpace=wrap;html=1;fillColor={fill};strokeColor=#666666;fontColor=#222222;fontSize=14;align=left;verticalAlign=top;spacing=10;labelBackgroundColor=none;", x, y, w, h)

    def note(self, x, y, w, text, size=13):
        rows = [(line, size, False, LABEL, 1) for line in wrap(text, size, w)]
        body, _ = text_block(x, y, rows, anchor="start")
        self.top.append(body)
        self._vertex(self._id(), esc(text), f"text;html=1;whiteSpace=wrap;align=left;verticalAlign=top;fontSize={size};fontColor={LABEL};", x, y, w, len(rows) * size * 1.22)

    # ---------- связи ----------

    def anchor(self, ident, side, t=0.5):
        x, y, w, h = self.shapes[ident]
        return {"l": (x, y + h * t), "r": (x + w, y + h * t), "t": (x + w * t, y), "b": (x + w * t, y + h)}[side]

    @staticmethod
    def _arrow(p, q):
        angle = math.atan2(q[1] - p[1], q[0] - p[0])
        size, half = 12, 5.5
        bx, by = q[0] - size * math.cos(angle), q[1] - size * math.sin(angle)
        left = (bx + half * math.sin(angle), by - half * math.cos(angle))
        right = (bx - half * math.sin(angle), by + half * math.cos(angle))
        return f'<path d="M{q[0]:.1f},{q[1]:.1f} L{left[0]:.1f},{left[1]:.1f} L{right[0]:.1f},{right[1]:.1f} z" fill="{EDGE}"/>'

    def edge(self, src, dst, via=(), label=(), at=None, both=False, dashed=False):
        """src/dst: (id, сторона l|r|t|b, доля); via — изломы; label — строки, первая жирная; at — центр подписи."""
        points = [self.anchor(*src), *via, self.anchor(*dst)]
        dash = ' stroke-dasharray="7 5"' if dashed else ""
        path = " ".join(f"{'M' if i == 0 else 'L'}{x:.1f},{y:.1f}" for i, (x, y) in enumerate(points))
        self.middle.append(f'<path d="{path}" fill="none" stroke="{EDGE}" stroke-width="2"{dash}/>')
        self.middle.append(self._arrow(points[-2], points[-1]))
        if both:
            self.middle.append(self._arrow(points[1], points[0]))
        geometry = "".join(f'<mxPoint x="{x:.0f}" y="{y:.0f}"/>' for x, y in via)
        exits = {"l": (0, None), "r": (1, None), "t": (None, 0), "b": (None, 1)}

        def port(prefix, side, t):
            fx, fy = exits[side]
            fx = t if fx is None else fx
            fy = t if fy is None else fy
            return f"{prefix}X={fx};{prefix}Y={fy};{prefix}Dx=0;{prefix}Dy=0;"

        style = f"edgeStyle=none;rounded=0;html=1;strokeColor={EDGE};strokeWidth=2;endArrow=block;endFill=1;" + ("startArrow=block;startFill=1;" if both else "") + ("dashed=1;" if dashed else "") + port("exit", *src[1:]) + port("entry", *dst[1:])
        self.cells.append(f'<mxCell id="{self._id()}" style="{style}" edge="1" parent="1" source="{src[0]}" target="{dst[0]}"><mxGeometry relative="1" as="geometry"><Array as="points">{geometry}</Array></mxGeometry></mxCell>')
        if label:
            if at is None:
                (ax, ay), (bx, by) = points[len(points) // 2 - 1], points[len(points) // 2]
                at = ((ax + bx) / 2, (ay + by) / 2)
            self.label(at, label)

    def label(self, at, lines, size=12.5):
        rows = [(line, size, i == 0, "#333333" if i == 0 else LABEL, 1) for i, line in enumerate(lines)]
        width = max(measure(line, size, i == 0) for i, line in enumerate(lines)) + 8
        _, height = text_block(0, 0, rows)
        x, y = at[0] - width / 2, at[1] - height / 2
        body, _ = text_block(at[0], y, rows)
        self.top.append(f'<rect x="{x:.1f}" y="{y - 2:.1f}" width="{width:.1f}" height="{height + 4:.1f}" fill="#FFFFFF"/>{body}')
        value = "<b>" + esc(lines[0]) + "</b>" + "".join("<br>" + esc(line) for line in lines[1:])
        self._vertex(self._id(), value, f"text;html=1;align=center;verticalAlign=middle;labelBackgroundColor=#FFFFFF;fontSize=12;fontColor={LABEL};", x, y, width, height)

    # ---------- легенда и вывод ----------

    def legend(self, x, y, items, width=520):
        """items: (вид, текст); вид — ключ PALETTE, «node», «line», «both» или «dashed»."""
        h = 44 + 30 * len(items)
        self.back.append(f'<rect x="{x}" y="{y}" width="{width}" height="{h}" fill="#FFFFFF" stroke="#9A9A9A" stroke-width="1.2"/>')
        body, _ = text_block(x + 14, y + 8, [("Обозначения", 14, True, "#222222", 1)], anchor="start")
        self.back.append(body)
        for i, (kind, text) in enumerate(items):
            cy = y + 52 + 30 * i
            if kind in PALETTE:
                fill, stroke = PALETTE[kind]
                if kind.startswith("person"):
                    self.back.append(f'<rect x="{x + 14}" y="{cy - 6}" width="30" height="15" rx="6" fill="{fill}"/><circle cx="{x + 29}" cy="{cy - 9}" r="6" fill="{fill}"/>')
                else:
                    self.back.append(f'<rect x="{x + 14}" y="{cy - 11}" width="30" height="22" rx="4" fill="{fill}" stroke="{stroke}"/>')
            elif kind == "node":
                self.back.append(f'<rect x="{x + 14}" y="{cy - 11}" width="30" height="22" rx="4" fill="#F4F6F8" stroke="#666666" stroke-width="1.5"/>')
            else:
                dash = ' stroke-dasharray="6 4"' if kind == "dashed" else ""
                self.back.append(f'<path d="M{x + 14},{cy} H{x + 42}" stroke="{EDGE}" stroke-width="2"{dash}/>' + self._arrow((x + 14, cy), (x + 46, cy)))
                if kind == "both":
                    self.back.append(self._arrow((x + 46, cy), (x + 12, cy)))
            row, _ = text_block(x + 58, cy - 10, [(text, 13, False, "#222222", 1)], anchor="start")
            self.back.append(row)

    def svg(self):
        model = f'<mxfile host="blood-supply"><diagram name="{esc(self.name)}" id="d1"><mxGraphModel grid="1" gridSize="10" page="1" pageWidth="{self.width}" pageHeight="{self.height}"><root><mxCell id="0"/><mxCell id="1" parent="0"/>{"".join(self.cells)}</root></mxGraphModel></diagram></mxfile>'
        layers = "".join(self.back + self.middle + self.front + self.top)
        return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.width}" height="{self.height}" viewBox="0 0 {self.width} {self.height}" '
                f'font-family="{FONT}" content="{esc(model)}"><title>{esc(self.name)}</title>'
                f'<rect width="100%" height="100%" fill="#FFFFFF"/>{layers}</svg>\n')

    def write(self, relative):
        path = ROOT / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.svg(), encoding="utf-8", newline="\n")
        return path
