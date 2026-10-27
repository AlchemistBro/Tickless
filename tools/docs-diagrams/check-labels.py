#!/usr/bin/env python3
"""Проверка схем для README: подпись не закрывает чужие линии, подписи не налезают друг на друга.

Подпись — белый прямоугольник без рамки под текстом. Он закрывает всё, что нарисовано под ним, поэтому
линия, проходящая под подписью, на картинке обрывается. Своя линия подписи (проходит через её центр) допустима.
Запуск: python tools/docs-diagrams/check-labels.py [файлы.svg]; без аргументов — схемы C4.
"""

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
DEFAULT = sorted((ROOT / "docs/c4").glob("*.svg"))
NUM = r"-?\d+(?:\.\d+)?"


def attrs(tag):
    return dict(re.findall(r'([\w:-]+)="([^"]*)"', tag))


def path_segments(d):
    """Отрезки пути из команд M, L, H, V, z (абсолютные координаты, как пишут генераторы)."""
    segments, pos, start = [], None, None
    for cmd, args in re.findall(r"([MLHVZz])([^MLHVZz]*)", d):
        values = [float(v) for v in re.findall(NUM, args)]
        if cmd == "M":
            pos = start = (values[0], values[1])
        elif cmd == "L":
            for i in range(0, len(values), 2):
                segments.append((pos, (values[i], values[i + 1])))
                pos = (values[i], values[i + 1])
        elif cmd == "H":
            segments.append((pos, (values[0], pos[1])))
            pos = (values[0], pos[1])
        elif cmd == "V":
            segments.append((pos, (pos[0], values[0])))
            pos = (pos[0], values[0])
        elif cmd in "Zz" and pos and start:
            segments.append((pos, start))
            pos = start
    return segments


def crosses(segment, box):
    """Пересекает ли отрезок внутренность прямоугольника (клиппинг Лианга — Барски)."""
    (x1, y1), (x2, y2) = segment
    left, top, right, bottom = box
    dx, dy = x2 - x1, y2 - y1
    t0, t1 = 0.0, 1.0
    for p, q in ((-dx, x1 - left), (dx, right - x1), (-dy, y1 - top), (dy, bottom - y1)):
        if p == 0:
            if q <= 0:
                return False
        else:
            t = q / p
            if p < 0:
                t0 = max(t0, t)
            else:
                t1 = min(t1, t)
            if t0 > t1:
                return False
    return t1 - t0 > 1e-6


def through(segment, point, tolerance=2.0):
    """Проходит ли отрезок через точку: так узнаём собственную линию подписи."""
    (x1, y1), (x2, y2) = segment
    px, py = point
    if not (min(x1, x2) - tolerance <= px <= max(x1, x2) + tolerance and min(y1, y2) - tolerance <= py <= max(y1, y2) + tolerance):
        return False
    length = ((x2 - x1) ** 2 + (y2 - y1) ** 2) ** 0.5 or 1
    return abs((x2 - x1) * (y1 - py) - (x1 - px) * (y2 - y1)) / length <= tolerance


def check(path):
    svg = path.read_text(encoding="utf-8")
    labels, lines = [], []
    for tag in re.findall(r"<(?:rect|path)\b[^>]*>", svg):
        a = attrs(tag)
        if tag.startswith("<rect"):
            if a.get("width", "").endswith("%"):
                continue
            x, y, w, h = (float(a[k]) for k in ("x", "y", "width", "height"))
            if a.get("fill") == "#FFFFFF" and "stroke" not in a:
                labels.append((x, y, x + w, y + h))
            elif "stroke" in a:
                lines += [((x, y), (x + w, y)), ((x + w, y), (x + w, y + h)), ((x + w, y + h), (x, y + h)), ((x, y + h), (x, y))]
        elif "stroke" in a or a.get("fill") not in (None, "none", "#FFFFFF"):
            lines += path_segments(a["d"])
    problems = []
    for i, box in enumerate(labels):
        inner = (box[0] + 1.5, box[1] + 1.5, box[2] - 1.5, box[3] - 1.5)
        center = ((box[0] + box[2]) / 2, (box[1] + box[3]) / 2)
        own = [s for s in lines if through(s, center)]
        hits = [s for s in lines if s not in own and crosses(s, inner)]
        if hits:
            (x1, y1), (x2, y2) = hits[0]
            problems.append(f"подпись у ({center[0]:.0f}, {center[1]:.0f}) закрывает линию ({x1:.0f},{y1:.0f})–({x2:.0f},{y2:.0f})")
        for other in labels[i + 1:]:
            if box[0] < other[2] - 1 and other[0] < box[2] - 1 and box[1] < other[3] - 1 and other[1] < box[3] - 1:
                problems.append(f"подписи у ({center[0]:.0f}, {center[1]:.0f}) и ({(other[0] + other[2]) / 2:.0f}, {(other[1] + other[3]) / 2:.0f}) перекрываются")
    return problems


def main():
    paths = [pathlib.Path(p) for p in sys.argv[1:]] or DEFAULT
    failed = 0
    for path in paths:
        problems = check(path)
        failed += len(problems)
        name = path.relative_to(ROOT) if path.is_absolute() and ROOT in path.parents else path
        for problem in problems:
            print(f"{name}: {problem}", file=sys.stderr)
    if failed:
        return 1
    print(f"Подписи: {len(paths)} схем, перекрытий нет")
    return 0


if __name__ == "__main__":
    sys.exit(main())
