#!/usr/bin/env python3
"""tile_gen.py — generate a unique branded featured image per listing.

The business name is the hero (auto-wrapped + auto-sized), over a
category-coloured gradient with a small category icon and the Glasgow Trader
wordmark. Google Places photos are ToS-frozen (rule 5); these original tiles
give every card a distinct, professional image until the owner uploads real
photos via the claim flow.
"""
import html
import math
import os

from PIL import Image, ImageDraw, ImageFont

W, H = 1200, 775
FONT_BOLD = "/System/Library/Fonts/Supplemental/Arial Bold.ttf"
FONT_BLACK = "/System/Library/Fonts/Supplemental/Arial Black.ttf"

# category id -> (label, top colour, bottom colour, icon name)
CATEGORY_STYLE = {
    159: ("Gardener", (46, 125, 50), (27, 94, 32), "leaf"),
    161: ("Landscaper", (0, 105, 92), (0, 77, 64), "hills"),
    166: ("Tree Surgeon", (51, 105, 30), (33, 66, 20), "tree"),
    22:  ("Plumber", (21, 101, 192), (13, 71, 161), "drop"),
    26:  ("Electrician", (40, 53, 147), (26, 35, 126), "bolt"),
    82:  ("Builder", (191, 101, 21), (130, 66, 12), "bricks"),
}
DEFAULT_STYLE = ("Listing", (55, 71, 79), (38, 50, 56), "leaf")


def _font(path, size):
    return ImageFont.truetype(path, size)


def _vgrad(top, bot):
    img = Image.new("RGB", (W, H), top)
    d = ImageDraw.Draw(img)
    for y in range(H):
        t = y / H
        d.line([(0, y), (W, y)],
               fill=tuple(int(top[i] + (bot[i] - top[i]) * t) for i in range(3)))
    return img


def _wrap(draw, text, font, max_w):
    words, lines, cur = text.split(), [], ""
    for w in words:
        trial = (cur + " " + w).strip()
        if draw.textlength(trial, font=font) <= max_w:
            cur = trial
        else:
            if cur:
                lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines


def _fit(draw, text, max_w, max_h, start=104, low=40, max_lines=3):
    for size in range(start, low - 1, -4):
        font = _font(FONT_BLACK, size)
        lines = _wrap(draw, text, font, max_w)
        if len(lines) > max_lines:
            continue
        lh = (draw.textbbox((0, 0), "Ag", font=font)[3]) + 12
        if lh * len(lines) <= max_h:
            return font, lines, lh
    font = _font(FONT_BLACK, low)
    lines = _wrap(draw, text, font, max_w)[:max_lines]
    lh = (draw.textbbox((0, 0), "Ag", font=font)[3]) + 12
    return font, lines, lh


def _icon(img, d, name, cx, cy, s, bg):
    if name == "leaf":
        L = int(s * 2.6); A = s * 0.72
        lf = Image.new("RGBA", (L, L), (0, 0, 0, 0)); ld = ImageDraw.Draw(lf)
        R, Lt, n = [], [], 40
        for i in range(n + 1):
            t = i / n; y = t * (L - 4) + 2; hw = A * math.sin(math.pi * t) ** 0.92
            R.append((L / 2 + hw, y)); Lt.append((L / 2 - hw, y))
        ld.polygon(R + Lt[::-1], fill=(255, 255, 255, 235))
        ld.line([(L / 2, L * 0.12), (L / 2, L * 0.88)], fill=bg + (255,), width=5)
        lf = lf.rotate(45, expand=True, resample=Image.BICUBIC)
        img.paste(lf, (cx - lf.width // 2, cy - lf.height // 2), lf)
    elif name == "hills":
        col = (255, 255, 255, 235)
        d.ellipse([cx + s * 0.35, cy - s * 1.1, cx + s * 0.9, cy - s * 0.55], fill=col)
        for dx, r in [(-s * 0.6, s * 1.05), (s * 0.2, s * 1.3), (s * 0.9, s * 0.95)]:
            d.pieslice([cx + dx - r, cy - r * 0.5, cx + dx + r, cy + r * 1.5], 180, 360, fill=col)
    elif name == "tree":
        col = (255, 255, 255, 235)
        d.rectangle([cx - int(s * 0.12), cy, cx + int(s * 0.12), cy + int(s * 1.05)], fill=col)
        d.ellipse([cx - s, cy - s * 1.25, cx + s, cy + int(s * 0.3)], fill=col)
        d.ellipse([cx - s * 1.05, cy - s * 0.45, cx - s * 0.3, cy + int(s * 0.45)], fill=col)
        d.ellipse([cx + s * 0.3, cy - s * 0.45, cx + s * 1.05, cy + int(s * 0.45)], fill=col)
    elif name == "drop":
        col = (255, 255, 255, 235)
        d.ellipse([cx - s * 0.7, cy - s * 0.2, cx + s * 0.7, cy + s], fill=col)
        d.polygon([(cx, cy - s), (cx - s * 0.7, cy + s * 0.2), (cx + s * 0.7, cy + s * 0.2)], fill=col)
    elif name == "bolt":
        col = (255, 255, 255, 235)
        d.polygon([(cx + s * 0.35, cy - s), (cx - s * 0.55, cy + s * 0.15),
                   (cx - s * 0.05, cy + s * 0.15), (cx - s * 0.35, cy + s),
                   (cx + s * 0.55, cy - s * 0.15), (cx + s * 0.05, cy - s * 0.15)],
                  fill=col)
    elif name == "bricks":
        col = (255, 255, 255, 235)
        bw, bh, gap = s * 0.62, s * 0.34, s * 0.08
        for row in range(3):
            y0 = cy - s * 0.55 + row * (bh + gap)
            off = 0 if row % 2 == 0 else -(bw + gap) / 2
            for i in (-1, 0, 1):
                x0 = cx + off + i * (bw + gap) - bw / 2
                d.rectangle([x0, y0, x0 + bw, y0 + bh], fill=col)


def make_business_tile(name, cat_id, location, out_path):
    label, top, bot, icon = CATEGORY_STYLE.get(cat_id, DEFAULT_STYLE)
    name = html.unescape(name or "").strip()
    img = _vgrad(top, bot)
    d = ImageDraw.Draw(img, "RGBA")

    # small category icon, top-centre
    _icon(img, d, icon, W // 2, 150, 54, top)

    # business name — hero, auto-fit, vertically centred in the mid band
    font, lines, lh = _fit(d, name, max_w=1000, max_h=330)
    total = lh * len(lines)
    y = 300 + (330 - total) // 2
    for line in lines:
        d.text((W // 2, y + lh // 2), line, font=font, fill=(255, 255, 255), anchor="mm")
        y += lh

    # category · location
    sub = f"{label}  ·  {location}" if location else label
    d.text((W // 2, 662), sub, font=_font(FONT_BOLD, 40),
           fill=(255, 255, 255, 235), anchor="mm")
    d.line([(W // 2 - 150, 706), (W // 2 + 150, 706)], fill=(255, 255, 255, 110), width=2)
    d.text((W // 2, 738), "GLASGOW TRADER", font=_font(FONT_BOLD, 30),
           fill=(255, 255, 255, 220), anchor="mm")

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    img.convert("RGB").save(out_path, "JPEG", quality=86)
    return out_path
