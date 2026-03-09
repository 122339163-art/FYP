"""
FYP Open Day Poster Generator
==============================
Requirements:  pip install reportlab

Usage:
  1. Set the three image paths below to your actual files.
  2. Run:  python generate_poster.py
  3. Output: FYP_Project_Poster.pdf  (A1 portrait, print-ready)

Image notes:
  - All three images should have WHITE backgrounds so they sit flush in the cards.
  - Any common format works: PNG, JPG, TIFF, etc.
  - ARCH_IMG   : your system architecture diagram (seminar slide 7 / Fig. 1)
  - POWER_IMG  : matplotlib power trace plot      (seminar slide 10)
  - NETWORK_IMG: Wireshark screenshot or packet-rate plot (seminar slide 11)
"""

# ── CONFIGURE YOUR IMAGE PATHS HERE ──────────────────────────────────────────
ARCH_IMG    = "System_Architecture_Diagram.png"   # architecture diagram
POWER_IMG   = "power_trace_plot.png"              # power trace figure
NETWORK_IMG = "network_data_plot.png"             # network capture figure

OUTPUT_PDF  = "FYP_Project_Poster.pdf"
# ─────────────────────────────────────────────────────────────────────────────

from reportlab.lib.pagesizes import A1
from reportlab.pdfgen import canvas
from reportlab.lib import colors
import os

W, H = A1  # 1684 x 2384 points  (portrait)

# ── COLOUR PALETTE ────────────────────────────────────────────────────────────
NAVY        = colors.HexColor("#0B1F3A")
NAVY_MED    = colors.HexColor("#163154")
TEAL        = colors.HexColor("#007A85")
TEAL_LIGHT  = colors.HexColor("#00AAB5")
AMBER       = colors.HexColor("#E8890A")
WHITE       = colors.HexColor("#FFFFFF")
PAGE_BG     = colors.HexColor("#F4F6F9")
CARD_BG     = colors.HexColor("#FFFFFF")
CARD_BORDER = colors.HexColor("#D0DAE8")
BODY_TEXT   = colors.HexColor("#1C2B3A")
PH_BORDER   = colors.HexColor("#7ACBD3")
PH_LABEL    = colors.HexColor("#007A85")

# ── CANVAS ────────────────────────────────────────────────────────────────────
c = canvas.Canvas(OUTPUT_PDF, pagesize=A1)

# ── PAGE BACKGROUND ───────────────────────────────────────────────────────────
c.setFillColor(PAGE_BG)
c.rect(0, 0, W, H, fill=1, stroke=0)

# Subtle dot grid texture
c.setFillColor(colors.HexColor("#DCE5EE"))
for gx in range(0, int(W) + 30, 30):
    for gy in range(0, int(H) + 30, 30):
        c.circle(gx, gy, 1.0, fill=1, stroke=0)

# ── HEADER BAND ───────────────────────────────────────────────────────────────
HDR_H = 198
c.setFillColor(NAVY)
c.rect(0, H - HDR_H, W, HDR_H, fill=1, stroke=0)

c.setFillColor(TEAL_LIGHT)                          # teal top bar
c.rect(0, H - 8, W, 8, fill=1, stroke=0)

c.setFillColor(AMBER)                               # amber left stripe
c.rect(0, H - HDR_H, 10, HDR_H, fill=1, stroke=0)

c.setFillColor(NAVY_MED)                            # slightly lighter right block
c.rect(W - 220, H - HDR_H, 220, HDR_H, fill=1, stroke=0)
c.setFillColor(TEAL)
c.rect(W - 220, H - HDR_H, 6, HDR_H, fill=1, stroke=0)

c.setFillColor(WHITE)
c.setFont("Helvetica-Bold", 48)
c.drawString(28, H - 72,  "Security at the Edge with Machine Learning")

c.setFillColor(TEAL_LIGHT)
c.setFont("Helvetica", 23)
c.drawString(28, H - 108, "Creating a Correlated Network and Power Dataset for IoT IDS")

c.setFillColor(colors.HexColor("#A8C4D4"))
c.setFont("Helvetica", 18)
c.drawString(28, H - 143,
    "Ian Kenny   |   Supervisor: Prof. Emanuel Popovici   |   Co-Supervisor: Dr. Colin Murphy")

c.setFillColor(AMBER)                               # institution pill
c.roundRect(28, H - 183, 520, 28, 4, fill=1, stroke=0)
c.setFillColor(NAVY)
c.setFont("Helvetica-Bold", 16)
c.drawString(40, H - 170, "BE Electronic Engineering  ·  University College Cork")

c.setStrokeColor(TEAL_LIGHT)
c.setLineWidth(2.5)
c.line(0, H - HDR_H, W, H - HDR_H)

# ── LAYOUT GRID ───────────────────────────────────────────────────────────────
MARGIN = 38
GAP    = 18
TOP_Y  = H - HDR_H - GAP
UW     = W - 2 * MARGIN          # usable width
C2     = (UW - GAP) / 2          # two-column width
C3     = (UW - 2 * GAP) / 3      # three-column width
LX     = MARGIN                  # left column x
MX     = MARGIN + C2 + GAP       # right column x (two-col layout)

# ── HELPER: text card ─────────────────────────────────────────────────────────
def card(x, y, w, h, title, bullets, accent=TEAL):
    """White card with coloured top bar, left accent stripe, and bullet points."""
    c.setFillColor(colors.HexColor("#C8D4E0"))      # drop shadow
    c.roundRect(x + 3, y - 3, w, h, 6, fill=1, stroke=0)
    c.setFillColor(CARD_BG)                          # white body
    c.roundRect(x, y, w, h, 6, fill=1, stroke=0)
    c.setFillColor(accent)                           # coloured top bar
    c.roundRect(x, y + h - 28, w, 28, 6, fill=1, stroke=0)
    c.rect(x, y + h - 28, w, 14, fill=1, stroke=0)  # flatten bottom of top bar
    c.setFillColor(accent)                           # left accent stripe
    c.roundRect(x, y, 5, h, 3, fill=1, stroke=0)
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", 18)
    c.drawString(x + 16, y + h - 22, title)
    c.setStrokeColor(CARD_BORDER)                    # thin border
    c.setLineWidth(0.8)
    c.setFillColor(colors.transparent)
    c.roundRect(x, y, w, h, 6, fill=0, stroke=1)
    yp = y + h - 52
    for line in bullets:
        if yp < y + 10:
            break
        c.setFillColor(accent)
        c.circle(x + 22, yp + 5, 3.5, fill=1, stroke=0)
        c.setFillColor(BODY_TEXT)
        c.setFont("Helvetica", 15)
        c.drawString(x + 34, yp, line)
        yp -= 23


# ── HELPER: figure card (with image or placeholder) ───────────────────────────
def figure_card(x, y, w, h, tag_text, img_path, ph_label, ph_sublabel, accent=TEAL):
    """
    White card containing a figure.
    - If img_path exists on disk, the image is embedded.
    - If not, a dashed placeholder box is drawn instead.
    """
    PAD = 12
    img_x = x + PAD + 4
    img_y = y + PAD
    img_w = w - PAD * 2 - 4
    img_h = h - 50

    # Drop shadow
    c.setFillColor(colors.HexColor("#C8D4E0"))
    c.roundRect(x + 3, y - 3, w, h, 6, fill=1, stroke=0)
    # White body
    c.setFillColor(CARD_BG)
    c.roundRect(x, y, w, h, 6, fill=1, stroke=0)
    # Left accent stripe
    c.setFillColor(accent)
    c.roundRect(x, y, 5, h, 3, fill=1, stroke=0)
    # Border
    c.setStrokeColor(CARD_BORDER)
    c.setLineWidth(0.8)
    c.setFillColor(colors.transparent)
    c.roundRect(x, y, w, h, 6, fill=0, stroke=1)
    # Tag pill
    tw = c.stringWidth(tag_text, "Helvetica-Bold", 14) + 24
    c.setFillColor(accent)
    c.roundRect(x + 14, y + h - 34, tw, 24, 4, fill=1, stroke=0)
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", 14)
    c.drawString(x + 26, y + h - 25, tag_text)

    if img_path and os.path.exists(img_path):
        # ── embed the real image ──────────────────────────────────────────────
        c.setFillColor(WHITE)
        c.roundRect(img_x, img_y, img_w, img_h, 4, fill=1, stroke=0)
        c.drawImage(img_path, img_x, img_y,
                    width=img_w, height=img_h,
                    preserveAspectRatio=True, anchor='c')
    else:
        # ── draw placeholder ─────────────────────────────────────────────────
        c.setFillColor(WHITE)
        c.roundRect(img_x, img_y, img_w, img_h, 4, fill=1, stroke=0)
        c.setStrokeColor(PH_BORDER)
        c.setLineWidth(1.5)
        c.setDash(8, 5)
        c.roundRect(img_x, img_y, img_w, img_h, 4, fill=0, stroke=1)
        c.setDash()
        cx_ = img_x + img_w / 2
        cy_ = img_y + img_h / 2
        c.setStrokeColor(colors.HexColor("#B0D8DC"))
        c.setLineWidth(1)
        c.line(cx_ - 20, cy_, cx_ + 20, cy_)
        c.line(cx_, cy_ - 20, cx_, cy_ + 20)
        c.circle(cx_, cy_, 7, fill=0, stroke=1)
        c.setFillColor(PH_LABEL)
        c.setFont("Helvetica-Bold", 15)
        c.drawCentredString(cx_, cy_ + 28, ph_label)
        if ph_sublabel:
            c.setFillColor(colors.HexColor("#5AABB5"))
            c.setFont("Helvetica", 13)
            c.drawCentredString(cx_, cy_ + 10, ph_sublabel)


# ── HELPER: decorative corner brackets ────────────────────────────────────────
def corner_brackets(x, y, flip_x=False, flip_y=False):
    sx = -1 if flip_x else 1
    sy = -1 if flip_y else 1
    c.setStrokeColor(TEAL)
    c.setLineWidth(2.5)
    c.line(x, y, x + sx * 38, y)
    c.line(x, y, x, y + sy * 38)
    c.setStrokeColor(colors.HexColor("#A0C8D0"))
    c.setLineWidth(1)
    c.line(x + sx * 12, y + sy * 12, x + sx * 28, y + sy * 12)
    c.line(x + sx * 12, y + sy * 12, x + sx * 12, y + sy * 28)


# ═════════════════════════════════════════════════════════════════════════════
# ROW 1 — Motivation  +  Research Gap
# ═════════════════════════════════════════════════════════════════════════════
R1T = TOP_Y
R1H = 200

card(LX, R1T - R1H, C2, R1H,
     "Motivation",
     ["IoT devices face large, growing attack surfaces",
      "Resource-constrained — limited built-in security",
      "ML-based IDS require high-quality training data",
      "Power side-channels reflect internal device state",
      "Attacks affect both network traffic and power draw"],
     accent=TEAL)

card(MX, R1T - R1H, C2, R1H,
     "Research Gap",
     ["Network-only datasets: ToN-IoT, Edge-IIoTset, IoT-23",
      "Power-only datasets: Dragon Pi (DragonBoard / RPi)",
      "No synchronised multimodal dataset currently exists",
      "Limits joint network + power IDS research",
      "This work directly addresses that gap"],
     accent=NAVY)

# ═════════════════════════════════════════════════════════════════════════════
# ROW 2 — System Architecture  (full width)
# ═════════════════════════════════════════════════════════════════════════════
R2T = R1T - R1H - GAP
R2H = 310

figure_card(LX, R2T - R2H, UW, R2H,
            "SYSTEM ARCHITECTURE",
            ARCH_IMG,
            "[ Insert System Architecture Diagram ]",
            "Set ARCH_IMG path at the top of this script",
            accent=TEAL)

# ═════════════════════════════════════════════════════════════════════════════
# ROW 3 — IoT Emulation  |  Data Collection  |  Synchronization
# ═════════════════════════════════════════════════════════════════════════════
R3T = R2T - R2H - GAP
R3H = 215

card(LX, R3T - R3H, C3, R3H,
     "IoT Device Emulation",
     ["Smart camera behaviour written in C",
      "Idle + active video-capture phases",
      "TCP file uploads to host machine",
      "Randomised timing for realism",
      "UDP JSON event labels at each phase"],
     accent=TEAL)

card(LX + C3 + GAP, R3T - R3H, C3, R3H,
     "Data Collection Pipeline",
     ["Network traffic → PCAP via tcpdump",
      "Power sampled externally at 4,900 Hz",
      "Labels on dedicated port 9001",
      "Label packets excluded from training CSV",
      "Python post-processing merges both streams"],
     accent=NAVY)

card(LX + 2 * (C3 + GAP), R3T - R3H, C3, R3H,
     "Synchronization Method",
     ["Both streams converted to master time",
      "JSON program-start packet as network anchor",
      "Power CSV start time as power anchor",
      "Linear interpolation at merge step",
      "No artificial power disturbances introduced"],
     accent=TEAL)

# ═════════════════════════════════════════════════════════════════════════════
# ROW 4 — Power trace plot  |  Network capture plot
# ═════════════════════════════════════════════════════════════════════════════
R4T = R3T - R3H - GAP
R4H = 305

figure_card(LX, R4T - R4H, C2, R4H,
            "COLLECTED POWER DATA",
            POWER_IMG,
            "[ Insert Power Trace Plot ]",
            "Set POWER_IMG path at the top of this script",
            accent=TEAL)

figure_card(MX, R4T - R4H, C2, R4H,
            "COLLECTED NETWORK DATA",
            NETWORK_IMG,
            "[ Insert Network Capture / Packet-Rate Plot ]",
            "Set NETWORK_IMG path at the top of this script",
            accent=NAVY)

# ═════════════════════════════════════════════════════════════════════════════
# ROW 5 — Merged Dataset Structure  |  Future Work
# ═════════════════════════════════════════════════════════════════════════════
R5T = R4T - R4H - GAP
R5H = 200

card(LX, R5T - R5H, C2, R5H,
     "Merged Dataset Structure",
     ["date, time  —  unified master timestamp",
      "current  —  continuous 4,900 Hz power signal",
      "source, destination  —  packet addresses",
      "protocol, length  —  per-packet fields",
      "info  —  Wireshark metadata field",
      "Labels: normal / attack  (from event markers)"],
     accent=TEAL)

card(MX, R5T - R5H, C2, R5H,
     "Future Work",
     ["Extend baseline normal-operation captures",
      "Execute DoS, brute-force, port-scan attacks",
      "CSV alignment, labelling, and validation",
      "Feature extraction from power + network signals",
      "Train & compare network-only, power-only,",
      "  and combined multimodal IDS models"],
     accent=NAVY)

# ═════════════════════════════════════════════════════════════════════════════
# FOOTER BAND
# ═════════════════════════════════════════════════════════════════════════════
FOOT_H = 80
c.setFillColor(NAVY)
c.rect(0, 0, W, FOOT_H, fill=1, stroke=0)
c.setFillColor(TEAL_LIGHT)
c.rect(0, FOOT_H - 3, W, 3, fill=1, stroke=0)

c.setFillColor(colors.HexColor("#A8C4D4"))
c.setFont("Helvetica", 15)
c.drawString(MARGIN, 48,
    "University College Cork  ·  BE Electronic Engineering  ·  FYP 2025–2026")

c.setFillColor(TEAL_LIGHT)
c.setFont("Helvetica-Bold", 15)
c.drawRightString(W - MARGIN, 48,
    "IoT Security  ·  Machine Learning  ·  Side-Channel Analysis")

c.setFillColor(colors.HexColor("#6A90A8"))
c.setFont("Helvetica", 13)
c.drawCentredString(W / 2, 24,
    "Supervisors: Prof. Emanuel Popovici  &  Dr. Colin Murphy   |   Ian Kenny  (122339163)")

c.setFillColor(AMBER)
c.rect(0, 0, W, 6, fill=1, stroke=0)

# ── CORNER BRACKETS ───────────────────────────────────────────────────────────
corner_brackets(MARGIN - 6,     H - HDR_H - GAP + 4)
corner_brackets(W - MARGIN + 6, H - HDR_H - GAP + 4, flip_x=True)
corner_brackets(MARGIN - 6,     FOOT_H + 4,            flip_y=True)
corner_brackets(W - MARGIN + 6, FOOT_H + 4,            flip_x=True, flip_y=True)

# ── SAVE ──────────────────────────────────────────────────────────────────────
c.save()
print(f"Poster saved to: {OUTPUT_PDF}")
