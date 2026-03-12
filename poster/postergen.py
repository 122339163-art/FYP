"""
FYP Open Day Poster  —  generate_poster.py
==========================================
Requirements:  pip install reportlab
Run:           python generate_poster.py
"""

# ── IMAGE PATHS ───────────────────────────────────────────────────────────────
ARCH_IMG    = "System_Architecture_Diagram.png"
POWER_IMG   = "current_vs_time.png"
NETWORK_IMG = "packet_rate_vs_time.png"
LABEL_IMG   = "current_by_label.png"
OUTPUT_PDF  = "FYP_Project_Poster.pdf"
# ─────────────────────────────────────────────────────────────────────────────

from reportlab.lib.pagesizes import A1
from reportlab.pdfgen import canvas
from reportlab.lib import colors
import os

W, H = A1   # 1684 × 2384 pt  (portrait)

# ── PALETTE ───────────────────────────────────────────────────────────────────
NAVY        = colors.HexColor("#0B1F3A")
NAVY_MED    = colors.HexColor("#163154")
TEAL        = colors.HexColor("#007A85")
TEAL_LIGHT  = colors.HexColor("#00AAB5")
AMBER       = colors.HexColor("#E8890A")
WHITE       = colors.HexColor("#FFFFFF")
PAGE_BG     = colors.HexColor("#F2F5F9")
CARD_BG     = colors.HexColor("#FFFFFF")
CARD_BORDER = colors.HexColor("#CDD8E8")
SHADOW      = colors.HexColor("#C4D0DF")
BODY_TEXT   = colors.HexColor("#1C2B3A")

# ── CANVAS ────────────────────────────────────────────────────────────────────
c = canvas.Canvas(OUTPUT_PDF, pagesize=A1)

# ── PAGE BACKGROUND ───────────────────────────────────────────────────────────
c.setFillColor(PAGE_BG)
c.rect(0, 0, W, H, fill=1, stroke=0)
c.setFillColor(colors.HexColor("#D8E3EE"))
for gx in range(0, int(W) + 30, 30):
    for gy in range(0, int(H) + 30, 30):
        c.circle(gx, gy, 0.9, fill=1, stroke=0)

# ── HEADER ────────────────────────────────────────────────────────────────────
HDR_H = 192
c.setFillColor(NAVY)
c.rect(0, H - HDR_H, W, HDR_H, fill=1, stroke=0)
c.setFillColor(TEAL_LIGHT)
c.rect(0, H - 9, W, 9, fill=1, stroke=0)
c.setFillColor(AMBER)
c.rect(0, H - HDR_H, 10, HDR_H, fill=1, stroke=0)
# Logo area — white rounded rect in the header right side
LOGO_IMG = "/mnt/user-data/uploads/UCC_Master_Logo_2025_RGB_DIGITAL.png"
LOGO_W = 290
LOGO_H = HDR_H - 20
LOGO_X = W - LOGO_W - 18
LOGO_Y = H - HDR_H + 10
c.drawImage(LOGO_IMG, LOGO_X, LOGO_Y,
            width=LOGO_W, height=LOGO_H,
            preserveAspectRatio=True, anchor='c')

c.setFillColor(WHITE)
c.setFont("Helvetica-Bold", 50)
c.drawString(26, H - 72, "Security at the Edge with Machine Learning")
c.setFillColor(TEAL_LIGHT)
c.setFont("Helvetica", 23)
c.drawString(26, H - 108, "Creating a Correlated Network and Power Dataset for IoT IDS")
c.setFillColor(colors.HexColor("#A0BFCF"))
c.setFont("Helvetica", 18)
c.drawString(26, H - 142,
    "Ian Kenny   |   Supervisor: Prof. Emanuel Popovici   |   Co-Supervisor: Dr. Colin Murphy")
c.setFillColor(AMBER)
c.roundRect(26, H - 180, 510, 28, 4, fill=1, stroke=0)
c.setFillColor(NAVY)
c.setFont("Helvetica-Bold", 16)
c.drawString(38, H - 166, "BE Electronic Engineering  ·  University College Cork")
c.setStrokeColor(TEAL_LIGHT)
c.setLineWidth(2.5)
c.line(0, H - HDR_H, W, H - HDR_H)

# ── LAYOUT ────────────────────────────────────────────────────────────────────
M      = 36
G      = 16
FOOT_H = 76
TOP    = H - HDR_H - G        # 2176 pt
UW     = W - 2*M              # 1612 pt
C2     = (UW - G) / 2         # 798 pt
C3     = (UW - 2*G) / 3       # 526.7 pt
LX     = M
MX     = M + C2 + G
RX     = M + 2*(C3 + G)

# Row heights — sum to exactly TOP - FOOT_H - 5*G = 2020 pt
R1H = 370    # text cards
R2H = 460    # architecture diagram
R3H = 340    # method cards
R4H = 460    # two wide plots
R5H = 390    # boxplot + stacked cards

# ── HELPERS ───────────────────────────────────────────────────────────────────
def shadow_card(x, y, w, h, r=6):
    c.setFillColor(SHADOW)
    c.roundRect(x+3, y-3, w, h, r, fill=1, stroke=0)
    c.setFillColor(CARD_BG)
    c.roundRect(x, y, w, h, r, fill=1, stroke=0)
    c.setStrokeColor(CARD_BORDER)
    c.setLineWidth(0.8)
    c.setFillColor(colors.transparent)
    c.roundRect(x, y, w, h, r, fill=0, stroke=1)

def tag_pill(x, y, text, accent, fsize=14):
    tw = c.stringWidth(text, "Helvetica-Bold", fsize) + 24
    c.setFillColor(accent)
    c.roundRect(x, y, tw, 24, 4, fill=1, stroke=0)
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", fsize)
    c.drawString(x + 12, y + 6, text)

def text_card(x, y, w, h, title, bullets, accent=TEAL,
              title_size=19, bullet_size=19, bullet_gap=38):
    """White card with coloured top bar and bullet list."""
    shadow_card(x, y, w, h)
    # coloured top bar
    bar_h = 32
    c.setFillColor(accent)
    c.roundRect(x, y+h-bar_h, w, bar_h, 6, fill=1, stroke=0)
    c.rect(x, y+h-bar_h, w, bar_h//2, fill=1, stroke=0)
    # left accent stripe
    c.setFillColor(accent)
    c.roundRect(x, y, 5, h, 3, fill=1, stroke=0)
    # title
    c.setFillColor(WHITE)
    c.setFont("Helvetica-Bold", title_size)
    c.drawString(x+16, y+h-bar_h+8, title)
    # bullets
    yp = y + h - bar_h - 28
    for line in bullets:
        if yp < y + 10:
            break
        c.setFillColor(accent)
        c.circle(x+22, yp+6, 4, fill=1, stroke=0)
        c.setFillColor(BODY_TEXT)
        c.setFont("Helvetica", bullet_size)
        c.drawString(x+36, yp, line)
        yp -= bullet_gap

def image_card(x, y, w, h, tag_text, img_path, accent=TEAL, tag_fsize=14):
    """Card containing a real embedded image."""
    shadow_card(x, y, w, h)
    c.setFillColor(accent)
    c.roundRect(x, y, 5, h, 3, fill=1, stroke=0)
    tag_pill(x+12, y+h-36, tag_text, accent, fsize=tag_fsize)
    PAD = 12
    ix, iy = x+PAD+4, y+PAD
    iw, ih = w-PAD*2-4, h-52
    c.setFillColor(WHITE)
    c.roundRect(ix, iy, iw, ih, 3, fill=1, stroke=0)
    if os.path.exists(img_path):
        c.drawImage(img_path, ix, iy, width=iw, height=ih,
                    preserveAspectRatio=True, anchor='c')

def corner_brackets(x, y, flip_x=False, flip_y=False):
    sx = -1 if flip_x else 1
    sy = -1 if flip_y else 1
    c.setStrokeColor(TEAL); c.setLineWidth(2.5)
    c.line(x, y, x+sx*36, y); c.line(x, y, x, y+sy*36)
    c.setStrokeColor(colors.HexColor("#9EC4CC")); c.setLineWidth(1)
    c.line(x+sx*11, y+sy*11, x+sx*26, y+sy*11)
    c.line(x+sx*11, y+sy*11, x+sx*11, y+sy*26)

# ══════════════════════════════════════════════════════════════════════════════
# ROW 1  —  Motivation  |  Research Gap
# ══════════════════════════════════════════════════════════════════════════════
R1T = TOP

text_card(LX, R1T - R1H, C2, R1H,
    "Motivation",
    ["IoT devices face large, growing attack surfaces",
     "Resource-constrained with limited built-in security",
     "Traditional IDS struggle to detect novel attacks",
     "ML-based IDS can model normal behaviour and flag deviations",
     "Power side-channels reflect internal device state",
     "Attacks affect both network traffic and power consumption",
     "Multimodal data enables richer anomaly detection"],
    accent=TEAL)

text_card(MX, R1T - R1H, C2, R1H,
    "Research Gap",
    ["Network-only datasets: ToN-IoT, Edge-IIoTset, IoT-23",
     "Power-only side-channel datasets: Dragon Pi",
     "Multimodal datasets are scarce and domain-specific (e.g. CICEVSE2024)",
     "Joint network + power IDS research is constrained",
     "Existing frameworks are domain-specific or single-modality",
     "Reproducible controlled testbeds are scarce",
     "This framework directly addresses that gap"],
    accent=NAVY)

# ══════════════════════════════════════════════════════════════════════════════
# ROW 2  —  System Architecture  (full width)
# ══════════════════════════════════════════════════════════════════════════════
R2T = R1T - R1H - G

image_card(LX, R2T - R2H, UW, R2H,
    "SYSTEM ARCHITECTURE", ARCH_IMG, accent=TEAL, tag_fsize=15)

# ══════════════════════════════════════════════════════════════════════════════
# ROW 3  —  IoT Emulation  |  Data Collection  |  Synchronization
# ══════════════════════════════════════════════════════════════════════════════
R3T = R2T - R2H - G

text_card(LX, R3T - R3H, C3, R3H,
    "IoT Device Emulation",
    ["Smart camera behaviour written in C",
     "Idle periods and active capture phases",
     "TCP file uploads to host machine",
     "Randomised inter-phase timing for realism",
     "UDP JSON event labels transmitted at each phase",
     "Cross-compiled for ARM and deployed via SCP"],
    accent=TEAL)

text_card(LX + C3 + G, R3T - R3H, C3, R3H,
    "Data Collection Pipeline",
    ["Network traffic captured as PCAP via tcpdump",
     "Host-side capture avoids overhead on device",
     "Power sampled externally at 4,900 Hz (Agilent N6705A)",
     "Labels transmitted on dedicated port 9001",
     "Label packets excluded from training dataset",
     "Python pipeline merges and exports final CSV"],
    accent=NAVY)

text_card(RX, R3T - R3H, C3, R3H,
    "Synchronization Method",
    ["Both streams independently timestamped",
     "Converted to a shared master time reference",
     "JSON program-start event anchors network stream",
     "Power CSV capture start time as power anchor",
     "Linear interpolation fills gaps at merge step",
     "No artificial disturbances introduced to power signal"],
    accent=TEAL)

# ══════════════════════════════════════════════════════════════════════════════
# ROW 4  —  Current vs Time  |  Packet Rate vs Time
# ══════════════════════════════════════════════════════════════════════════════
R4T = R3T - R3H - G

image_card(LX, R4T - R4H, C2, R4H,
    "CURRENT vs TIME", POWER_IMG, accent=TEAL, tag_fsize=15)

image_card(MX, R4T - R4H, C2, R4H,
    "PACKET RATE vs TIME", NETWORK_IMG, accent=NAVY, tag_fsize=15)

# ══════════════════════════════════════════════════════════════════════════════
# ROW 5  —  Current Distribution by Label  |  Dataset Structure + Future Work
# ══════════════════════════════════════════════════════════════════════════════
R5T = R4T - R4H - G

BOX_W  = C3 * 2 + G            # ~2/3 width suits the boxplot aspect ratio
SIDE_X = LX + BOX_W + G
SIDE_W = UW - BOX_W - G
SIDE_H = (R5H - G) // 2        # each stacked card gets half the height

image_card(LX, R5T - R5H, BOX_W, R5H,
    "CURRENT DISTRIBUTION BY LABEL", LABEL_IMG, accent=TEAL, tag_fsize=15)

text_card(SIDE_X, R5T - SIDE_H, SIDE_W, SIDE_H,
    "Dataset Structure",
    ["date / time  —  unified master timestamp",
     "current  —  4,900 Hz continuous power signal",
     "source, dest, protocol, length, info",
     "Labels: idle / camera operation / uploading"],
    accent=NAVY, bullet_size=19, bullet_gap=38)

text_card(SIDE_X, R5T - R5H, SIDE_W, SIDE_H,
    "Future Work",
    ["Execute DoS, brute-force, port-scan attacks",
     "Feature extraction from both modalities",
     "Train & compare uni- vs multimodal IDS models"],
    accent=TEAL, bullet_size=19, bullet_gap=38)

# ── FOOTER ────────────────────────────────────────────────────────────────────
c.setFillColor(NAVY)
c.rect(0, 0, W, FOOT_H, fill=1, stroke=0)
c.setFillColor(TEAL_LIGHT)
c.rect(0, FOOT_H - 3, W, 3, fill=1, stroke=0)
c.setFillColor(colors.HexColor("#A0BFCF"))
c.setFont("Helvetica", 15)
c.drawString(M, 48,
    "University College Cork  ·  BE Electronic Engineering  ·  FYP 2025–2026")
c.setFillColor(TEAL_LIGHT)
c.setFont("Helvetica-Bold", 15)
c.drawRightString(W - M, 48,
    "IoT Security  ·  Machine Learning  ·  Side-Channel Analysis")
c.setFillColor(colors.HexColor("#60889A"))
c.setFont("Helvetica", 13)
c.drawCentredString(W/2, 26,
    "Supervisors: Prof. Emanuel Popovici  &  Dr. Colin Murphy   |   Ian Kenny  (122339163)")
c.setFillColor(AMBER)
c.rect(0, 0, W, 6, fill=1, stroke=0)

# ── CORNER BRACKETS ───────────────────────────────────────────────────────────
corner_brackets(M - 5,     H - HDR_H - G + 2)
corner_brackets(W - M + 5, H - HDR_H - G + 2, flip_x=True)
corner_brackets(M - 5,     FOOT_H + 2,          flip_y=True)
corner_brackets(W - M + 5, FOOT_H + 2,          flip_x=True, flip_y=True)

c.save()
print(f"Poster saved → {OUTPUT_PDF}")
