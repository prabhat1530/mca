# chg1_merged_parser.py — OCR-first CHG-1 extractor (fixed)
# -------------------------------------------------------------------------------------------------
# Merge policy per your request:
#   • Sections 1–9  → from Codebase #2 (more robust pre-9 extraction + strong Section 9)
#   • Sections 10–16 → from Codebase #1 (you preferred its behavior “after 9”)
#
# Highlights
#   - OCR ENABLED: uses pytesseract if Tesseract is available; combines text-layer + OCR text.
#   - Region OCR: when a label is found, OCR a right/below region if text-layer neighbor is empty.
#   - Section 7 detection from #2 (lien/hypothecation cues + “Others” fix).
#   - Section 9 uses #2’s robust parser (works with Address Line I/II OR single Address blob,
#     solid PAN/email/Pin fallbacks, etc.).
#   - Sections 10–16 follow #1: simpler, light-weight cascades you preferred post-9.
#   - Outputs CSV, XLSX, JSON (one row per PDF).
# -------------------------------------------------------------------------------------------------

import os, re, io, json, unicodedata, shutil
from datetime import datetime
from typing import Optional  # 3.9-compatible type hints

import fitz  # PyMuPDF
from PIL import Image, ImageOps, ImageFilter
try:
    import pytesseract
except Exception:
    pytesseract = None

import pandas as pd

# ===== CONFIG =====
INPUT_PATH  = "13_Form_CHG-1-17042021_signed.pdf"   # file or folder (you can point this to a folder)
OUTPUT_BASE = "chg1_merged_output"
APPEND_MODE = False
OCR_LANGS   = "eng"      # "eng+hin" if needed
BASE_DPI    = 300
REGION_DPI  = 400
# ==================

# ---- OCR availability ----
HAVE_TESS = shutil.which("tesseract") is not None and (pytesseract is not None)

def _preprocess_for_ocr(img: Image.Image) -> Image.Image:
    g = ImageOps.grayscale(img)
    g = ImageOps.autocontrast(g)
    g = g.filter(ImageFilter.UnsharpMask(radius=1, percent=150, threshold=3))
    return g

def _ocr_image_to_text(img: Image.Image, lang=OCR_LANGS) -> str:
    if not HAVE_TESS:
        return ""
    try:
        img = _preprocess_for_ocr(img)
        return pytesseract.image_to_string(img, lang=lang)
    except Exception:
        return ""

def _ocr_page_image(p: fitz.Page, dpi=BASE_DPI) -> Optional[Image.Image]:
    try:
        pix = p.get_pixmap(dpi=dpi)
        return Image.open(io.BytesIO(pix.tobytes("png")))
    except Exception:
        return None

# ---------------- Text extraction ----------------
def extract_text_blocks_and_images(pdf_path):
    """
    Returns:
      full_text (text-layer + OCR combined),
      page_blocks: list[list[{"rect":(x0,y0,x1,y1), "text":...}]],
      page_images: list[PIL.Image] (rasterized pages @BASE_DPI),
      page_sizes:  list[(width_pt,height_pt)] in PDF points for coordinate mapping
    """
    doc = fitz.open(pdf_path)
    page_texts, page_blocks, page_images, page_sizes = [], [], [], []

    for p in doc:
        # 1) text layer
        t_text = (p.get_text("text") or "").replace("\r", "\n")

        # 2) OCR full page
        ocr_img = _ocr_page_image(p, dpi=BASE_DPI)
        page_images.append(ocr_img)
        page_sizes.append((p.rect.width, p.rect.height))
        t_ocr = _ocr_image_to_text(ocr_img, OCR_LANGS) if ocr_img is not None else ""

        # 3) Combine (text-layer first, OCR appended)
        combined = (t_text or "")
        if t_ocr and len(t_ocr.strip()) > 20:
            combined = combined + "\n" + t_ocr
        page_texts.append(combined)

        # 4) text blocks (layout)
        blocks = p.get_text("blocks") or []
        blocks = sorted(blocks, key=lambda b: (round(b[1]), round(b[0])))
        bdicts = []
        for b in blocks:
            if len(b) >= 6:
                x0, y0, x1, y1, txt, *_ = b
                bdicts.append({"rect": (x0, y0, x1, y1), "text": (txt or "").replace("\r", "\n")})
        page_blocks.append(bdicts)

    doc.close()
    return "\n".join(page_texts), page_blocks, page_images, page_sizes

# ---------------- Normalizers & helpers ----------------
def norm_text(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKC", s)
    return (s.replace("—","-").replace("–","-").replace("­","")
             .replace("ﬁ","fi").replace("ﬂ","fl"))

def norm_space(s: str) -> str:
    return re.sub(r"[ \t]+", " ", (s or "").strip())

def norm_date(d: str) -> str:
    if not d: return ""
    d = d.strip().replace(".", "/").replace("-", "/").replace("\\", "/")
    d = re.sub(r"[^\d/]", "", d)
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%d %m %Y", "%d/%m/%Y "):
        try:
            return datetime.strptime(d.strip(), fmt).strftime("%d/%m/%Y")
        except Exception:
            pass
    m = re.search(r"\b(\d{2})(\d{2})(\d{4})\b", d)
    if m: return f"{m.group(1)}/{m.group(2)}/{m.group(3)}"
    return d

EMAIL_RE = re.compile(r"[\w\.-]+@[\w\.-]+\.\w+")
PAN_RE   = re.compile(r"\b([A-Z]{5}[0-9]{4}[A-Z])\b")
RS_AMOUNT_RE = re.compile(
    r"(?:₹|rs\.?|in\s*rs\.?)\s*[:\-]?\s*([0-9][0-9,\s\.]*)(?:\s*/-\s*)?",
    re.IGNORECASE
)

def neighbors(blocks, idx, max_dist_y=30, x_tol=140):
    """Blocks below or to the right near label block (from codebase #2)."""
    x0, y0, x1, y1 = blocks[idx]["rect"]
    for j in range(idx + 1, len(blocks)):
        X0, Y0, X1, Y1 = blocks[j]["rect"]
        if Y0 >= y1 and (Y0 - y1) < max_dist_y and abs(X0 - x0) < x_tol:
            yield blocks[j]
        if abs(Y0 - y0) < 18 and X0 > x1:
            yield blocks[j]

def find_next_nonempty(blocks, idx, max_steps=8):
    steps = 0
    for nb in neighbors(blocks, idx, max_dist_y=80, x_tol=280):
        txt = (nb["text"] or "").strip()
        if txt:
            return txt
        steps += 1
        if steps >= max_steps:
            break
    return ""

def _ocr_region_from_page(page_img: Optional[Image.Image], page_size_pts, rect_pts, dpi=REGION_DPI) -> str:
    """OCR a rectangle (in PDF points) from a pre-rendered page image."""
    if page_img is None or not HAVE_TESS:
        return ""
    pw, ph = page_size_pts
    sx = page_img.width  / float(pw or 1)
    sy = page_img.height / float(ph or 1)
    x0, y0, x1, y1 = rect_pts
    pad = 6
    x0p = max(0, int(x0 * sx) - pad); y0p = max(0, int(y0 * sy) - pad)
    x1p = min(page_img.width,  int(x1 * sx) + pad); y1p = min(page_img.height, int(y1 * sy) + pad)
    if x1p <= x0p or y1p <= y0p:
        return ""
    crop = page_img.crop((x0p, y0p, x1p, y1p))
    if REGION_DPI > BASE_DPI:
        scale = REGION_DPI / float(BASE_DPI)
        crop = crop.resize((int(crop.width*scale), int(crop.height*scale)), Image.BICUBIC)
    return _ocr_image_to_text(crop, OCR_LANGS)

def find_label_value_blocks_hybrid(page_blocks, label_regexes, page_images, page_sizes):
    """Label → value using text-layer neighbor first; fallback to OCR right/below (from #2)."""
    for page_idx, pb in enumerate(page_blocks):
        for i, b in enumerate(pb):
            low = norm_text((b["text"] or "").lower())
            for lr in label_regexes:
                if re.search(lr, low, re.IGNORECASE):
                    # 1) text-layer neighbor
                    val = find_next_nonempty(pb, i)
                    if val and norm_space(val):
                        return norm_space(val.replace("\n", " "))
                    # 2) OCR right
                    x0, y0, x1, y1 = b["rect"]
                    right_rect = (x1 + 2, y0 - 6, x1 + 360, y0 + 36)
                    ocr_right = _ocr_region_from_page(page_images[page_idx], page_sizes[page_idx], right_rect)
                    if norm_space(ocr_right):
                        return norm_space(ocr_right.replace("\n", " "))
                    # 3) OCR below
                    below_rect = (x0, y1 + 2, x0 + 420, y1 + 120)
                    ocr_below = _ocr_region_from_page(page_images[page_idx], page_sizes[page_idx], below_rect)
                    if norm_space(ocr_below):
                        return norm_space(ocr_below.replace("\n", " "))
    return ""

def find_global_regex(text, patterns):
    for pat in patterns:
        m = re.search(pat, text or "", re.IGNORECASE | re.DOTALL | re.MULTILINE)
        if m:
            return norm_space(m.group(1) if m.groups() else m.group(0))
    return ""

def slice_between(text, start_pat, end_pat):
    ms = re.search(start_pat, text or "", re.IGNORECASE | re.DOTALL | re.MULTILINE)
    if not ms:
        return ""
    start = ms.end()
    me = re.search(end_pat, (text or "")[start:], re.IGNORECASE | re.DOTALL | re.MULTILINE)
    return (text or "")[start:start + me.start()] if me else (text or "")[start:]

def kv_value_from_lines(section, label):
    m = re.search(fr"{label}\s*[:\-]\s*(.+)", section or "", re.IGNORECASE)
    if m: return norm_space(m.group(1))
    m = re.search(fr"{label}[^\n]*\n([^\n]+)", section or "", re.IGNORECASE)
    if m: return norm_space(m.group(1))
    return ""

def first_number(s):
    m = re.search(r"[\d,]+", s or "")
    return m.group(0) if m else ""

def yes_no_from_text_context(full_text, label_rgx, window=260):
    txt = full_text or ""
    for m in re.finditer(label_rgx, txt, re.IGNORECASE):
        start = m.end()
        ctx = txt[start:start+600]
        if re.search(r"\(\s*No\s*\)", ctx, re.IGNORECASE):
            return "No"
        if re.search(r"\(\s*Yes\s*\)", ctx, re.IGNORECASE):
            return "Yes"
        if re.search(r"\bNo\b", ctx[:window], re.IGNORECASE) and not re.search(r"\bYes\b", ctx[:window], re.IGNORECASE):
            return "No"
        if re.search(r"\bYes\b", ctx[:window], re.IGNORECASE) and not re.search(r"\bNo\b", ctx[:window], re.IGNORECASE):
            return "Yes"
    return ""

def value_right_or_below_hybrid(page_blocks, label_res, page_images, page_sizes):
    return find_label_value_blocks_hybrid(page_blocks, [label_res], page_images, page_sizes)

# ---------------- Section 7 helpers (from #2) ----------------
SECTION7_OPTIONS = [
    "Uncalled share capital",
    "Calls made but not paid",
    "Immovable property",
    "Movable property",
    "Floating charge",
    "Motor Vehicle",
    "Any property for securing the issue of secured deposits",
    "Goodwill",
    "Patent",
    "Licence under a patent",
    "Trade mark",
    "Copyright",
    "Book debts",
    "Ship",
    "Solely of Property situated outside India",
    "Others",
]

def detect_selected_7a(full_text, strict_selected=True):
    blob = (full_text or "").lower()
    picked = set()
    lien_hit = bool(re.search(r"lien\s+over\s+deposit|lien\s+on\s+fixed|fixed\s+deposit\s+advice|memorandum\s+of\s+charge", blob, re.IGNORECASE))
    hyp_hit  = "hypothecat" in blob

    if lien_hit or hyp_hit:
        picked.add("Others")

    if strict_selected and "Others" in picked:
        return "Others"

    if any(w in blob for w in ["book debt", "book debts", "receivable", "debtors", "current assets"]):
        picked.add("Book debts")
    if any(w in blob for w in ["immovable", "land", "building", "plot", "flat", "mortgage"]):
        picked.add("Immovable property")
    if any(w in blob for w in ["movable", "machinery", "vehicle"]):
        picked.add("Movable property")
    if "floating charge" in blob:
        picked.add("Floating charge")

    return ", ".join([x for x in SECTION7_OPTIONS if x in picked])

# ---------------- Robust Section 9 (from #2) ----------------
def parse_section9(full_text: str, page_blocks, page_images, page_sizes) -> dict:
    result = {
        "Category":"", "Name":"", "Address Line I":"", "Address Line II":"",
        "ISO Country Code":"", "City":"", "State":"", "Country":"", "Pin Code":"",
        "Email":"", "Has Valid PAN":"", "PAN":"", "Branch Code":""
    }
    sec9 = slice_between(full_text, r"(?:^|\n)\s*9[\.\(]\s*", r"(?:^|\n)\s*10[\.\(]") or ""

    def pull(label_regexes, kv_label_rgx=None, global_rgx=None):
        v = find_label_value_blocks_hybrid(page_blocks, label_regexes, page_images, page_sizes)
        if (not v) and kv_label_rgx:
            v = kv_value_from_lines(sec9, kv_label_rgx)
        if (not v) and global_rgx:
            v = find_global_regex(full_text, [global_rgx])
        return norm_space(v)

    result["Category"] = pull([r"\bcategory\b", r"category of the charge holder"], r"Category", r"Category\s*[:\-]\s*([^\n]+)")

    result["Name"] = pull([r"\bname\b", r"name of the charge holder", r"name of charge holder"],
                          r"Name", r"(?:Name|Name of the charge holder)\s*[:\-]\s*([^\n]+)")
    if not result["Name"]:
        g = re.search(r"(?:Charge\s*holder.*?\n)?([^\n]*bank[^\n]*limited)", sec9 or full_text, re.IGNORECASE)
        if g:
            result["Name"] = norm_space(g.group(1))

    # Address (line I/II or single blob)
    addr1 = pull([r"address\s*line\s*i\b", r"address\s*line\s*1\b"], r"Address\s*Line\s*I|Address\s*Line\s*1")
    addr2 = pull([r"address\s*line\s*ii\b", r"address\s*line\s*2\b"], r"Address\s*Line\s*II|Address\s*Line\s*2")
    if not addr1 and not addr2:
        blob = pull([r"\baddress\b(?!.*line)"], r"Address(?!.*Line)", r"Address\s*[:\-]\s*([^\n]+)")
        if blob:
            parts = [p.strip(" ,") for p in re.split(r",|\n", blob) if p.strip(" ,")]
            if parts:
                addr1 = parts[0]
                addr2 = parts[1] if len(parts) > 1 else ""
    result["Address Line I"]  = addr1
    result["Address Line II"] = addr2

    result["ISO Country Code"] = pull([r"iso\s*country\s*code"], r"ISO\s*country\s*code", r"ISO\s*country\s*code\s*[:\-]\s*([A-Z]{2})")
    result["City"]    = pull([r"\bcity\b"], r"City", r"City\s*[:\-]\s*([^\n]+)")
    result["State"]   = pull([r"\bstate\b"], r"State", r"State\s*[:\-]\s*([^\n]+)")
    result["Country"] = pull([r"\bcountry\b"], r"Country", r"Country\s*[:\-]\s*([^\n]+)")

    pin_val = pull([r"pin\s*code|pincode|pin\s*code"], r"Pin\s*code|Pin\s*Code|Pincode",
                   r"(?:Pin\s*Code|Pincode)\s*[:\-]\s*([0-9]{6})")
    if not re.fullmatch(r"\d{6}", pin_val or ""):
        m = re.search(r"\b\d{6}\b", (sec9 or full_text))
        pin_val = m.group(0) if m else pin_val
    result["Pin Code"] = pin_val

    em = pull([r"e-?mail\s*id|email\s*id|email"], r"e-?mail\s*id|email\s*id|email")
    if not EMAIL_RE.search(em or ""):
        m = EMAIL_RE.search(sec9 or full_text or "")
        em = m.group(0) if m else em
    result["Email"] = norm_space(em)

    hvp = ""
    if re.search(r"valid\s+income\s+tax\s+pan.*?\(\s*yes\s*\)", sec9, re.IGNORECASE):
        hvp = "Yes"
    elif re.search(r"valid\s+income\s+tax\s+pan.*?\(\s*no\s*\)", sec9, re.IGNORECASE):
        hvp = "No"
    else:
        m = re.search(r"(valid\s+income\s+tax\s+pan[^\n]{0,200})", sec9, re.IGNORECASE)
        if not m:
            m = re.search(r"(valid\s+income\s+tax\s+pan[^\n]{0,200})", full_text, re.IGNORECASE)
        if m:
            ctx = m.group(1)
            if re.search(r"\byes\b", ctx, re.IGNORECASE) and not re.search(r"\bno\b", ctx, re.IGNORECASE):
                hvp = "Yes"
            elif re.search(r"\bno\b", ctx, re.IGNORECASE) and not re.search(r"\byes\b", ctx, re.IGNORECASE):
                hvp = "No"
    result["Has Valid PAN"] = hvp

    pan = kv_value_from_lines(sec9, r"(?:Income\s*tax-)?\s*Permanent\s*Account\s*Number\s*\(PAN\)|\bPAN\b")
    if not pan:
        m = re.search(r"(?:Permanent\s*Account\s*Number\s*\(PAN\)|\bPAN\b)\s*[:\-]?\s*([A-Z0-9]{8,12})", sec9, re.IGNORECASE)
        if not m:
            m = re.search(r"(?:Permanent\s*Account\s*Number\s*\(PAN\)|\bPAN\b)\s*[:\-]?\s*([A-Z0-9]{8,12})", full_text, re.IGNORECASE)
        pan = m.group(1) if m else ""
    m = PAN_RE.search(pan or "")
    if m:
        pan = m.group(1)
    result["PAN"] = (pan or "").upper()

    bsr = kv_value_from_lines(sec9, r"(?:BSR\s*Code|Branch\s*Code|BSR\s*Code\s*/\s*Branch\s*Code)")
    if not bsr:
        m = re.search(r"(?:BSR\s*Code|Branch\s*Code)\s*[:\-]?\s*([A-Z0-9\-\/]{1,30})", sec9, re.IGNORECASE)
        bsr = m.group(1) if m else ""
    result["Branch Code"] = norm_space(bsr)

    return result

# ---------------- Schema ----------------
def mk_empty_record():
    return {
        "1(a) CIN":"", "1(b) GLN":"",
        "2(a) Company Name":"", "2(b) Address":"", "2(c) Email":"",
        "3(a) This form is for registration of":"",
        "4 Applicant Type":"",
        "5(a) Instrument Date":"", "5(b) Instrument Description":"", "5(c) Charge created outside India":"",
        "7(a) Type of Charge":"", "7(b) If others, specify":"",
        "8(a) Consortium Finance":"", "8(b) Joint Charge":"", "8(c) Number of Charge Holders":"",
        "9 Charge Holder":{
            "Category":"", "Name":"", "Address Line I":"", "Address Line II":"",
            "ISO Country Code":"", "City":"", "State":"", "Country":"", "Pin Code":"",
            "Email":"", "Has Valid PAN":"", "PAN":"", "Branch Code":""
        },
        "10(a) Amount Secured (Rs)":"", "10(b) Amount Secured (Words)":"", "10(c) Foreign Currency Details":"",
        "11(a) Date of Creating Security Interest":"", "11(b) Borrower's Account Number":"",
        "11(c) Rate of Interest":"", "11(d) Repayment Term":"", "11(e) Terms of Repayment":"",
        "11(f) Nature of Facility":"", "11(g) Date of Disbursement":"", "11(h) Miscellaneous Info":"",
        "11(i) Margin":"", "11(j) Extent of Charge":"", "11(k) Others":"",
        "12(a) Instrument Creation Date":"", "12(b) Instrument Description":"", "12(c) Property Acquisition Date":"",
        "12(d) Amount of Charge (Rs)":"", "12(e) Property Particulars":"",
        "13 Property Details":"",
        "14 Title Document":{"No. of Documents":"", "Details":""},
        "15(a) Property not in company name":"",
        "16 Date of Creation/Last Modification":""
    }

def flatten_record(rec: dict, file_name: str) -> dict:
    out = {"__file__": file_name}
    for k, v in rec.items():
        if isinstance(v, dict):
            for kk, vv in v.items():
                out[f"{k}__{kk}"] = vv
        else:
            out[k] = v
    return out

# ---------------- Parser ----------------
def parse_chg1_full(pdf_path: str) -> dict:
    full_text, blocks, page_images, page_sizes = extract_text_blocks_and_images(pdf_path)
    full_text = norm_text(full_text)
    text_low = (full_text or "").lower()

    data = mk_empty_record()

    # ====== Sections 1–9 (from Codebase #2) ======

    # 1. CIN / GLN
    m = re.search(r"\bCIN\b.*?([A-Z0-9]{21})", full_text or "", re.IGNORECASE | re.DOTALL)
    data["1(a) CIN"] = m.group(1) if m else ""
    m = re.search(r"\bGLN\b.*?([A-Z0-9]+)\b", full_text or "", re.IGNORECASE | re.DOTALL)
    data["1(b) GLN"] = m.group(1) if m else ""

    # 2. Company name / address / email
    name_val = find_label_value_blocks_hybrid(blocks, [r"\bname of the company\b", r"\bcompany name\b", r"\bname\s*of\s*company\b"], page_images, page_sizes)
    if not name_val:
        name_val = find_global_regex(full_text, [
            r"2\(?\s*a\)?[^\n]*\n(.*?)(?=\n\s*2\(?\s*b\)?)",
            r"\bname of the company\b.*?\n(.*)"
        ])
    name_val = re.sub(r"^\(?b\)?\s*address.*", "", name_val or "", flags=re.IGNORECASE).strip()
    data["2(a) Company Name"] = norm_space(name_val)

    addr_val = find_label_value_blocks_hybrid(blocks, [r"\baddress\b", r"\bregistered office address\b", r"\baddress of the registered office\b"], page_images, page_sizes)
    if not addr_val:
        addr_val = find_global_regex(full_text, [r"2\(?\s*b\)?[^\n]*\n(.*?)(?=\n\s*2\(?\s*c\)?)"])
    data["2(b) Address"] = norm_space(addr_val)

    email_val = find_label_value_blocks_hybrid(blocks, [r"\bemail\b", r"\be-?mail\b"], page_images, page_sizes)
    if not email_val or not EMAIL_RE.search(email_val or ""):
        em = EMAIL_RE.search(full_text or "")
        email_val = em.group(0) if em else email_val
    data["2(c) Email"] = norm_space(email_val)

    # 3. Registration type
    if re.search(r"creation of\s*charge", text_low or ""):
        data["3(a) This form is for registration of"] = "Creation of charge"
    elif re.search(r"modification of\s*charge", text_low or ""):
        data["3(a) This form is for registration of"] = "Modification of charge"
    elif re.search(r"rectification of\s*charge", text_low or ""):
        data["3(a) This form is for registration of"] = "Rectification of charge"

    # 4. Applicant Type
    if re.search(r"whether the applicant.*the company", text_low or "", re.DOTALL):
        data["4 Applicant Type"] = "The Company"
    elif re.search(r"whether the applicant.*the charge holder", text_low or "", re.DOTALL):
        data["4 Applicant Type"] = "The charge holder"

    # 5. Date / description / outside India
    five_a = find_global_regex(full_text, [
        r"5\.\s*\(a\).*?date.*?(\d{1,2}[^\w]\d{1,2}[^\w]\d{2,4})",
        r"date of the instrument.*?(\d{1,2}[^\w]\d{1,2}[^\w]\d{2,4})"
    ]) or value_right_or_below_hybrid(blocks, r"5\.\s*\(a\)\s*date\s+of\s+the\s+instrument|date\s+of\s+the\s+instrument\s+creating", page_images, page_sizes)
    data["5(a) Instrument Date"] = norm_date(five_a)

    five_b = find_global_regex(full_text, [
        r"5\.\s*\(b\).*?(?:Nature.*?particulars[^\n]*\n)(.*?)(?=\n\s*5\.\s*\(c\)|\n\s*7[\.\(])"
    ]) or value_right_or_below_hybrid(blocks, r"5\.\s*\(b\)\s*nature.*description|nature,\s*description\s*and\s*brief\s*particulars", page_images, page_sizes)
    if not norm_space(five_b):
        five_b = find_global_regex(full_text, [
            r"(Sanction\s+Letter.*?dated\s+[0-9/\-\.]+[^\n]*)",
            r"(Memorandum\s+of\s+Charge[^\n]*)",
            r"(Lien\s+over\s+Deposits[^\n]*)"
        ])
    data["5(b) Instrument Description"] = norm_space(five_b)

    # *** FIXED single-line assignment ***
    data["5(c) Charge created outside India"] = yes_no_from_text_context(
        full_text,
        r"5\.\s*\(c\)\s*whether\s+charge\s+is\s+created\s+or\s+modified\s+outside\s+india"
    ) or "No"

    # 7. Type of charge (detector from #2)
    data["7(a) Type of Charge"] = detect_selected_7a(full_text, strict_selected=True)
    secl7 = slice_between(full_text, r"(?:^|\n)\s*7[\.\(]", r"(?:^|\n)\s*8[\.\(]") or ""
    others_val = (kv_value_from_lines(secl7, r"If others, specify")
                  or find_global_regex(full_text, [r"7\(\s*b\s*\).*?(?:If others, specify)?\s*([^\n]+)"]))
    if not norm_space(others_val):
        if re.search(r"lien\s+over\s+deposit|lien\s+on\s+fixed|fixed\s+deposit\s+advice|memorandum\s+of\s+charge", text_low or "", re.IGNORECASE):
            others_val = "Lien on Fixed Deposit"
        elif "hypothecat" in (text_low or ""):
            others_val = "Hypothecation"
    data["7(b) If others, specify"] = norm_space(others_val)
    if data["7(b) If others, specify"] and ("Others" not in (data["7(a) Type of Charge"] or "")):
        data["7(a) Type of Charge"] = (data["7(a) Type of Charge"] + (", " if data["7(a) Type of Charge"] else "") + "Others").strip(",")

    # 8. Consortium / Joint / Number
    v8a = value_right_or_below_hybrid(blocks, r"8\.\s*\(a\)\s*whether\s+consortium\s+finance\s+is\s+involved", page_images, page_sizes)
    yn8a = ""
    if re.search(r"\(\s*No\s*\)", v8a or "", re.IGNORECASE) or re.search(r"\bNo\b", v8a or "", re.IGNORECASE):
        yn8a = "No"
    elif re.search(r"\(\s*Yes\s*\)", v8a or "", re.IGNORECASE) or re.search(r"\bYes\b", v8a or "", re.IGNORECASE):
        yn8a = "Yes"
    if not yn8a:
        yn8a = yes_no_from_text_context(full_text, r"8\.\s*\(a\)\s*whether\s+consortium\s+finance\s+is\s+involved") or "No"
    data["8(a) Consortium Finance"] = yn8a

    v8b = value_right_or_below_hybrid(blocks, r"8\.\s*\(b\)\s*whether\s+joint\s+charge\s+is\s+involved", page_images, page_sizes)
    yn8b = ""
    if re.search(r"\(\s*No\s*\)", v8b or "", re.IGNORECASE) or re.search(r"\bNo\b", v8b or "", re.IGNORECASE):
        yn8b = "No"
    elif re.search(r"\(\s*Yes\s*\)", v8b or "", re.IGNORECASE) or re.search(r"\bYes\b", v8b or "", re.IGNORECASE):
        yn8b = "Yes"
    if not yn8b:
        yn8b = yes_no_from_text_context(full_text, r"8\.\s*\(b\)\s*whether\s+joint\s+charge\s+is\s+involved") or "No"
    data["8(b) Joint Charge"] = yn8b

    sec8 = slice_between(full_text, r"(?:^|\n)\s*8[\.\(]", r"(?:^|\n)\s*9[\.\(]") or ""
    m = re.search(r"Number of charge holder\(s\)\s*[\*:]*\s*(\d+)", sec8, re.IGNORECASE)
    if not m:
        nval = value_right_or_below_hybrid(blocks, r"number\s+of\s+charge\s+holder\(s\)", page_images, page_sizes)
        m = re.search(r"\d+", nval or "")
    data["8(c) Number of Charge Holders"] = m.group(0) if m else ""

    # 9. Charge Holder (robust from #2)
    ch = parse_section9(full_text, blocks, page_images, page_sizes)
    data["9 Charge Holder"].update(ch)

    # ====== Sections 10–16 (from Codebase #1; your preferred “after 9”) ======

    # 10
    amount_rs = find_label_value_blocks_hybrid(blocks, [r"amount secured.*rs"], page_images, page_sizes)
    data["10(a) Amount Secured (Rs)"] = re.sub(r"[^\d]", "", amount_rs or "")

    amount_words = find_label_value_blocks_hybrid(blocks, [r"amount secured.*words"], page_images, page_sizes)
    data["10(b) Amount Secured (Words)"] = amount_words

    data["10(c) Foreign Currency Details"] = find_label_value_blocks_hybrid(blocks, [r"foreign currency"], page_images, page_sizes)

    # 11
    data["11(a) Date of Creating Security Interest"] = norm_date(
        find_label_value_blocks_hybrid(blocks, [r"date of creating security interest"], page_images, page_sizes)
    )
    data["11(b) Borrower's Account Number"] = find_label_value_blocks_hybrid(blocks, [r"borrower.*account number"], page_images, page_sizes)
    data["11(c) Rate of Interest"] = find_label_value_blocks_hybrid(blocks, [r"rate of interest"], page_images, page_sizes)
    data["11(d) Repayment Term"] = find_label_value_blocks_hybrid(blocks, [r"repayment term"], page_images, page_sizes)
    data["11(e) Terms of Repayment"] = find_label_value_blocks_hybrid(blocks, [r"terms of repayment"], page_images, page_sizes)
    data["11(f) Nature of Facility"] = find_label_value_blocks_hybrid(blocks, [r"nature of facility"], page_images, page_sizes)
    data["11(g) Date of Disbursement"] = norm_date(
        find_label_value_blocks_hybrid(blocks, [r"date of disbursement"], page_images, page_sizes)
    )
    data["11(h) Miscellaneous Info"] = find_label_value_blocks_hybrid(blocks, [r"miscellaneous"], page_images, page_sizes)
    data["11(i) Margin"] = find_label_value_blocks_hybrid(blocks, [r"margin"], page_images, page_sizes)
    data["11(j) Extent of Charge"] = find_label_value_blocks_hybrid(blocks, [r"extent.*operation.*charge"], page_images, page_sizes)
    data["11(k) Others"] = find_label_value_blocks_hybrid(blocks, [r"others.*11"], page_images, page_sizes)

    # 12
    data["12(a) Instrument Creation Date"] = norm_date(
        find_label_value_blocks_hybrid(blocks, [r"date of instrument.*creating.*charge"], page_images, page_sizes)
    )
    data["12(b) Instrument Description"] = find_label_value_blocks_hybrid(blocks, [r"description.*instrument.*creating"], page_images, page_sizes)
    data["12(c) Property Acquisition Date"] = norm_date(
        find_label_value_blocks_hybrid(blocks, [r"date of acquisition.*property"], page_images, page_sizes)
    )
    amount_charge = find_label_value_blocks_hybrid(blocks, [r"amount of the charge"], page_images, page_sizes)
    data["12(d) Amount of Charge (Rs)"] = re.sub(r"[^\d]", "", amount_charge or "")
    data["12(e) Property Particulars"] = find_label_value_blocks_hybrid(blocks, [r"particulars.*property.*charged"], page_images, page_sizes)

    # 13
    data["13 Property Details"] = find_label_value_blocks_hybrid(blocks, [r"short particulars.*property"], page_images, page_sizes)

    # 14
    data["14 Title Document"]["No. of Documents"] = find_label_value_blocks_hybrid(blocks, [r"number of title documents"], page_images, page_sizes)
    data["14 Title Document"]["Details"] = find_label_value_blocks_hybrid(blocks, [r"description.*document.*acquired.*title"], page_images, page_sizes)

    # 15
    data["15(a) Property not in company name"] = yes_no_from_text_context(full_text, r"whether any of the property.*not registered.*name of the company")

    # 16
    data["16 Date of Creation/Last Modification"] = norm_date(
        find_label_value_blocks_hybrid(blocks, [r"date of creation.*last modification"], page_images, page_sizes)
    )

    # Clean blanks
    data = {k: v for k, v in data.items() if (v not in [None, "", " "] if not isinstance(v, dict) else True)}
    # Also clean nested dicts (Section 9, 14 Title Doc)
    if "9 Charge Holder" in data and isinstance(data["9 Charge Holder"], dict):
        data["9 Charge Holder"] = {k: v for k, v in data["9 Charge Holder"].items() if v not in [None, "", " "]}
    if "14 Title Document" in data and isinstance(data["14 Title Document"], dict):
        data["14 Title Document"] = {k: v for k, v in data["14 Title Document"].items() if v not in [None, "", " "]}

    return data

# ---------------- Runner ----------------
def run_one(pdf_path: str) -> dict:
    return parse_chg1_full(pdf_path)

def run_batch():
    rows = []
    paths = []
    if os.path.isdir(INPUT_PATH):
        for f in sorted(os.listdir(INPUT_PATH)):
            if f.lower().endswith(".pdf"):
                paths.append(os.path.join(INPUT_PATH, f))
    else:
        paths.append(INPUT_PATH)

    for p in paths:
        try:
            rec = run_one(p)
            rows.append(flatten_record(rec, os.path.basename(p)))
            print(f"Parsed: {os.path.basename(p)}")
        except Exception as e:
            print(f"❌ Error parsing {os.path.basename(p)}: {e}")

    if not rows:
        print("No PDFs parsed.")
        return

    df = pd.DataFrame(rows)
    csv_path  = f"{OUTPUT_BASE}.csv"
    xlsx_path = f"{OUTPUT_BASE}.xlsx"
    json_path = f"{OUTPUT_BASE}.json"

    if APPEND_MODE and os.path.exists(csv_path):
        old = pd.read_csv(csv_path)
        df = pd.concat([old, df], ignore_index=True)

    df.to_csv(csv_path, index=False, encoding="utf-8")
    try:
        df.to_excel(xlsx_path, index=False)  # requires openpyxl
        print("XLSX ->", xlsx_path)
    except Exception as e:
        print(f"⚠️ Could not write Excel (install openpyxl). Error: {e}")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)

    print("✅ Saved outputs:")
    print("CSV  ->", csv_path)
    print("JSON ->", json_path)
    if HAVE_TESS:
        print("🔎 OCR enabled via Tesseract; region OCR used near labels.")
    else:
        print("ℹ️ OCR not available (Tesseract not found). Using text-layer + heuristics only.")

if __name__ == "__main__":
    run_batch()
