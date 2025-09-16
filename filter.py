
import os, re, io, json, argparse, shutil, subprocess, unicodedata
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import fitz  # PyMuPDF
import pandas as pd

try:
    from PIL import Image, ImageOps, ImageFilter
except Exception:
    Image = None

try:
    import pytesseract
except Exception:
    pytesseract = None

# ----------- DEFAULT PATHS (edit these if you want built-in defaults) -----------
DEFAULT_INPUT = "pdfs/"          # <— your default input folder with CHG-1 PDFs
DEFAULT_OUTDIR = "out_json"      # default per-file JSON output folder
DEFAULT_OUTBASE = "chg1_master"  # default basename for master CSV/XLSX/JSON
# -------------------------------------------------------------------------------

# ---------------- OCR INIT ----------------

def init_ocr(tesseract_cmd: Optional[str]) -> Tuple[bool, str]:
    """Configure pytesseract to a specific binary; return (enabled, msg)."""
    if pytesseract is None:
        return (False, "pytesseract not installed")
    cmd = tesseract_cmd or shutil.which("tesseract")
    if not cmd or not Path(cmd).exists():
        return (False, "tesseract binary not found")
    pytesseract.pytesseract.tesseract_cmd = cmd
    try:
        v = subprocess.check_output([cmd, "--version"], text=True).splitlines()[0]
    except Exception:
        v = "tesseract (version unknown)"
    return (True, f"{v} @ {cmd}")

# ---------------- Utilities ----------------
EMAIL_RE = re.compile(r"[\w\.-]+@[\w\.-]+\.\w+")
PAN_RE   = re.compile(r"\b([A-Z]{5}[0-9]{4}[A-Z])\b")
PIN_RE   = re.compile(r"\b(\d{6})\b")
DATE_RE  = re.compile(r"(\d{2}/\d{2}/\d{4})")

INDIA_STATES = {
    "ANDHRA PRADESH":"AP","ARUNACHAL PRADESH":"AR","ASSAM":"AS","BIHAR":"BR","CHHATTISGARH":"CG",
    "GOA":"GA","GUJARAT":"GJ","HARYANA":"HR","HIMACHAL PRADESH":"HP","JHARKHAND":"JH",
    "KARNATAKA":"KA","KERALA":"KL","MADHYA PRADESH":"MP","MAHARASHTRA":"MH","MANIPUR":"MN",
    "MEGHALAYA":"ML","MIZORAM":"MZ","NAGALAND":"NL","ODISHA":"OD","PUNJAB":"PB",
    "RAJASTHAN":"RJ","SIKKIM":"SK","TAMIL NADU":"TN","TELANGANA":"TS","TRIPURA":"TR",
    "UTTAR PRADESH":"UP","UTTARAKHAND":"UK","WEST BENGAL":"WB","DELHI":"DL","JAMMU & KASHMIR":"JK",
    "LADAKH":"LD","PUDUCHERRY":"PY","CHANDIGARH":"CH","ANDAMAN AND NICOBAR ISLANDS":"AN",
    "DADRA AND NAGAR HAVELI AND DAMAN AND DIU":"DN","LAKSHADWEEP":"LD"
}
STATE_REGEX = re.compile(r"\b(" + "|".join(map(re.escape, INDIA_STATES.keys())) + r")\b", re.I)

def norm(s:str)->str:
    return re.sub(r"\s+"," ", (s or "")).strip()

def first(text, patterns, flags=re.I|re.S, group=1) -> str:
    for pat in patterns:
        m = re.search(pat, text or "", flags)
        if m:
            try:  return norm(m.group(group))
            except IndexError: return norm(m.group(0))
    return ""

def cut(text, start_pats, end_pats) -> str:
    sp = -1
    for p in start_pats:
        m = re.search(p, text, re.I|re.M)
        if m: sp = m.start(); break
    if sp < 0: return ""
    sub = text[sp:]
    ep = len(sub)
    for p in end_pats:
        m2 = re.search(p, sub, re.I|re.M)
        if m2 and m2.start() > 5:
            ep = m2.start(); break
    return sub[:ep]

def read_text_blocks(pdf_path: Path, ocr_enabled: bool, ocr_lang: str, ocr_dpi: int, min_text_len: int):
    doc = fitz.open(str(pdf_path))
    pages = []
    for p in doc:
        txt  = (p.get_text("text", sort=True) or "").replace("\r","")
        if ocr_enabled and Image is not None and len(txt.strip()) < min_text_len:
            try:
                pix = p.get_pixmap(dpi=ocr_dpi)
                img = Image.open(io.BytesIO(pix.tobytes("png")))
                img = ImageOps.grayscale(img)
                img = ImageOps.autocontrast(img)
                img = img.filter(ImageFilter.UnsharpMask(radius=1, percent=150, threshold=3))
                ocr = pytesseract.image_to_string(img, lang=ocr_lang) or ""
                if len(ocr.strip()) > len(txt.strip()):
                    txt = ocr
            except Exception:
                pass
        rawb =  p.get_text("blocks") or []
        blks = [{"rect":fitz.Rect(b[:4]), "text": (b[4] or "").replace("\r","")} for b in rawb]
        pages.append({"page": p.number, "text": txt, "blocks": blks})
    doc.close()
    return pages

def near_block_text(blocks, label_regex, direction="right", max_dx=320, max_dy=48) -> str:
    lbl=None
    for b in blocks:
        if re.search(label_regex, b["text"], re.I):
            lbl=b; break
    if not lbl: return ""
    lx0,ly0,lx1,ly1 = lbl["rect"]
    best=""
    for b in blocks:
        x0,y0,x1,y1 = b["rect"]
        if direction=="right":
            same_line = abs((y0+y1)/2 - (ly0+ly1)/2) < max_dy
            right = x0 >= lx1 and (x0-lx1) < max_dx
            if right and same_line:
                if len(b["text"]) > len(best): best=b["text"]
        else:
            below = y0 >= ly1 and (y0-ly1)<max_dy and abs(x0-lx0)<max_dx
            if below:
                if len(b["text"]) > len(best): best=b["text"]
    return norm(best)

# ---------------- Schema ----------------

def empty_record()->Dict:
    return {
      "1": {"corporate_identity_number_cin": "", "global_location_number_gln": None},
      "2": {"name_of_the_company": "", "address_of_the_registered_office": "", "email_id_of_the_company": ""},
      "3": {"form_for_registration_of": ""},
      "4": {"applicant_is": ""},
      "5": {"date_of_instrument_creating_or_modifying_charge": "", "nature_description_of_instrument": "", "charge_created_or_modified_outside_india": ""},
      "6": None,
      "7": {"charge_on": [], "others_specify": ""},
      "8": {"consortium_finance_involved": "", "joint_charge_involved": "", "number_of_charge_holders": ""},
      "9": {
        "category": "",
        "name": "",
        "address": {"line1": "", "line2": "", "city": "", "state": "", "country": "", "pin_code": "", "iso_country_code": ""},
        "email_id": "",
        "has_pan": "",
        "pan": "",
        "bsr_code_branch_code": None
      },
      "10": {"amount_secured_by_charge_in_rs": "", "amount_secured_by_charge_in_words": "", "amount_in_foreign_currency_details": None},
      "11": {"date_of_creating_security_interest": "", "borrower_customer_account_number": None, "rate_of_interest": "", "repayment_term_in_months": "", "terms_of_repayment": None, "nature_of_facility": "", "date_of_disbursement": None, "miscellaneous_narrative_information": None, "margin": "", "extent_and_operation_of_the_charge": "", "others": None},
      "12": {"date_of_instrument_creating_or_evidencing_the_charge": None,"description_of_the_instrument_creating_or_evidencing_the_charge": None,"date_of_acquisition_of_the_property": None,"amount_of_the_charge_in_rs": None,"particulars_of_the_property_charged": None},
      "13": {"short_particulars_of_property_or_assets_charged": ""},
      "14": {"number_of_title_documents_deposited_by_customer": None},
      "15": {"is_property_not_registered_in_company_name": ""},
      "16": {"date_of_creation_or_last_modification_prior_to_present": None}
    }

# ---------------- Parser ----------------

def parse_pdf(pdf_path: Path, ocr_enabled: bool=False, ocr_lang: str="eng", ocr_dpi: int=300, min_text_len: int=150) -> Dict:
    pages = read_text_blocks(pdf_path, ocr_enabled, ocr_lang, ocr_dpi, min_text_len)
    full = "\n".join(p["text"] for p in pages)
    rec = empty_record()

    # Sections 1 & 2
    rec["1"]["corporate_identity_number_cin"] = first(full,[r"\bCIN\s*[:\-]?\s*([A-Z0-9]{21})", r"\b(L\d{5}[A-Z]{2}\d{4}PLC\d{6})\b"]) or rec["1"]["corporate_identity_number_cin"]
    rec["2"]["name_of_the_company"] = first(full,[r"Name of the company\s*([^\n]+)"])
    addr = first(full,[r"Address of the registered office.*?\n(.+?)\n\s*\d+\.", r"Address of the registered office.*?\n(.+?)\n\s*email id of the company"])
    if addr and "email id of the company" in addr: addr = addr.split("email id of the company")[0].strip()
    rec["2"]["address_of_the_registered_office"] = addr.strip()
    rec["2"]["email_id_of_the_company"] = first(full, [r"email id of the company\s*([^\n]+)", EMAIL_RE.pattern])

    # Sections 3 & 4
    if re.search(r"Creation of\s*charge", full, re.I): rec["3"]["form_for_registration_of"] = "Creation of charge"
    elif re.search(r"Modification of\s*charge", full, re.I): rec["3"]["form_for_registration_of"] = "Modification of charge"
    sec4 = cut(full,[r"^\s*4\s*\."],[r"^\s*5\s*\."])
    if re.search(r"The Company", sec4, re.I): rec["4"]["applicant_is"]="The Company"
    elif re.search(r"The charge holder", sec4, re.I): rec["4"]["applicant_is"]="The charge holder"

    # Section 5
    sec5 = cut(full,[r"^\s*5\s*\."],[r"^\s*6\s*\.", r"^\s*7\s*\."])
    rec["5"]["date_of_instrument_creating_or_modifying_charge"] = first(sec5,[DATE_RE.pattern]) or rec["5"]["date_of_instrument_creating_or_modifying_charge"]
    rec["5"]["nature_description_of_instrument"] = first(sec5,[r"(?:Nature, description.*?\n|instrument\(s\).+?\n)(.+?)(?:\n\s*(?:Whether|charge is created|7\.)|$)"])
    if re.search(r"outside India.*?Yes", sec5, re.I|re.S): rec["5"]["charge_created_or_modified_outside_india"]="Yes"
    elif re.search(r"outside India.*?No", sec5, re.I|re.S): rec["5"]["charge_created_or_modified_outside_india"]="No"

    # Section 7
    sec7 = cut(full,[r"^\s*7\s*\."],[r"^\s*8\s*\.", r"^\s*9\s*\."])
    types = ["Immovable property","Movable property","Floating charge","Book debts","Motor Vehicle","Others"]
    sel = [t for t in types if re.search(re.escape(t), sec7, re.I)]
    if "Others" in sel: sel=["Others"]
    rec["7"]["charge_on"]=sel
    rec["7"]["others_specify"] = first(sec7,[r"If others, specify\s*([^\n]+)", r"Others\s*[:\-]?\s*([^\n]+)"])
    if not rec["7"]["others_specify"]:
        m = re.search(r"(Lien on Fixed Deposit[s]?(?:\s*\(s\))?)", full, re.I)
        if m: rec["7"]["others_specify"]=m.group(1).replace("Deposit[s]","Deposit")

    # Section 8
    sec8 = cut(full,[r"^\s*8\s*\."],[r"^\s*9\s*\.", r"^\s*10\s*\."])
    if re.search(r"consortium finance.*?Yes", sec8, re.I|re.S): rec["8"]["consortium_finance_involved"]="Yes"
    if re.search(r"consortium finance.*?No", sec8, re.I|re.S): rec["8"]["consortium_finance_involved"]="No"
    if re.search(r"joint charge.*?Yes", sec8, re.I|re.S): rec["8"]["joint_charge_involved"]="Yes"
    if re.search(r"joint charge.*?No", sec8, re.I|re.S): rec["8"]["joint_charge_involved"]="No"
    rec["8"]["number_of_charge_holders"] = first(sec8,[r"Number of charge holder\(s\)\s*([0-9]+)"]) or rec["8"]["number_of_charge_holders"]

    # Section 9
    sec9 = cut(full,[r"^\s*9\s*\.?\s*Particulars of the charge holder", r"^\s*9\s*\."],[r"^\s*10\s*\.?", r"^\s*10\s*\(a\)"])
    if sec9:
        name_tmp = first(sec9,[r"\bName\s*\*?\s*([^\n]+)"])
        category = first(sec9,[r"Category\s*([^\n]+)"])
        if name_tmp and "BANK" in name_tmp.upper(): category = "Scheduled bank"
        rec["9"]["category"] = category or rec["9"]["category"]
        rec["9"]["name"] = name_tmp or rec["9"]["name"]
        em = EMAIL_RE.search(sec9);  rec["9"]["email_id"] = em.group(0) if em else rec["9"]["email_id"]

        pan_ctx = first(sec9,[r"valid Income Tax PAN\s*([^\n]+)"])
        if "Yes" in pan_ctx: rec["9"]["has_pan"]="Yes"
        elif "No" in pan_ctx: rec["9"]["has_pan"]="No"
        pm = PAN_RE.search(sec9); rec["9"]["pan"] = pm.group(1) if pm else rec["9"]["pan"]
        rec["9"]["bsr_code_branch_code"] = first(sec9,[r"BSR\s*Code\s*/\s*Branch\s*Code\s*[:\-]?\s*([^\n]+)"]) or rec["9"]["bsr_code_branch_code"]

        addrI = first(sec9,[r"Address\s*(?:Line\s*I|Line I)\s*[:\-]?\s*([^\n]+)"])
        addrII= first(sec9,[r"Line\s*II\s*[:\-]?\s*([^\n]+)"])
        if addrI: rec["9"]["address"]["line1"]=addrI
        if addrII: rec["9"]["address"]["line2"]=addrII
        city    = first(sec9,[r"\*?\s*City\s*[:\-]?\s*([^\n]+)"])
        state   = first(sec9,[r"\*?\s*State\s*[:\-]?\s*([^\n]+)"])
        country = first(sec9,[r"\*?\s*Country\s*[:\-]?\s*([^\n]+)"])
        pin_    = first(sec9,[r"\*?\s*Pin\s*code\s*[:\-]?\s*(\d{6})"]) or (PIN_RE.search(sec9).group(1) if PIN_RE.search(sec9) else "")
        if not country and re.search(r"\bIndia\b", sec9, re.I): country = "INDIA"
        if state and "-" not in state:
            su = state.upper()
            code = INDIA_STATES.get(su, "")
            state = f"{state}-{code}" if code else state
        rec["9"]["address"]["city"]=(city or "").upper()
        rec["9"]["address"]["state"]=state or rec["9"]["address"]["state"]
        rec["9"]["address"]["country"]=country or rec["9"]["address"]["country"]
        rec["9"]["address"]["pin_code"]=pin_ or rec["9"]["address"]["pin_code"]
        if (rec["9"]["address"]["country"] or "").lower() in ("india","republic of india","bharat"):
            rec["9"]["address"]["iso_country_code"]="IN"

    # Section 10
    sec10 = cut(full,[r"^\s*10\s*\.?"],[r"^\s*11\s*\.?"])
    page2 = next((p for p in pages if p["page"]==1), pages[0])
    val10a = near_block_text(page2["blocks"], r"Amount\s+secured\s+by\s+the\s+charge.*?\(in\s*Rs", "right")
    if not re.search(r'\d', val10a or ''):
        val10a = first(sec10, [r"charge\s*\(in\s*Rs\.?\)\s*([\d,]+)"])
    if not re.search(r'\d', val10a or ''):
        m = re.search(r"fixed at\s*Rs\.?\s*([\d,]+)", full, re.I)
        if m: val10a = m.group(1)
    if val10a:
        rec["10"]["amount_secured_by_charge_in_rs"] = val10a.replace(",","")
    rec["10"]["amount_secured_by_charge_in_words"] = first(sec10, [r"Amount secured .*?in words\s*([^\n]+)", r"in words\s*([^\n]+)"])

    # Section 11
    sec11 = cut(full,[r"^\s*11\s*\.?"],[r"^\s*12\s*\.?", r"^\s*13\s*\."])
    rec["11"]["date_of_creating_security_interest"] = first(sec11,[r"Date of Creating Security Interest.*?(\d{2}/\d{2}/\d{4})"]) or rec["11"]["date_of_creating_security_interest"]
    rec["11"]["rate_of_interest"] = first(sec11,[r"Rate of interest\s*([^\n]+)"])
    rec["11"]["repayment_term_in_months"] = first(sec11,[r"Repayment term.*?\(in months\)\s*([^\n]+)", r"Repayment\s*term\s*[:\-]?\s*([^\n]+)"])
    rec["11"]["nature_of_facility"] = first(sec11,[r"Nature of facility\s*([^\n]+)"])
    rec["11"]["margin"] = first(sec11,[r"\bMargin\s*[:\-]?\s*([^\n]+)"])
    rec["11"]["extent_and_operation_of_the_charge"] = first(sec11,[r"Extent and operation of the charge\s*([\s\S]+?)(?:\n\s*\(k\)|\n\s*Others|\Z)"])

    # Section 13
    sec13 = cut(full,[r"^\s*13\s*\.?"],[r"^\s*14\s*\.", r"^\s*15\s*\."])
    rec["13"]["short_particulars_of_property_or_assets_charged"] = first(sec13,[r"Short particulars.*?\n([^\n]+)", r"(KND\s*[^\n]+)"])

    # Section 15
    sec15 = cut(full,[r"^\s*15\s*\.?"],[r"^\s*16\s*\."])
    if re.search(r"not registered.*?Yes", sec15, re.I|re.S): rec["15"]["is_property_not_registered_in_company_name"]="Yes"
    if re.search(r"not registered.*?No",  sec15, re.I|re.S): rec["15"]["is_property_not_registered_in_company_name"]="No"

    # Post: default 11.date to 5.date
    if not rec["11"]["date_of_creating_security_interest"] and rec["5"]["date_of_instrument_creating_or_modifying_charge"]:
        rec["11"]["date_of_creating_security_interest"] = rec["5"]["date_of_instrument_creating_or_modifying_charge"]

    return rec

# ---------------- Flatten for master ----------------

def flatten_for_tabular(o: dict, fname: str) -> dict:
    flat = {"__file__": fname}
    for k, v in o.items():
        if isinstance(v, dict):
            for kk, vv in v.items():
                if isinstance(vv, dict):
                    for kkk, vvv in vv.items():
                        flat[f"{k}.{kk}.{kkk}"] = vvv
                else:
                    flat[f"{k}.{kk}"] = vv
        else:
            flat[k] = v
    return flat

# ---------------- Diff/Validation ----------------

def deep_compare(a, b, path=""):
    diffs = []
    if isinstance(a, dict) and isinstance(b, dict):
        keys = set(a.keys())|set(b.keys())
        for k in sorted(keys):
            diffs += deep_compare(a.get(k), b.get(k), path+f".{k}" if path else k)
    elif isinstance(a, list) and isinstance(b, list):
        if len(a)!=len(b) or any(x!=y for x,y in zip(a,b)):
            diffs.append((path, a, b))
    else:
        if (a or "") != (b or ""):
            diffs.append((path, a, b))
    return diffs

# ---------------- CLI ----------------

def main():
    ap = argparse.ArgumentParser(description="CHG-1 Gold Extractor")
    # MADE OPTIONAL: --input now has a default; CLI still overrides
    ap.add_argument("--input", default=DEFAULT_INPUT, help="PDF file or folder (default: %(default)s)")
    ap.add_argument("--outdir", default=DEFAULT_OUTDIR, help="Folder for per-file JSONs (default: %(default)s)")
    ap.add_argument("--outbase", default=DEFAULT_OUTBASE, help="Basename for master CSV/XLSX/JSON (default: %(default)s)")
    ap.add_argument("--gt_json_dir", default=None, help="Optional folder with ground-truth JSONs (same basenames) for validation")
    ap.add_argument("--tesseract-cmd", default=None, help="Path to Tesseract binary (optional)")
    ap.add_argument("--ocr-lang", default="eng", help="OCR language")
    ap.add_argument("--ocr-dpi", type=int, default=300, help="OCR render DPI")
    ap.add_argument("--min-text-len", type=int, default=150, help="Below this, page is treated as sparse and OCR is attempted")
    args = ap.parse_args()

    # OCR init
    ocr_enabled = False
    ocr_msg = ""
    if args.tesseract_cmd or shutil.which("tesseract"):
        ok, msg = init_ocr(args.tesseract_cmd)
        ocr_enabled, ocr_msg = ok, msg
    print("OCR:", ("ENABLED - " + ocr_msg) if ocr_enabled else "DISABLED (text layer only)")

    in_path = Path(args.input or DEFAULT_INPUT)
    out_dir = Path(args.outdir); out_dir.mkdir(parents=True, exist_ok=True)

    if not in_path.exists():
        print(f"❗ Input path does not exist: {in_path.resolve()}")
        print("    • Edit DEFAULT_INPUT at top of the script OR pass --input /path/to/pdfs")
        return

    pdfs = []
    if in_path.is_dir():
        for f in sorted(in_path.iterdir()):
            if f.suffix.lower()==".pdf": pdfs.append(f)
    elif in_path.is_file() and in_path.suffix.lower()==".pdf":
        pdfs.append(in_path)

    if not pdfs:
        print(f"No PDFs found under: {in_path.resolve()}")
        return

    master_records = []
    flat_rows = []
    diff_rows = []

    for p in pdfs:
        try:
            rec = parse_pdf(p, ocr_enabled=ocr_enabled, ocr_lang=args.ocr_lang, ocr_dpi=args.ocr_dpi, min_text_len=args.min_text_len)
            # Save JSON
            out_json = out_dir / (p.stem + ".json")
            out_json.write_text(json.dumps(rec, indent=2, ensure_ascii=False))

            master_records.append({"__file__": p.name, **rec})
            flat_rows.append(flatten_for_tabular(rec, p.name))

            # Validation if GT available
            if args.gt_json_dir:
                gt_path = Path(args.gt_json_dir) / (p.stem + ".json")
                if gt_path.exists():
                    gt = json.loads(gt_path.read_text())
                    diffs = deep_compare(rec, gt)
                    for path, a, b in diffs:
                        diff_rows.append({"__file__": p.name, "field": path, "parsed": a, "ground_truth": b})

            print(f"✅ Parsed: {p.name}")
        except Exception as e:
            print(f"❌ Error parsing {p.name}: {e}")

    # Write master
    outbase = args.outbase
    Path(outbase + ".json").write_text(json.dumps(master_records, indent=2, ensure_ascii=False))
    df = pd.DataFrame(flat_rows)
    df.to_csv(outbase + ".csv", index=False)
    df.to_excel(outbase + ".xlsx", index=False)

    # Diffs
    if diff_rows:
        ddf = pd.DataFrame(diff_rows)
        ddf.to_csv(outbase + "_diffs.csv", index=False)
        print(f"⚠️ Wrote diffs to: {outbase}_diffs.csv")

    print("\nDone.")
    print("  Per-file JSONs:", out_dir.resolve())
    print("  Master JSON   :", Path(outbase + '.json').resolve())
    print("  Master CSV    :", Path(outbase + '.csv').resolve())
    print("  Master XLSX   :", Path(outbase + '.xlsx').resolve())

if __name__ == "__main__":
    main()
