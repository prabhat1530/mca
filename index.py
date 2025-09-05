import os, re, io, json, unicodedata, shutil
from datetime import datetime
from typing import List, Dict, Tuple, Optional

import fitz  # PyMuPDF
from PIL import Image
try:
    import pytesseract
except ImportError:
    pytesseract = None

import pandas as pd

# ====== CONFIG ======
INPUT_PATH  = "13_Form_CHG-1-17042021_signed.pdf"  # file or folder
OUTPUT_BASE = "chg1_full_output_optimized"
APPEND_MODE = False
# ====================

# ---- OCR availability (optional) ----
HAVE_TESS = shutil.which("tesseract") is not None and (pytesseract is not None)

# ---------------- OCR helpers ----------------
def _ocr_image_to_text(img, lang="eng"):
    if HAVE_TESS:
        try:
            return pytesseract.image_to_string(img, lang=lang)
        except Exception:
            return ""
    return ""

def _ocr_pixmap(pix, lang="eng"):
    try:
        img = Image.open(io.BytesIO(pix.tobytes("png")))
    except Exception:
        return ""
    return _ocr_image_to_text(img, lang=lang)

def ocr_page(page, dpi=300):
    if not HAVE_TESS: return ""
    pix = page.get_pixmap(dpi=dpi)
    return _ocr_pixmap(pix)

# ---------------- Text + blocks extraction ----------------
def extract_text_and_blocks(pdf_path) -> Tuple[str, List[List[Dict]]]:
    doc = fitz.open(pdf_path)
    page_texts, page_blocks = [], []
    for p in doc:
        t = p.get_text("text", sort=True) or ""
        if len(t.strip()) < 150: # fallback to OCR if text layer is sparse
            t_ocr = ocr_page(p)
            if len(t_ocr.strip()) > len(t.strip()):
                t = t_ocr
        t = t.replace("\r", "")
        page_texts.append(t)

        blocks = sorted(p.get_text("blocks"), key=lambda b: (b[1], b[0]))
        bdicts = [{"rect": b[:4], "text": b[4].replace("\r", "")} for b in blocks]
        page_blocks.append(bdicts)
    doc.close()
    return "\n".join(page_texts), page_blocks

# ---------------- Normalizers & Regex Helpers ----------------
def norm_text(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKC", s)
    return re.sub(r'\s+', ' ', s).strip()

def find_global_regex(text, patterns: List[str]) -> str:
    for pat in patterns:
        m = re.search(pat, text or "", re.IGNORECASE | re.DOTALL | re.MULTILINE)
        if m:
            return norm_text(m.group(1).replace('\n', ' '))
    return ""

def norm_date(d: str) -> str:
    if not d: return ""
    d = re.sub(r'[\s\.\-\\]', '/', d)
    d = re.sub(r'[^\d/]', '', d)
    m = re.search(r'(\d{2}/\d{2}/\d{4})', d)
    return m.group(1) if m else d.strip()

EMAIL_RE = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')
PAN_RE   = re.compile(r'\b([A-Z]{5}[0-9]{4}[A-Z])\b')

def _get_section_text(full_text, start_patterns, end_patterns):
    """More robustly isolates the text of a section."""
    text = full_text
    start_pos = -1

    for pat in start_patterns:
        match = re.search(pat, text, re.IGNORECASE | re.MULTILINE)
        if match:
            start_pos = match.start()
            break
    if start_pos == -1:
        return ""

    text_from_start = text[start_pos:]
    end_pos = len(text_from_start) # Default to end of document

    for pat in end_patterns:
        match = re.search(pat, text_from_start, re.IGNORECASE | re.MULTILINE)
        if match and match.start() > 5: # Ensure it's not matching itself
            end_pos = match.start()
            break
            
    return text_from_start[:end_pos]

def find_selected_option(text_blob, options, label_regex=""):
    """Finds a selected option, often marked with [X], (X), or a filled circle."""
    context = text_blob
    if label_regex:
        m = re.search(label_regex, text_blob, re.IGNORECASE | re.DOTALL)
        if m:
            context = text_blob[m.start():]

    for option in options:
        # Pattern to find option preceded by a graphical or text marker of selection
        pat = r'[\s(]X[\s)]|☑|⦿\s*|O\s+' + re.escape(option)
        if re.search(pat, context, re.IGNORECASE):
            return option
    return ""

# ---------------- Section 9 (Charge Holder) - REWRITTEN ----------------
def parse_section9(full_text: str) -> dict:
    result = {
        "Category":"", "Name":"", "Address Line I":"", "Address Line II":"",
        "ISO Country Code":"", "City":"", "State":"", "Country":"", "Pin Code":"",
        "Email":"", "Has Valid PAN":"", "PAN":"", "Branch Code":""
    }
    sec9_text = _get_section_text(full_text,
        [r"^\s*9\s*\.?\s*Particulars of the charge holder"],
        [r"^\s*10\s*\.?\s*Amount secured by the charge"]
    )
    if not sec9_text: return result

    result["Category"] = find_global_regex(sec9_text, [r"Category\s+([^\n]+)"])
    result["Name"] = find_global_regex(sec9_text, [r"Name\s+([^\n]+)"])
    
    # Address is often multi-line
    addr_match = re.search(r"Address\s*\*?\s*Line\s*I\s+([^\n]+)\s*Line\s*II\s+([^\n]+)", sec9_text, re.IGNORECASE)
    if addr_match:
        result["Address Line I"] = norm_text(addr_match.group(1))
        result["Address Line II"] = norm_text(addr_match.group(2))

    result["City"] = find_global_regex(sec9_text, [r"\*?\s*City\s+([^\n]+)"])
    result["State"] = find_global_regex(sec9_text, [r"\*?\s*State\s+([^\n]+?)\s*\*"])
    result["Country"] = find_global_regex(sec9_text, [r"\*?\s*Country\s+([^\n]+)"])
    result["Pin Code"] = find_global_regex(sec9_text, [r"\*?\s*Pin\s*code\s+(\d{6})"])
    result["ISO Country Code"] = find_global_regex(sec9_text, [r"\*?\s*ISO\s*country\s*code\s+([A-Z]{2})"])
    
    email_match = EMAIL_RE.search(sec9_text)
    result["Email"] = email_match.group(0) if email_match else ""

    pan_context = find_global_regex(sec9_text, [r"(valid Income Tax PAN.*)"])
    if "Yes" in pan_context: result["Has Valid PAN"] = "Yes"
    elif "No" in pan_context: result["Has Valid PAN"] = "No"

    pan_match = PAN_RE.search(sec9_text)
    result["PAN"] = pan_match.group(1) if pan_match else ""
    result["Branch Code"] = find_global_regex(sec9_text, [r"BSR Code\s*/\s*Branch Code\s*([^\n]*)"])

    return result

# ---------------- Full schema ----------------
def mk_empty_record() -> Dict[str, object]:
    return {
        # Section 1-8
        "1(a) CIN":"", "1(b) GLN":"", "2(a) Company Name":"", "2(b) Address":"", "2(c) Email":"",
        "3(a) Registration Type":"", "4 Applicant Type":"", "5(a) Instrument Date":"", "5(b) Instrument Description":"",
        "5(c) Charge created outside India":"", "7(a) Type of Charge":"", "7(b) Others Specify":"",
        "8(a) Consortium Finance":"", "8(b) Joint Charge":"", "8(c) Number of Charge Holders":"",
        # Section 9
        "9 Charge Holder":{
            "Category":"", "Name":"", "Address Line I":"", "Address Line II":"", "ISO Country Code":"",
            "City":"", "State":"", "Country":"", "Pin Code":"", "Email":"", "Has Valid PAN":"", "PAN":"", "Branch Code":""
        },
        # Section 10
        "10(a) Amount Secured (Rs)":"", "10(b) Amount Secured (Words)":"", "10(c) Foreign Currency Details":"",
        # Section 11
        "11(a) Security Interest Date":"", "11(b) Borrower Account No":"", "11(c) Rate of Interest":"",
        "11(d) Repayment Term (Months)":"", "11(e) Terms of Repayment":"", "11(f) Nature of Facility":"",
        "11(g) Disbursement Date":"", "11(h) Misc Info":"", "11(i) Margin":"", "11(j) Extent of Charge":"",
        # Sections 12-16
        "13 Property Details":"", "15(a) Property not in company name":"", "16 Last Modification Date":"",
        # Attachments, Declarations, Certification
        "ATT Instrument File":"", "DECL Company Resolution No":"", "DECL Company Resolution Date":"",
        "DECL Company Signer Name":"", "DECL Company Designation":"", "DECL Company DIN":"",
        "CERT Professional Type":"", "CERT Fellowship":"", "CERT Membership No":"", "CERT COP No":""
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

# ---------------- Main Parser ----------------
def parse_chg1_full(pdf_path: str) -> dict:
    full_text, blocks = extract_text_and_blocks(pdf_path)
    data = mk_empty_record()

    # Section 1 & 2: Company Details
    data["1(a) CIN"] = find_global_regex(full_text, [r"Corporate identity number \(CIN\).*?([A-Z0-9]{21})"])
    data["2(a) Company Name"] = find_global_regex(full_text, [r"Name of the company\s+([^\n]+)"])
    data["2(b) Address"] = _get_section_text(full_text, [r"Address of the registered office"], [r"email id of the company"])
    data["2(c) Email"] = find_global_regex(full_text, [r"email id of the company\s+([^\n]+)"])

    # Section 3 & 4: Registration Type & Applicant
    data["3(a) Registration Type"] = find_selected_option(full_text, ["Creation of charge", "Modification of charge"], r"This form is for registration of")
    data["4 Applicant Type"] = find_selected_option(full_text, ["The Company", "The charge holder"], r"Whether the applicant is")

    # Section 5: Instrument Details
    data["5(a) Instrument Date"] = norm_date(find_global_regex(full_text, [r"Date of the instrument creating.*?(\d{2}/\d{2}/\d{4})"]))
    data["5(b) Instrument Description"] = _get_section_text(full_text, [r"instrument\(s\) creating or modifying the charge"], [r"charge is created or modified outside India"])
    data["5(c) Charge created outside India"] = find_selected_option(full_text, ["Yes", "No"], r"charge is created or modified outside India") or "No"

    # Section 7 & 8: Charge Type and Finance
    data["7(a) Type of Charge"] = find_selected_option(full_text, ["Others", "Immovable property", "Movable property", "Floating charge", "Book debts"], r"7\.\s*Type of charge")
    data["7(b) Others Specify"] = find_global_regex(full_text, [r"If others, specify\s+([^\n]+)"])
    data["8(a) Consortium Finance"] = find_selected_option(full_text, ["Yes", "No"], r"consortium finance is involved") or "No"
    data["8(b) Joint Charge"] = find_selected_option(full_text, ["Yes", "No"], r"joint charge is involved") or "No"
    data["8(c) Number of Charge Holders"] = find_global_regex(full_text, [r"Number of charge holder\(s\)\s+(\d+)"])

    # Section 9: Charge Holder (uses dedicated function)
    data["9 Charge Holder"] = parse_section9(full_text)

    # Section 10: Secured Amount
    sec10_text = _get_section_text(full_text, [r"10\.\s*\(a\)"], [r"11\.\s*Brief particulars"])
    data["10(a) Amount Secured (Rs)"] = find_global_regex(sec10_text, [r"charge \(in Rs\.\)\s+([\d,]+)"])
    data["10(b) Amount Secured (Words)"] = find_global_regex(sec10_text, [r"charge in words\s+([^\n]+)"])
    data["10(c) Foreign Currency Details"] = find_global_regex(sec10_text, [r"foreign currency, mention details\s+([^\n]+)"])

    # Section 11: Terms and Conditions
    sec11_text = _get_section_text(full_text, [r"11\.\s*Brief particulars"], [r"12\.\s*In case of acquisition"])
    data["11(a) Security Interest Date"] = norm_date(find_global_regex(sec11_text, [r"Date of Creating Security Interest.*?(\d{2}/\d{2}/\d{4})"]))
    data["11(b) Borrower Account No"] = find_global_regex(sec11_text, [r"Borrower's customer/account number\s+([^\n]*)"])
    data["11(c) Rate of Interest"] = find_global_regex(sec11_text, [r"Rate of interest\s+([^\n]+)"])
    data["11(d) Repayment Term (Months)"] = find_global_regex(sec11_text, [r"Repayment term \(in months\)\s+([^\n]*)"])
    data["11(e) Terms of Repayment"] = find_global_regex(sec11_text, [r"Terms of Repayment\s+([^\n]+)"])
    data["11(f) Nature of Facility"] = find_global_regex(sec11_text, [r"Nature of facility\s+([^\n]+)"])
    data["11(g) Disbursement Date"] = norm_date(find_global_regex(sec11_text, [r"Date of Disbursement\s+([^\n(]*)"]))
    data["11(i) Margin"] = find_global_regex(sec11_text, [r"\(i\)\s*Margin\s+([^\n]+)"])
    data["11(j) Extent of Charge"] = find_global_regex(sec11_text, [r"\(j\)\s*Extent and operation of the charge\s+([^\n]+)"])

    # Section 13 & 15 & 16: Property Details
    data["13 Property Details"] = find_global_regex(full_text, [r"13\.\s*\*?Short particulars of the property.*?charged[^\n]*\n([^\n]+)"])
    data["15(a) Property not in company name"] = find_selected_option(full_text, ["Yes", "No"], r"property or interest.*?not registered") or "No"
    data["16 Last Modification Date"] = norm_date(find_global_regex(full_text, [r"16\.\s*\*?Date of creation/last modification.*?(\d{2}/\d{2}/\d{4})"]))
    
    # Attachments
    att_text = _get_section_text(full_text, [r"List of attachments"], [r"Declaration"])
    data["ATT Instrument File"] = find_global_regex(att_text, [r"Instrument\(s\) of creation.*?charge;\s+([^\n]+)"])
    
    # Declaration
    decl_text = _get_section_text(full_text, [r"Declaration"], [r"To be digitally signed by\s*\n\s*Charge holder"])
    data["DECL Company Resolution No"] = find_global_regex(decl_text, [r"resolution no\*?\s+([^\s]+)"])
    data["DECL Company Resolution Date"] = norm_date(find_global_regex(decl_text, [r"\*?\s*dated\s+([^\n]+)"]))
    data["DECL Company Signer Name"] = find_global_regex(decl_text, [r"To be digitally signed by\s+([^\n]+)"])
    data["DECL Company Designation"] = find_global_regex(decl_text, [r"Designation\s+([^\n]+)"])
    data["DECL Company DIN"] = find_global_regex(decl_text, [r"company secretary\s+([^\n]+)"])

    # Certification
    cert_text = _get_section_text(full_text, [r"Certificate by practicing professional"], [r"This eForm has been taken"])
    data["CERT Professional Type"] = find_selected_option(cert_text, ["Chartered accountant", "Cost accountant", "Company secretary"])
    data["CERT Fellowship"] = find_selected_option(cert_text, ["Associate", "Fellow"])
    data["CERT Membership No"] = find_global_regex(cert_text, [r"Membership number\s+([^\s]+)"])
    data["CERT COP No"] = find_global_regex(cert_text, [r"Certificate of Practice number\s+([^\s]+)"])

    return data

# ---------------- Runner ----------------
def run_batch():
    rows = []
    paths = []
    if os.path.isdir(INPUT_PATH):
        for f in sorted(os.listdir(INPUT_PATH)):
            if f.lower().endswith(".pdf"):
                paths.append(os.path.join(INPUT_PATH, f))
    elif os.path.exists(INPUT_PATH):
        paths.append(INPUT_PATH)

    for p in paths:
        try:
            rec = parse_chg1_full(p)
            rows.append(flatten_record(rec, os.path.basename(p)))
            print(f"✅ Parsed: {os.path.basename(p)}")
        except Exception as e:
            print(f"❌ Error parsing {os.path.basename(p)}: {e}")
            import traceback
            traceback.print_exc()

    if not rows:
        print("No PDFs found or parsed.")
        return

    df = pd.DataFrame(rows)
    csv_path  = f"{OUTPUT_BASE}.csv"
    xlsx_path = f"{OUTPUT_BASE}.xlsx"
    json_path = f"{OUTPUT_BASE}.json"

    if APPEND_MODE and os.path.exists(csv_path):
        old = pd.read_csv(csv_path)
        df = pd.concat([old, df], ignore_index=True)

    df.to_csv(csv_path, index=False, encoding="utf-8")
    df.to_excel(xlsx_path, index=False)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)

    print("\n🚀 Saved outputs:")
    print("  -> CSV: ", os.path.abspath(csv_path))
    print("  -> XLSX:", os.path.abspath(xlsx_path))
    print("  -> JSON:", os.path.abspath(json_path))
    if not HAVE_TESS:
        print("\nℹ️  Note: OCR is disabled (Tesseract not found). Extraction relies on the PDF's text layer.")

if __name__ == "__main__":
    run_batch()