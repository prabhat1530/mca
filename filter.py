import os
import re
import json
import pandas as pd
import fitz  # PyMuPDF
from PIL import Image, ImageOps, ImageFilter
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
import io
import shutil

try:
    import pytesseract
except ImportError:
    pytesseract = None

# ===== CONFIG =====
INPUT_PATH = "13_Form_CHG-1-17042021_signed.pdf"
OUTPUT_BASE = "chg1_extracted_data"
APPEND_MODE = False
OCR_LANGS = "eng"
BASE_DPI = 300
REGION_DPI = 400
# ==================

# ---- OCR availability ----
HAVE_TESS = shutil.which("tesseract") is not None and (pytesseract is not None)

def _preprocess_for_ocr(img: Image.Image) -> Image.Image:
    """Light preprocessing to improve OCR."""
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

def extract_text_blocks_and_images(pdf_path: str) -> Tuple[str, List, List, List]:
    """Extract text and images from PDF."""
    doc = fitz.open(pdf_path)
    page_texts, page_blocks, page_images, page_sizes = [], [], [], []

    for p in doc:
        # Text layer
        t_text = (p.get_text("text") or "").replace("\r", "\n")
        
        # OCR full page
        ocr_img = _ocr_page_image(p, dpi=BASE_DPI)
        page_images.append(ocr_img)
        page_sizes.append((p.rect.width, p.rect.height))
        t_ocr = _ocr_image_to_text(ocr_img, OCR_LANGS) if ocr_img is not None else ""
        
        # Combine text
        combined = t_text
        if t_ocr and len(t_ocr.strip()) > 20:
            combined += "\n" + t_ocr
        page_texts.append(combined)
        
        # Text blocks
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

def norm_text(s: str) -> str:
    """Normalize text."""
    if not s:
        return ""
    s = s.replace("—", "-").replace("–", "-").replace("­", "")
    s = s.replace("ﬁ", "fi").replace("ﬂ", "fl")
    return re.sub(r"\s+", " ", s).strip()

def norm_date(d: str) -> str:
    """Normalize date format."""
    if not d:
        return ""
    d = re.sub(r"[^\d/]", "/", d.strip())
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(d, fmt).strftime("%d/%m/%Y")
        except ValueError:
            continue
    return d

def extract_field(pattern: str, text: str, default: str = "") -> str:
    """Extract field using regex pattern."""
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    return match.group(1).strip() if match else default

def extract_checkbox_options(text: str, options: List[str]) -> List[str]:
    """Extract selected checkbox options."""
    selected = []
    for option in options:
        if re.search(rf"[☑✓X■●]\s*{re.escape(option)}", text, re.IGNORECASE):
            selected.append(option)
    return selected

def extract_radio_option(text: str, options: List[str]) -> str:
    """Extract selected radio option."""
    for option in options:
        if re.search(rf"[☑✓X■●]\s*{re.escape(option)}", text, re.IGNORECASE):
            return option
    return ""

def parse_chg1_form(pdf_path: str) -> Dict[str, Any]:
    """Parse CHG-1 form and extract all fields."""
    full_text, blocks, page_images, page_sizes = extract_text_blocks_and_images(pdf_path)
    full_text = norm_text(full_text)
    
    result = {}
    
    # Section 1: Company Identification
    result["1(a) CIN"] = extract_field(r"CIN.*?([A-Z0-9]{21})", full_text)
    result["1(b) GLN"] = extract_field(r"GLN.*?([A-Z0-9]+)", full_text)
    
    # Section 2: Company Details
    result["2(a) Company Name"] = extract_field(r"Name of the company\s*([^\n]+)", full_text)
    result["2(b) Address"] = extract_field(r"Address of the registered office[^\n]*\s*([^\n]+(?:\n[^\n]+){0,5})", full_text)
    result["2(c) Email"] = extract_field(r"email id of the company[^\n]*\s*([^\n@]+@[^\n@]+\.[^\n@]+)", full_text)
    
    # Section 3: Form Purpose
    form_options = ["Creation of charge", "Modification of charge"]
    result["3(a) Form Purpose"] = extract_radio_option(full_text, form_options)
    
    # Section 4: Applicant Type
    applicant_options = ["The Company", "The charge holder"]
    result["4 Applicant Type"] = extract_radio_option(full_text, applicant_options)
    
    # Section 5: Instrument Details
    result["5(a) Instrument Date"] = norm_date(extract_field(r"Date of the instrument[^\n]*\s*([\d/]+)", full_text))
    result["5(b) Instrument Description"] = extract_field(r"Nature.*?particulars[^\n]*\s*([^\n]+(?:\n[^\n]+){0,3})", full_text)
    result["5(c) Charge Outside India"] = extract_radio_option(full_text, ["Yes", "No"])
    
    # Section 7: Type of Charge
    charge_options = [
        "Uncalled share capital", "Calls made but not paid", "Immovable property",
        "Movable property", "Floating charge", "Motor Vehicle",
        "Any property for securing the issue of secured deposits", "Goodwill",
        "Patent", "Licence under a patent", "Trade mark", "Copyright",
        "Book debts", "Ship or any share in a ship", "Solely of Property situated outside India", "Others"
    ]
    selected_charges = extract_checkbox_options(full_text, charge_options)
    result["7(a) Type of Charge"] = ", ".join(selected_charges)
    result["7(b) Other Charge Type"] = extract_field(r"If others.*?specify[^\n]*\s*([^\n]+)", full_text)
    
    # Section 8: Finance Details
    result["8(a) Consortium Finance"] = extract_radio_option(full_text, ["Yes", "No"])
    result["8(b) Joint Charge"] = extract_radio_option(full_text, ["Yes", "No"])
    result["8(c) Number of Charge Holders"] = extract_field(r"Number of charge holder\(s\)[^\n]*\s*(\d+)", full_text)
    
    # Section 9: Charge Holder Details
    result["9 Charge Holder Category"] = extract_field(r"Category[^\n]*\s*([^\n]+)", full_text)
    result["9 Charge Holder Name"] = extract_field(r"Name of charge holder[^\n]*\s*([^\n]+)", full_text)
    result["9 Charge Holder Address Line I"] = extract_field(r"Address.*?Line I[^\n]*\s*([^\n]+)", full_text)
    result["9 Charge Holder Address Line II"] = extract_field(r"Line II[^\n]*\s*([^\n]+)", full_text)
    result["9 Charge Holder City"] = extract_field(r"City[^\n]*\s*([^\n]+)", full_text)
    result["9 Charge Holder State"] = extract_field(r"State[^\n]*\s*([^\n]+)", full_text)
    result["9 Charge Holder Country"] = extract_field(r"Country[^\n]*\s*([^\n]+)", full_text)
    result["9 Charge Holder Pin Code"] = extract_field(r"Pin code[^\n]*\s*([^\n]+)", full_text)
    result["9 Charge Holder Email"] = extract_field(r"e-mail id[^\n]*\s*([^\n@]+@[^\n@]+\.[^\n@]+)", full_text)
    result["9 Charge Holder ISO Country Code"] = extract_field(r"ISO country code[^\n]*\s*([A-Z]{2})", full_text)
    result["9 Charge Holder Has Valid PAN"] = extract_radio_option(full_text, ["Yes", "No"])
    result["9 Charge Holder PAN"] = extract_field(r"Permanent Account Number.*?\(PAN\)[^\n]*\s*([A-Z0-9]+)", full_text)
    result["9 Charge Holder BSR Code"] = extract_field(r"BSR Code.*?Branch Code[^\n]*\s*([^\n]+)", full_text)
    
    # Section 10: Amount Secured
    result["10(a) Amount Secured (Rs)"] = extract_field(r"Amount secured.*?Rs\.?\)?[^\n]*\s*([\d,]+)", full_text)
    result["10(b) Amount Secured (Words)"] = extract_field(r"Amount secured.*?words[^\n]*\s*([^\n]+)", full_text)
    result["10(c) Foreign Currency Details"] = extract_field(r"foreign currency.*?details[^\n]*\s*([^\n]+)", full_text)
    
    # Section 11: Terms and Conditions
    result["11(a) Date of Creating Security Interest"] = norm_date(extract_field(r"Date of Creating Security Interest[^\n]*\s*([\d/]+)", full_text))
    result["11(b) Borrower's Account Number"] = extract_field(r"Borrower.*?account number[^\n]*\s*([^\n]+)", full_text)
    result["11(c) Rate of Interest"] = extract_field(r"Rate of interest[^\n]*\s*([^\n]+)", full_text)
    result["11(d) Repayment Term"] = extract_field(r"Repayment term[^\n]*\s*([^\n]+)", full_text)
    result["11(e) Terms of Repayment"] = extract_field(r"Terms of Repayment[^\n]*\s*([^\n]+)", full_text)
    result["11(f) Nature of Facility"] = extract_field(r"Nature of facility[^\n]*\s*([^\n]+)", full_text)
    result["11(g) Date of Disbursement"] = norm_date(extract_field(r"Date of Disbursement[^\n]*\s*([\d/]+)", full_text))
    result["11(h) Miscellaneous Info"] = extract_field(r"Miscellaneous[^\n]*\s*([^\n]+)", full_text)
    result["11(i) Margin"] = extract_field(r"Margin[^\n]*\s*([^\n]+)", full_text)
    result["11(j) Extent of Charge"] = extract_field(r"Extent.*?charge[^\n]*\s*([^\n]+)", full_text)
    result["11(k) Others"] = extract_field(r"Others.*?11[^\n]*\s*([^\n]+)", full_text)
    
    # Section 12: Property Acquisition
    result["12(a) Instrument Creation Date"] = norm_date(extract_field(r"Instrument Creation Date[^\n]*\s*([\d/]+)", full_text))
    result["12(b) Instrument Description"] = extract_field(r"Instrument Description[^\n]*\s*([^\n]+)", full_text)
    result["12(c) Property Acquisition Date"] = norm_date(extract_field(r"Property Acquisition Date[^\n]*\s*([\d/]+)", full_text))
    result["12(d) Amount of Charge"] = extract_field(r"Amount of Charge[^\n]*\s*([\d,]+)", full_text)
    result["12(e) Property Particulars"] = extract_field(r"Property Particulars[^\n]*\s*([^\n]+)", full_text)
    
    # Section 13: Property Details
    result["13 Property Details"] = extract_field(r"Short particulars.*?property[^\n]*\s*([^\n]+(?:\n[^\n]+){0,3})", full_text)
    
    # Section 14: Title Document
    result["14 Title Document Number"] = extract_field(r"Number of title documents[^\n]*\s*([^\n]+)", full_text)
    result["14 Title Document Description"] = extract_field(r"Description.*?document[^\n]*\s*([^\n]+)", full_text)
    
    # Section 15: Property Registration
    result["15(a) Property Not in Company Name"] = extract_radio_option(full_text, ["Yes", "No"])
    
    # Section 16: Modification Date
    result["16 Date of Creation/Last Modification"] = norm_date(extract_field(r"Date of creation.*?modification[^\n]*\s*([\d/]+)", full_text))
    
    # Clean empty values
    result = {k: v for k, v in result.items() if v not in ["", None]}
    
    return result

def process_pdf_files():
    """Process PDF files and generate CSV and JSON outputs."""
    pdf_files = []
    
    if os.path.isdir(INPUT_PATH):
        for file in os.listdir(INPUT_PATH):
            if file.lower().endswith(".pdf"):
                pdf_files.append(os.path.join(INPUT_PATH, file))
    else:
        pdf_files.append(INPUT_PATH)
    
    all_data = []
    
    for pdf_file in pdf_files:
        try:
            print(f"Processing: {os.path.basename(pdf_file)}")
            data = parse_chg1_form(pdf_file)
            data["__file__"] = os.path.basename(pdf_file)
            all_data.append(data)
        except Exception as e:
            print(f"Error processing {pdf_file}: {str(e)}")
    
    if not all_data:
        print("No PDF files processed successfully.")
        return
    
    # Create DataFrame
    df = pd.DataFrame(all_data)
    
    # Ensure all expected columns exist
    expected_columns = [
        "__file__", "1(a) CIN", "1(b) GLN", "2(a) Company Name", "2(b) Address", 
        "2(c) Email", "3(a) Form Purpose", "4 Applicant Type", "5(a) Instrument Date", 
        "5(b) Instrument Description", "5(c) Charge Outside India", "7(a) Type of Charge", 
        "7(b) Other Charge Type", "8(a) Consortium Finance", "8(b) Joint Charge", 
        "8(c) Number of Charge Holders", "9 Charge Holder Category", "9 Charge Holder Name", 
        "9 Charge Holder Address Line I", "9 Charge Holder Address Line II", "9 Charge Holder City", 
        "9 Charge Holder State", "9 Charge Holder Country", "9 Charge Holder Pin Code", 
        "9 Charge Holder Email", "9 Charge Holder ISO Country Code", "9 Charge Holder Has Valid PAN", 
        "9 Charge Holder PAN", "9 Charge Holder BSR Code", "10(a) Amount Secured (Rs)", 
        "10(b) Amount Secured (Words)", "10(c) Foreign Currency Details", 
        "11(a) Date of Creating Security Interest", "11(b) Borrower's Account Number", 
        "11(c) Rate of Interest", "11(d) Repayment Term", "11(e) Terms of Repayment", 
        "11(f) Nature of Facility", "11(g) Date of Disbursement", "11(h) Miscellaneous Info", 
        "11(i) Margin", "11(j) Extent of Charge", "11(k) Others", "12(a) Instrument Creation Date", 
        "12(b) Instrument Description", "12(c) Property Acquisition Date", "12(d) Amount of Charge", 
        "12(e) Property Particulars", "13 Property Details", "14 Title Document Number", 
        "14 Title Document Description", "15(a) Property Not in Company Name", 
        "16 Date of Creation/Last Modification"
    ]
    
    for col in expected_columns:
        if col not in df.columns:
            df[col] = ""
    
    # Reorder columns
    df = df[expected_columns]
    
    # Save to CSV
    csv_path = f"{OUTPUT_BASE}.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"CSV saved to: {csv_path}")
    
    # Save to JSON
    json_path = f"{OUTPUT_BASE}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)
    print(f"JSON saved to: {json_path}")
    
    return all_data

if __name__ == "__main__":
    process_pdf_files()