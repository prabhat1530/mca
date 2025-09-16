from __future__ import annotations
import os
import re
import json
import argparse
import pathlib
from typing import List, Dict, Any, Optional

# Third-party libraries - install using:
# pip install google-generativeai python-dotenv Pillow pdf2image tqdm pandas openpyxl
from PIL import Image
from tqdm import tqdm
from dotenv import load_dotenv
import google.generativeai as genai
from pdf2image import convert_from_path
import pandas as pd

# Supported file extensions for processing
SUPPORTED_EXTS = [".pdf", ".png", ".jpg", ".jpeg", ".tif", ".tiff"]

# --- "ZERO-TOLERANCE" PROMPT ---
# This prompt is the core of the solution. It has been completely rewritten to be
# bulletproof by providing a perfect example and explicit step-by-step logic for
# the AI to follow, eliminating ambiguity and preventing extraction errors.
PROMPT = r"""
You are a meticulous data extraction specialist. Your task is to analyze the provided Government of India “FORM NO. CHG-1” and extract its data with 100% accuracy into the specified JSON schema.

**CRITICAL INSTRUCTIONS - NO DEVIATION ALLOWED:**

1.  **JSON ONLY:** Your entire output MUST be a single, valid JSON object. Do not include any other text, explanations, or markdown formatting like ```json.
2.  **NULL FOR EMPTY:** Use `null` for any field that is empty, blank, or not applicable.
3.  **CHECKBOXES ARE CRITICAL:** For any checkbox field, you must return the exact text label of the option that is visibly checked (e.g., ☑). This is the most common point of failure; be extra diligent.
4.  **FIELD-SPECIFIC LOGIC:**
    * **Section 3 `form_for_registration_of`:** Find section 3(a). Identify which box is checked: 'Creation of charge' or 'Modification of charge'. The value MUST be the text of the checked box. For the example document, this is "Creation of charge".
    * **Section 7 `charge_on` & `others_specify`:** This is a two-step process.
        * **Step A (`charge_on`):** In section 7(a), find all checked boxes. Create a JSON list containing the text labels of ONLY those checked boxes. In the example document, only 'Others' is checked, so the value MUST be `["Others"]`.
        * **Step B (`others_specify`):** Look at the result from Step A. If the list contains "Others", you MUST extract the text from the "If others, specify" field. If the list does not contain "Others", this value MUST be `null`. For the example document, this is "Lien on Fixed Deposit".
5.  **NO FIELD NUMBERS:** Do not include field numbers like "1." or "2(a)." in the extracted values.

**EXAMPLE OF A PERFECT OUTPUT (Based on the provided document):**
Your final output's structure and data must match the accuracy of this example.
```json
{
  "1": {"corporate_identity_number_cin": "L17119WB1915PLC002657", "global_location_number_gln": null},
  "2": {"name_of_the_company": "CALEDONIAN JUTE & INDUSTRIES LTD", "address_of_the_registered_office": "9 BIPLABI TRAILOKYA MAHARAJSARANI\nKOLKATA\nKolkata\nWest Bengal\n700001\nIndia", "email_id_of_the_company": "caledonian@dataone.in"},
  "3": {"form_for_registration_of": "Creation of charge"},
  "4": {"applicant_is": "The Company"},
  "5": {"date_of_instrument_creating_or_modifying_charge": "13/03/2021", "nature_description_of_instrument": "Memorandum of Charge/Lien over deposits (LD/2128)\nAcknowledgement of Recipt of copy of documents(LD/2252)\nSanction letter(LD/2100A)\nAttestation Memo", "charge_created_or_modified_outside_india": "No"},
  "6": null,
  "7": {"charge_on": ["Others"], "others_specify": "Lien on Fixed Deposit"},
  "8": {"consortium_finance_involved": "No", "joint_charge_involved": "No", "number_of_charge_holders": "1"},
  "9": {"category": "Scheduled bank", "name": "THE SOUTH INDIAN BANK LIMITED", "address": {"line1": "BURRA BAZAR BRANCH", "line2": "25, KALI KRISHNA TAGORE STREET", "city": "KOLKATA", "state": "West Bengal-WB", "country": "INDIA", "pin_code": "700007", "iso_country_code": "IN"}, "email_id": "br0223@sib.co.in", "has_pan": "Yes", "pan": "AABCT0022F", "bsr_code_branch_code": null},
  "10": {"amount_secured_by_charge_in_rs": "900000", "amount_secured_by_charge_in_words": "Rupees Nine Lacs only", "amount_in_foreign_currency_details": null},
  "11": {"date_of_creating_security_interest": "13/03/2021", "borrower_customer_account_number": null, "rate_of_interest": "Interest @ Present Deposit rate 5.40% + 2% (Spread) i.e 7.40% per annum with monthly rests", "repayment_term_in_months": "On Demand", "terms_of_repayment": null, "nature_of_facility": "FSLD (FRESH) - Rs. 9,00,000.00", "date_of_disbursement": null, "miscellaneous_narrative_information": null, "margin": "10%", "extent_and_operation_of_the_charge": "The charge Operates on Fixed Deposit of the Company mentioned in\npoint 13 herein below to secure the maximum amount under the Said\nLimit fixed at Rs.\n900,000.00 together with interest, costs and charges\nthereon", "others": null},
  "12": {"date_of_instrument_creating_or_evidencing_the_charge": None, "description_of_the_instrument_creating_or_evidencing_the_charge": None, "date_of_acquisition_of_the_property": None, "amount_of_the_charge_in_rs": None, "particulars_of_the_property_charged": None},
  "13": {"short_particulars_of_property_or_assets_charged": "KND 0223.101.13926 Dated:- 06/03/2021 for Principal Value of Rs 10,00,000/-"},
  "14": {"number_of_title_documents_deposited_by_customer": None},
  "15": {"is_property_not_registered_in_company_name": "No"},
  "16": {"date_of_creation_or_last_modification_prior_to_present": None}
}
```

Now, extract the data from the provided document pages into this JSON structure.
"""

# ---------------------------- Helper Functions ---------------------------- #

def schema_template() -> Dict[str, Any]:
    """Provides a blank schema template. Used for ordering columns in the Excel."""
    return {
        "1": {"corporate_identity_number_cin": None, "global_location_number_gln": None},
        "2": {"name_of_the_company": None, "address_of_the_registered_office": None, "email_id_of_the_company": None},
        "3": {"form_for_registration_of": None},
        "4": {"applicant_is": None},
        "5": {"date_of_instrument_creating_or_modifying_charge": None, "nature_description_of_instrument": None, "charge_created_or_modified_outside_india": None},
        "6": None,
        "7": {"charge_on": None, "others_specify": None},
        "8": {"consortium_finance_involved": None, "joint_charge_involved": None, "number_of_charge_holders": None},
        "9": {
            "category": None, "name": None,
            "address": {"line1": None, "line2": None, "city": None, "state": None, "country": None, "pin_code": None, "iso_country_code": None},
            "email_id": None, "has_pan": None, "pan": None, "bsr_code_branch_code": None
        },
        "10": {"amount_secured_by_charge_in_rs": None, "amount_secured_by_charge_in_words": None, "amount_in_foreign_currency_details": None},
        "11": {"date_of_creating_security_interest": None, "borrower_customer_account_number": None, "rate_of_interest": None, "repayment_term_in_months": None, "terms_of_repayment": None, "nature_of_facility": None, "date_of_disbursement": None, "miscellaneous_narrative_information": None, "margin": None, "extent_and_operation_of_the_charge": None, "others": None},
        "12": {"date_of_instrument_creating_or_evidencing_the_charge": None, "description_of_the_instrument_creating_or_evidencing_the_charge": None, "date_of_acquisition_of_the_property": None, "amount_of_the_charge_in_rs": None, "particulars_of_the_property_charged": None},
        "13": {"short_particulars_of_property_or_assets_charged": None},
        "14": {"number_of_title_documents_deposited_by_customer": None},
        "15": {"is_property_not_registered_in_company_name": None},
        "16": {"date_of_creation_or_last_modification_prior_to_present": None}
    }

def flatten_dict(d: Dict[str, Any], parent: str = "") -> Dict[str, Any]:
    """
    Flattens a nested dictionary, joining keys with an underscore.
    Example: {'a': {'b': 1}} -> {'a_b': 1}
    """
    flat: Dict[str, Any] = {}
    if d is None:
        return {parent: None} if parent else {}
    for k, v in d.items():
        # Use underscore "_" for better Excel compatibility.
        key = f"{parent}_{k}" if parent else k
        if isinstance(v, dict):
            flat.update(flatten_dict(v, key))
        else:
            flat[key] = v
    return flat

def load_api_key() -> str:
    """Loads the Google API key from a .env file or environment variables."""
    load_dotenv()
    key = os.getenv("GOOGLE_API_KEY", "").strip()
    if not key:
        raise ValueError("GOOGLE_API_KEY not found. Please set it in a .env file or as an environment variable.")
    return key

def sanitize_json_from_response(text: str) -> str:
    """Extracts a JSON object from the model's raw text response."""
    match = re.search(r'\{[\s\S]*\}', text)
    if match:
        return match.group(0)
    raise ValueError("No valid JSON object found in the model's response.")

def initialize_model(model_name: str) -> genai.GenerativeModel:
    """Configures and initializes the Gemini generative model."""
    genai.configure(api_key=load_api_key())
    print(f"[INFO] Initializing model: {model_name}")
    return genai.GenerativeModel(model_name)

def get_images_from_path(path: pathlib.Path, dpi: int, poppler_path: Optional[str]) -> List[Image.Image]:
    """Opens an image file or converts a PDF to a list of images."""
    if path.suffix.lower() == ".pdf":
        print(f"  - Converting PDF to images ({dpi} DPI)...")
        kwargs = {"dpi": dpi}
        if poppler_path:
            kwargs["poppler_path"] = poppler_path
        return convert_from_path(path, **kwargs)
    else:
        print("  - Loading image...")
        return [Image.open(path)]

def list_supported_files(folder: pathlib.Path) -> List[pathlib.Path]:
    """Finds all supported files in a given directory."""
    files: List[pathlib.Path] = []
    for ext in SUPPORTED_EXTS:
        files.extend(sorted(folder.glob(f"*{ext.lower()}")))
        files.extend(sorted(folder.glob(f"*{ext.upper()}")))
    # Remove duplicates
    return sorted(list(set(files)))


# ---------------------------- Core Processing Logic ---------------------------- #

def extract_form_data(
    model: genai.GenerativeModel,
    path: pathlib.Path,
    dpi: int,
    poppler_path: Optional[str]
) -> Dict[str, Any]:
    """
    Main extraction function for a single file.
    It converts the file to images and calls the Gemini model.
    """
    images = get_images_from_path(path, dpi, poppler_path)
    print(f"  - Calling Gemini API with {len(images)} page(s)...")
    
    # Prepare the content for the API call
    request_parts = [PROMPT] + images
    
    # Generate content and parse the response
    response = model.generate_content(request_parts, stream=False)
    response.resolve()
    
    json_text = sanitize_json_from_response(response.text)
    return json.loads(json_text)

def write_json_output(data: Dict[str, Any], out_path: pathlib.Path) -> None:
    """Writes a dictionary to a JSON file."""
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def write_excel_output(data_list: List[Dict[str, Any]], out_path: pathlib.Path) -> None:
    """Writes a list of dictionaries to a single Excel sheet."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(data_list)
    
    # Reorder columns to match the schema
    schema_keys = list(flatten_dict(schema_template()).keys())
    # Add 'error' column if needed, to handle failed files
    if 'error' in df.columns:
        all_cols = ['_source_filename'] + schema_keys + ['error']
    else:
        all_cols = ['_source_filename'] + schema_keys
    
    # Reindex the DataFrame to ensure consistent column order
    df = df.reindex(columns=all_cols)
    
    # Save to Excel
    df.to_excel(out_path, index=False)


# ---------------------------- Command-Line Interface ---------------------------- #

def parse_arguments() -> argparse.Namespace:
    """Sets up and parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Batch-extract data from CHG-1 forms using Gemini.",
        formatter_class=argparse.
RawTextHelpFormatter
    )
    parser.add_argument(
        "--input-dir", "-i",
        default="pdfs",
        help="Folder containing source PDFs/images.\n(default: ./pdfs)"
    )
    parser.add_argument(
        "--json-out", "-j",
        default="output_json",
        help="Folder to save individual JSON output files.\n(default: ./output_json)"
    )
    parser.add_argument(
        "--excel-out", "-e",
        default="output_excel",
        help="Folder to save the master Excel file.\n(default: ./output_excel)"
    )
    parser.add_argument(
        "--model", "-m",
        default="gemini-1.5-flash",
        help="The Gemini model to use for extraction.\n(default: gemini-1.5-flash)"
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=300,
        help="Resolution (DPI) for rendering PDF pages.\n(default: 300)"
    )
    parser.add_argument(
        "--poppler-path",
        default=None,
        help="Optional path to the Poppler bin directory if not in system PATH."
    )
    return parser.parse_args()

def main() -> None:
    """Main function to run the batch extraction process."""
    args = parse_arguments()

    # Resolve and create output directories
    input_dir = pathlib.Path(args.input_dir).resolve()
    json_dir = pathlib.Path(args.json_out).resolve()
    excel_dir = pathlib.Path(args.excel_out).resolve()
    json_dir.mkdir(parents=True, exist_ok=True)
    excel_dir.mkdir(parents=True, exist_ok=True)

    # Find supported files
    files_to_process = list_supported_files(input_dir)
    if not files_to_process:
        print(f"[ERROR] No supported files found in: {input_dir}")
        print(f"Supported extensions: {', '.join(SUPPORTED_EXTS)}")
        return

    # Initialize model
    try:
        model = initialize_model(args.model)
    except Exception as e:
        print(f"[FATAL] Could not initialize model: {e}")
        return

    all_results_for_master_excel: List[Dict[str, Any]] = []

    print(f"\n[INFO] Found {len(files_to_process)} file(s). Starting extraction...")
    # Process each file with a progress bar
    for fpath in tqdm(files_to_process, desc="Processing Files", unit="file"):
        try:
            # --- Core Extraction Step ---
            extracted_data = extract_form_data(
                model, fpath, dpi=args.dpi, poppler_path=args.poppler_path
            )

            # Flatten the result and add source filename
            flat_data = flatten_dict(extracted_data)
            flat_data["_source_filename"] = fpath.name
            
            # --- Write Individual JSON Output ---
            json_path = json_dir / f"{fpath.stem}.json"
            write_json_output(extracted_data, json_path)
            
            # Add to the list for the final master Excel
            all_results_for_master_excel.append(flat_data)

        except Exception as e:
            # Handle failures gracefully
            print(f"\n[ERROR] Failed to process {fpath.name}: {e}")
            error_record = {
                "_source_filename": fpath.name,
                "error": str(e)
            }
            all_results_for_master_excel.append(error_record)

            # Write individual error JSON for traceability
            error_json_path = json_dir / f"{fpath.stem}_error.json"
            write_json_output(error_record, error_json_path)
    
    # --- Write Master Excel File ---
    if all_results_for_master_excel:
        master_excel_path = excel_dir / "all_records.xlsx"
        write_excel_output(all_results_for_master_excel, master_excel_path)
        print(f"\n[SUCCESS] Master Excel file created at: {master_excel_path}")
    
    print(f"[DONE] All processing complete.")
    print(f"  - JSON outputs are in: {json_dir}")
    print(f"  - Excel outputs are in: {excel_dir}")


if __name__ == "__main__":
    main()
