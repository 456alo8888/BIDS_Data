import pandas as pd
import mne
import re
import unidecode
import os
import glob
from tqdm import tqdm

# === Function to extract EDF metadata ===
def extract_edf_metadata(edf_file):
    try:
        raw = mne.io.read_raw_edf(edf_file, preload=False, verbose=False)
        subject_info = raw.info.get('subject_info', {})

        # Extract last_name and birth suffix
        last_name_raw = subject_info.get('last_name')
        match_num = re.search(r'^(.*?)[_]?(\d+)?$', last_name_raw)
        name_only = match_num.group(1).replace("_", " ").strip() if match_num else None
        birth_suffix = match_num.group(2) if match_num else None

        return name_only, birth_suffix
    except Exception as e:
        print(f"Error reading EDF file {edf_file}: {e}")
        return None, None

# === Function to extract birth year suffix ===
def extract_birth_year_suffix(birth_date):
    try:
        birth_date = pd.to_datetime(birth_date)
        return str(birth_date.year)[-4:]  # Extract last 4 digits of year
    except:
        return None

# === Main Script ===
# Read clinical sheet
clinical_sheet_original = pd.read_excel("/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/kqcls.xlsx")  # Keep original for final export
clinical_sheet = clinical_sheet_original.copy()  # Work on copy for processing

# Standardize names
clinical_sheet['PATIENT_NAME_STD'] = clinical_sheet['PATIENT_NAME'].apply(lambda x: unidecode.unidecode(str(x)).upper())

# Try to create BIRTH_YEAR if BIRTH_DATE exists
if 'BIRTH_DATE' in clinical_sheet.columns:
    clinical_sheet['BIRTH_YEAR'] = clinical_sheet['BIRTH_DATE'].apply(lambda x: extract_birth_year_suffix(x))
else:
    clinical_sheet['BIRTH_YEAR'] = None  # Set to None if BIRTH_DATE is missing

# Aggregate rows with the same DOC_NO for matching
aggregated_sheet = clinical_sheet.groupby('DOC_NO').agg({
    'PATIENT_NAME_STD': 'first',
    'BIRTH_DATE': 'first',
    'GENDER': 'first',
    'HFL_NAME': 'first',
    'PARA_RESULT': lambda x: ';'.join(x.dropna()) if x.notnull().any() else 'n/a'  # Combine PARA_RESULT
}).reset_index()

# Directory containing multiple EDF files
edf_dir = "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/EEG2100/edf_files"  # Replace with your actual EDF folder path
matched_patients_xlsx = "./matched_patients.xlsx"  # Output file for matched patients' rows

# Get list of all EDF files
edf_files = glob.glob(os.path.join(edf_dir, "*.edf"))

# List to collect matched DOC_NO
matched_doc_nos = []

# Process each EDF file with progress bar
for edf_file in tqdm(edf_files, desc="Processing EDF files"):
    print(f"Processing {edf_file}...")

    # Extract EDF metadata
    edf_name, edf_birth_suffix = extract_edf_metadata(edf_file)
    if edf_name is None:
        continue  # Skip if EDF file couldn't be read
    edf_name_std = unidecode.unidecode(edf_name).upper()

    # Match EDF with aggregated sheet
    if edf_name_std and edf_birth_suffix and 'BIRTH_YEAR' in aggregated_sheet.columns and aggregated_sheet['BIRTH_YEAR'].notnull().any():
        # Match with both name and birth year suffix
        matched_row = aggregated_sheet[
            (aggregated_sheet['PATIENT_NAME_STD'] == edf_name_std) &
            (aggregated_sheet['BIRTH_YEAR'].str.endswith(str(edf_birth_suffix), na=False))
        ]
    else:
        # Match with name only if birth year is unavailable
        matched_row = aggregated_sheet[
            (aggregated_sheet['PATIENT_NAME_STD'] == edf_name_std)
        ]

    if not matched_row.empty:
        print("Matched patient:")
        print(matched_row.iloc[0])
        
        # Collect matched DOC_NO (unique per patient)
        doc_no = matched_row['DOC_NO'].iloc[0]
        if doc_no not in matched_doc_nos:
            matched_doc_nos.append(doc_no)
    else:
        print("No match found for EDF:", edf_file)

# After processing all files, create new XLSX with original rows of matched patients
if matched_doc_nos:
    matched_patients_df = clinical_sheet_original[clinical_sheet_original['DOC_NO'].isin(matched_doc_nos)]
    matched_patients_df.to_excel(matched_patients_xlsx, index=False)
    print(f"New XLSX created for matched patients: {matched_patients_xlsx}")
    print(f"Number of matched patients: {len(matched_doc_nos)}")
else:
    print("No matched patients found.")