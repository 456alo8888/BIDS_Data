import pandas as pd
import mne
import re
import unidecode
import os
import json
from datetime import datetime
import shutil
import glob
from tqdm import tqdm

# === Configuration ===
# Paths
xlsx_path = "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/kqcls/matched_patients_translated_clean.xlsx"
edf_dir = "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/test"
bids_dir = "./test_bids"
anonymous_xlsx_path = "./anonymous_patients.xlsx"
edf_mapping_path = "./edf_mapping.csv"  # New file to store EDF filename to anonymous ID mapping

# English column names
doc_no_col = 'DOC_NO'
patient_name_col = 'PATIENT_NAME'
birth_date_col = 'BIRTH_DATE'
gender_col = 'GENDER'  # Fixed case to match your code
hfl_name_col = 'HFL_NAME'
para_result_col = 'PARA_RESULT'
unit_col = 'UNIT'  # Optional

# === Functions ===
def extract_edf_metadata(edf_file):
    try:
        raw = mne.io.read_raw_edf(edf_file, preload=True, verbose=False)
        subject_info = raw.info.get('subject_info', {})

        # Debugging: Verify raw object type
        print(f"Loaded EDF {edf_file}: raw object type = {type(raw)}")

        # Extract last_name and birth suffix
        last_name_raw = subject_info.get('last_name', '')
        match_num = re.search(r'^(.*?)[_]?(\d+)?$', last_name_raw)
        name_only = match_num.group(1).replace("_", " ").strip() if match_num else None
        birth_suffix = match_num.group(2) if match_num else None

        # Extract additional metadata
        sampling_rate = raw.info['sfreq']
        channel_types = {ch: 'eeg' for ch in raw.ch_names}
        recording_date = raw.info.get('meas_date')

        return name_only, birth_suffix, sampling_rate, channel_types, recording_date, raw
    except Exception as e:
        print(f"Error reading EDF file {edf_file}: {e}")
        return None, None, None, None, None, None

def extract_birth_year_suffix(birth_date):
    try:
        birth_date = pd.to_datetime(birth_date)
        return str(birth_date.year)[-4:]  # Last 4 digits of year
    except:
        return None

def calculate_age(birth_date, recording_date):
    try:
        birth_date = pd.to_datetime(birth_date).to_pydatetime().replace(tzinfo=None)
        if recording_date:
            if hasattr(recording_date, "tzinfo") and recording_date.tzinfo is not None:
                recording_date = recording_date.replace(tzinfo=None)
            age = recording_date.year - birth_date.year - (
                (recording_date.month, recording_date.day) < (birth_date.month, birth_date.day)
            )
            return age if age >= 0 else "n/a"
        return "n/a"
    except Exception as e:
        print(f"Error in calculate_age: {e}")
        return "n/a"

def standardize_name(name: str, remove_spaces: bool = False) -> str:
    if pd.isna(name):
        return None
    clean = unidecode.unidecode(str(name)).upper()
    clean = " ".join(clean.split())
    if remove_spaces:
        clean = clean.replace(" ", "")
    return clean

# === Main Script ===
# Load matched patients XLSX
try:
    df = pd.read_excel(xlsx_path)
except Exception as e:
    print(f"Error reading XLSX file {xlsx_path}: {e}")
    exit()

# Strip leading/trailing spaces from column names
df.columns = df.columns.str.strip()

# Print column names for debugging
print("Columns in matched patients XLSX:", df.columns.tolist())

# Check for required columns
required_columns = [doc_no_col, patient_name_col, birth_date_col, gender_col, hfl_name_col, para_result_col]
missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    print(f"Error: Missing required columns: {missing_columns}")
    exit()

# Standardize patient names for matching
df['PATIENT_NAME_STD'] = df[patient_name_col].apply(lambda x: standardize_name(x, remove_spaces=False))

# Create BIRTH_YEAR suffix
df['BIRTH_YEAR'] = df[birth_date_col].apply(extract_birth_year_suffix)

# Print sample data for debugging matching
print("Sample XLSX data:")
print(df[[patient_name_col, 'PATIENT_NAME_STD', 'BIRTH_YEAR']].head())
print("Number of unique patients:", len(df[doc_no_col].unique()))

# Group by DOC_NO for matching
aggregated_df = df.groupby(doc_no_col).agg({
    'PATIENT_NAME_STD': 'first',
    birth_date_col: 'first',
    gender_col: 'first'
}).reset_index()

# Get list of EDF files
edf_files = glob.glob(os.path.join(edf_dir, "*.edf"))
print(f"Found {len(edf_files)} EDF files in {edf_dir}")

# Map DOC_NO to anonymous sub-ID and EDF filename to sub-ID
sub_id_mapping = {}
edf_mapping = []  # List to store original EDF filename to sub-ID mapping
existing_subs = [d for d in os.listdir(bids_dir) if d.startswith('sub-') and os.path.isdir(os.path.join(bids_dir, d))]
next_sub_id = len(existing_subs) + 1

# Collect anonymous patient data
anonymous_data = []

# Process EDF files with progress bar
for edf_file in tqdm(edf_files, desc="Processing EDF files"):
    print(f"Processing {edf_file}...")

    # Extract EDF metadata
    name_only, birth_suffix, sampling_rate, channel_types, recording_date, raw = extract_edf_metadata(edf_file)
    if name_only is None or raw is None:
        print(f"Skipping {edf_file}: Invalid metadata or raw object")
        with open("./unmatched_edf_files.txt", 'a') as f:
            f.write(f"{edf_file}\n")
        continue
    edf_name_std = unidecode.unidecode(name_only).upper()

    # Debug matching inputs
    print(f"EDF name: {name_only} (std: {edf_name_std}), birth_suffix: {birth_suffix}")

    # Match with aggregated_df
    if birth_suffix and 'BIRTH_YEAR' in aggregated_df.columns and aggregated_df['BIRTH_YEAR'].notnull().any():
        matched_row = aggregated_df[
            (aggregated_df['PATIENT_NAME_STD'] == edf_name_std) &
            (aggregated_df['BIRTH_YEAR'].str.endswith(str(birth_suffix), na=False))
        ]
    else:
        matched_row = aggregated_df[
            (aggregated_df['PATIENT_NAME_STD'] == edf_name_std)
        ]

    # Debug matching output
    print(f"Number of matches: {len(matched_row)}")
    if not matched_row.empty:
        print(f"Sample matched row: {matched_row.iloc[0].to_dict()}")

    if matched_row.empty:
        print(f"No match found for EDF: {edf_file}")
        with open("./unmatched_edf_files.txt", 'a') as f:
            f.write(f"{edf_file}\n")
        continue

    # Safe to access iloc[0]
    doc_no = matched_row[doc_no_col].iloc[0]

    # Assign anonymous sub-ID
    if doc_no not in sub_id_mapping:
        sub_id = f"{next_sub_id:02d}"
        sub_id_mapping[doc_no] = sub_id
        edf_mapping.append({"original_edf_filename": os.path.basename(edf_file), "anonymous_id": f"sub-{sub_id}"})
        next_sub_id += 1
    sub_id = sub_id_mapping[doc_no]

    # Anonymize EDF
    try:
        raw.anonymize(keep_his=False, daysback=40000, verbose=True)  # Remove all patient info
        print(f"Anonymized EDF {edf_file}: subject_info = {raw.info.get('subject_info', {})}")
    except Exception as e:
        print(f"Error anonymizing {edf_file}: {e}")
        continue

    # Create BIDS directory for subject
    sub_dir = os.path.join(bids_dir, f"sub-{sub_id}")
    eeg_dir = os.path.join(sub_dir, "eeg")
    os.makedirs(eeg_dir, exist_ok=True)

    # Export anonymized EDF
    bids_edf = os.path.join(eeg_dir, f"sub-{sub_id}_task-rest_eeg.edf")
    try:
        raw.export(bids_edf, fmt='EDF', overwrite=True)
    except Exception as e:
        print(f"Warning: Failed to export EDF {edf_file}: {e}")
        print(f"Copying original EDF file instead.")
        shutil.copy(edf_file, bids_edf)

    # Create eeg.json
    eeg_metadata = {
        "TaskName": "rest",
        "EEGReference": "unknown",
        "SamplingFrequency": sampling_rate if sampling_rate else "n/a",
        "PowerLineFrequency": 50,
        "EEGChannelCount": len(channel_types) if channel_types else 0,
        "SoftwareFilters": "n/a",
        "RecordingDuration": raw.times[-1] if raw.times.size > 0 else "n/a"
    }
    with open(os.path.join(eeg_dir, f"sub-{sub_id}_task-rest_eeg.json"), 'w') as f:
        json.dump(eeg_metadata, f, indent=4)

    # Create channels.tsv
    channels_data = pd.DataFrame({
        "name": list(channel_types.keys()) if channel_types else [],
        "type": list(channel_types.values()) if channel_types else [],
        "units": ["uV"] * len(channel_types) if channel_types else [],
        "description": ["EEG channel"] * len(channel_types) if channel_types else [],
        "sampling_frequency": [sampling_rate] * len(channel_types) if channel_types else [],
        "reference": ["unknown"] * len(channel_types) if channel_types else []
    })
    channels_data.to_csv(os.path.join(eeg_dir, f"sub-{sub_id}_task-rest_channels.tsv"), sep='\t', index=False)

    # Calculate age
    birth_date = matched_row[birth_date_col].iloc[0]
    age = calculate_age(birth_date, recording_date)

    # Add to anonymous data for participants.tsv
    anonymous_data.append({
        "participant_id": f"sub-{sub_id}",
        "age": age,
        "sex": matched_row[gender_col].iloc[0].lower() if pd.notnull(matched_row[gender_col].iloc[0]) else "n/a",
        "group": "n/a"
    })

# Save EDF filename to sub-ID mapping
pd.DataFrame(edf_mapping).to_csv(edf_mapping_path, index=False)
print(f"EDF mapping saved to: {edf_mapping_path}")

# Create BIDS root files
os.makedirs(bids_dir, exist_ok=True)

# Create dataset_description.json
dataset_description = {
    "Name": "EEG Clinical Dataset",
    "BIDSVersion": "1.8.0",
    "Authors": ["Your Name"],
    "DatasetType": "raw"
}
with open(os.path.join(bids_dir, "dataset_description.json"), 'w') as f:
    json.dump(dataset_description, f, indent=4)

# Create participants.tsv and participants.json
participants_df = pd.DataFrame(anonymous_data)
participants_df.to_csv(os.path.join(bids_dir, "participants.tsv"), sep='\t', index=False)

participants_json = {
    "participant_id": {"Description": "Unique identifier for each participant"},
    "age": {"Description": "Age of the participant in years", "Units": "years"},
    "sex": {"Description": "Sex of the participant (male, female, or n/a)"},
    "group": {"Description": "Clinical group"}
}
with open(os.path.join(bids_dir, "participants.json"), 'w') as f:
    json.dump(participants_json, f, indent=4)

# Create phenotype/lab_results.tsv and lab_results.json
phenotype_dir = os.path.join(bids_dir, "phenotype")
os.makedirs(phenotype_dir, exist_ok=True)

lab_results_data = []
for doc_no, sub_id in sub_id_mapping.items():
    patient_rows = df[df[doc_no_col] == doc_no]
    for _, row in patient_rows.iterrows():
        lab_results_data.append({
            "participant_id": f"sub-{sub_id}",
            "test_name": row[hfl_name_col] if pd.notnull(row[hfl_name_col]) else "n/a",
            "result": row[para_result_col] if pd.notnull(row[para_result_col]) else "n/a",
            "unit": row[unit_col] if unit_col in df.columns and pd.notnull(row[unit_col]) else "n/a"
        })

lab_results_df = pd.DataFrame(lab_results_data)
lab_results_df.to_csv(os.path.join(phenotype_dir, "lab_results.tsv"), sep='\t', index=False)

lab_results_json = {
    "participant_id": {"Description": "Unique identifier for each participant"},
    "test_name": {"Description": "Name of the lab test"},
    "result": {"Description": "Result of the lab test"},
    "unit": {"Description": "Unit of measurement for the result"}
}
with open(os.path.join(phenotype_dir, "lab_results.json"), 'w') as f:
    json.dump(lab_results_json, f, indent=4)

# Create anonymized XLSX
anonymous_df = df.copy()
anonymous_df[doc_no_col] = anonymous_df[doc_no_col].map(sub_id_mapping).fillna(anonymous_df[doc_no_col])
anonymous_df = anonymous_df.drop(columns=[patient_name_col, birth_date_col], errors='ignore')
anonymous_df.to_excel(anonymous_xlsx_path, index=False)
print(f"Anonymous XLSX saved to: {anonymous_xlsx_path}")

print(f"BIDS dataset created in: {bids_dir}")