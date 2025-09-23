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
xlsx_path = "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/kqcls/matched_patients_translated_clean.xlsx"  # Replace with your matched patients XLSX path
edf_dir = "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/test"  # Replace with your EDF folder path
bids_dir = "./test_bids"  # BIDS output directory
anonymous_xlsx_path = "./anonymous_patients.xlsx"  # Anonymous XLSX output

# English column names
doc_no_col = 'DOC_NO'
patient_name_col = 'PATIENT_NAME'
birth_date_col = 'BIRTH_DATE'
GENDER_col = 'GENDER'
hfl_name_col = 'HFL_NAME'
para_result_col = 'PARA_RESULT'
unit_col = 'UNIT'  # Optional



# === Function to extract EDF metadata ===
def extract_edf_metadata(edf_file):
    raw = mne.io.read_raw_edf(edf_file, preload=False, verbose=False)
    subject_info = raw.info.get('subject_info', {})

    # Extract sex
    sex = subject_info.get('sex')
    sex_str = "female" if sex == 0 else "male" if sex == 1 else "n/a"

    # Extract last_name and birth suffix
    last_name_raw = subject_info.get('last_name')
    match_num = re.search(r'^(.*?)[_]?(\d+)?$', last_name_raw)
    name_only = match_num.group(1).replace("_", " ").strip() if match_num else None
    birth_suffix = match_num.group(2) if match_num else None

    # Extract additional EEG metadata
    sampling_rate = raw.info['sfreq']
    channel_types = {ch: 'eeg' for ch in raw.ch_names}  # Assuming all channels are EEG
    recording_date = raw.info.get('meas_date')

    return name_only, birth_suffix, sex_str, sampling_rate, channel_types, recording_date

def extract_birth_year_suffix(birth_date):
    try:
        birth_date = pd.to_datetime(birth_date)
        return str(birth_date.year)[-4:]  # Last 4 digits of year
    except:
        return None

def calculate_age(birth_date, recording_date):
    try:
        birth_date = pd.to_datetime(birth_date)
        if recording_date:
            age = (recording_date - birth_date).days // 365
            return age if age >= 0 else "n/a"
        return "n/a"
    except:
        return "n/a"

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
required_columns = [doc_no_col, patient_name_col, birth_date_col, GENDER_col, hfl_name_col, para_result_col]
missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    print(f"Error: Missing required columns: {missing_columns}")
    exit()


def standardize_name(name: str, remove_spaces: bool = False) -> str:
    """
    Chuẩn hóa tên bệnh nhân:
    - Bỏ dấu tiếng Việt
    - Viết hoa toàn bộ
    - Gộp khoảng trắng thừa
    - Tùy chọn: bỏ toàn bộ khoảng trắng
    """
    if pd.isna(name):
        return None
    
    # Bỏ dấu + uppercase
    clean = unidecode.unidecode(str(name)).upper()
    # Gộp khoảng trắng thừa
    clean = " ".join(clean.split())
    # Nếu cần bỏ hết dấu cách
    if remove_spaces:
        clean = clean.replace(" ", "")
    return clean

# Áp dụng cho cột PATIENT_NAME
df["PATIENT_NAME_STD"] = df["PATIENT_NAME"].apply(
    lambda x: standardize_name(x, remove_spaces=False)
)

# Create BIRTH_YEAR suffix
df['BIRTH_YEAR'] = df[birth_date_col].apply(extract_birth_year_suffix)

# Group by DOC_NO for matching
aggregated_df = df.groupby(doc_no_col).agg({
    'PATIENT_NAME_STD': 'first',
    birth_date_col: 'first',
    GENDER_col: 'first'
}).reset_index()

# Get list of EDF files
edf_files = glob.glob(os.path.join(edf_dir, "*.edf"))

# Map DOC_NO to anonymous sub-ID
sub_id_mapping = {}
existing_subs = [d for d in os.listdir(bids_dir) if d.startswith('sub-') and os.path.isdir(os.path.join(bids_dir, d))]
next_sub_id = len(existing_subs) + 1

# Collect anonymous patient data
anonymous_data = []

# Process EDF files with progress bar
for edf_file in tqdm(edf_files, desc="Processing EDF files"):
    print(f"Processing {edf_file}...")

    # Extract EDF metadata
    name_only, birth_suffix, sampling_rate, channel_types, recording_date, raw = extract_edf_metadata(edf_file)
    if name_only is None:
        continue
    edf_name_std = unidecode.unidecode(name_only).upper()

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

    # Match EDF with clinical sheet
    if edf_name_std and birth_suffix and 'BIRTH_YEAR' in df.columns and df['BIRTH_YEAR'].notnull().any():
        # Match with both name and birth year suffix
        matched_row = df[
            (df['PATIENT_NAME_STD'] == edf_name_std) &
            (df['BIRTH_YEAR'].str.endswith(str(birth_suffix), na=False))
        ]
    else:
        # Match with name only if birth year is unavailable
        matched_row = df[
            (df['PATIENT_NAME_STD'] == edf_name_std)
        ]

    # if matched_row.empty:
    #     print(f"No match found for EDF: {edf_file}")
    #     continue

    if not matched_row.empty:
        print("Matched patient:")
        print(matched_row.iloc[0])
        
        # Generate subject ID (e.g., based on DOC_NO)
        # sub_id = matched_row['DOC_NO'].iloc[0] if 'DOC_NO' in matched_row else f"{len(os.listdir(output_bids_dir)) + 1:04d}"
        
        # Create BIDS structure
        # create_bids_structure(output_bids_dir, sub_id, edf_file, matched_row, sampling_rate, channel_types, recording_date)
        # print(f"BIDS dataset created for sub-{sub_id} in {output_bids_dir}")
    else:
        print("No match found for EDF:", edf_file)

    doc_no = matched_row[doc_no_col].iloc[0]

    # Assign anonymous sub-ID
    if doc_no not in sub_id_mapping:
        sub_id = f"{next_sub_id:02d}"
        sub_id_mapping[doc_no] = sub_id
        next_sub_id += 1
    sub_id = sub_id_mapping[doc_no]

    # Anonymize EDF
    raw.anonymize()

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
        "SamplingFrequency": sampling_rate,
        "PowerLineFrequency": 50,  # Adjust if needed (50 Hz for Europe, 60 Hz for US)
        "EEGChannelCount": len(channel_types),
        "SoftwareFilters": "n/a",
        "RecordingDuration": raw.times[-1] if raw.times.size > 0 else "n/a"
    }
    with open(os.path.join(eeg_dir, f"sub-{sub_id}_task-rest_eeg.json"), 'w') as f:
        json.dump(eeg_metadata, f, indent=4)

    # Create channels.tsv
    channels_data = pd.DataFrame({
        "name": list(channel_types.keys()),
        "type": list(channel_types.values()),
        "units": ["uV"] * len(channel_types),
        "description": ["EEG channel"] * len(channel_types),
        "sampling_frequency": [sampling_rate] * len(channel_types),
        "reference": ["unknown"] * len(channel_types)
    })
    channels_data.to_csv(os.path.join(eeg_dir, f"sub-{sub_id}_task-rest_channels.tsv"), sep='\t', index=False)

    # Calculate age
    birth_date = matched_row[birth_date_col].iloc[0]
    age = calculate_age(birth_date, recording_date)

    # Add to anonymous data for participants.tsv
    anonymous_data.append({
        "participant_id": f"sub-{sub_id}",
        "age": age,
        "sex": matched_row[GENDER_col].iloc[0].lower() if pd.notnull(matched_row[GENDER_col].iloc[0]) else "n/a",
        "group": "n/a"  # Adjust if group info available
    })

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