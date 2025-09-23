import pandas as pd
import mne
import re
import unidecode
import os
import json
from datetime import datetime
import pathlib
import shutil

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

# === Function to create BIDS directory structure ===
def create_bids_structure(output_dir, sub_id, edf_file, matched_row, sampling_rate, channel_types, recording_date):
    # Create BIDS root directory
    os.makedirs(output_dir, exist_ok=True)

    # Create subject directory
    sub_dir = os.path.join(output_dir, f"sub-{sub_id}")
    eeg_dir = os.path.join(sub_dir, "eeg")
    os.makedirs(eeg_dir, exist_ok=True)

    # Copy EDF file to BIDS structure
    edf_basename = os.path.basename(edf_file)
    bids_edf = os.path.join(eeg_dir, f"sub-{sub_id}_task-rest_eeg.edf")
    shutil.copy(edf_file, bids_edf)

    # Create participants.tsv (if not exists)
    participants_file = os.path.join(output_dir, "participants.tsv")
    participant_data = {
        "participant_id": f"sub-{sub_id}",
        "age": calculate_age(matched_row['BIRTH_DATE'].iloc[0], recording_date) if 'BIRTH_DATE' in matched_row and pd.notnull(matched_row['BIRTH_DATE'].iloc[0]) else "n/a",
        "sex": matched_row['GENDER'].iloc[0].lower() if 'GENDER' in matched_row and pd.notnull(matched_row['GENDER'].iloc[0]) else "n/a",
        "group": matched_row['PARA_RESULT'].iloc[0] if 'PARA_RESULT' in matched_row and pd.notnull(matched_row['PARA_RESULT'].iloc[0]) else "n/a"
    }
    if not os.path.exists(participants_file):
        pd.DataFrame([participant_data]).to_csv(participants_file, sep='\t', index=False)
    else:
        participants_df = pd.read_csv(participants_file, sep='\t')
        if f"sub-{sub_id}" not in participants_df['participant_id'].values:
            participants_df = pd.concat([participants_df, pd.DataFrame([participant_data])], ignore_index=True)
            participants_df.to_csv(participants_file, sep='\t', index=False)

    # Create dataset_description.json
    dataset_description = {
        "Name": "EEG Clinical Dataset",
        "BIDSVersion": "1.8.0",
        "Authors": ["Your Name"],
        "DatasetType": "raw"
    }
    with open(os.path.join(output_dir, "dataset_description.json"), 'w') as f:
        json.dump(dataset_description, f, indent=4)

    # Create EEG metadata JSON
    eeg_metadata = {
        "TaskName": "rest",
        "EEGReference": "unknown",  # Update if known
        "SamplingFrequency": sampling_rate,
        "PowerLineFrequency": 50,  # Update based on your region (50 Hz for Europe, 60 Hz for US)
        "EEGChannelCount": len(channel_types),
        "SoftwareFilters": "n/a",
        "RecordingDuration": mne.io.read_raw_edf(edf_file, preload=False).times[-1]
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

# === Function to calculate age ===
def calculate_age(birth_date, recording_date):
    try:
        birth_date = pd.to_datetime(birth_date)
        if recording_date:
            age = (recording_date - birth_date).days // 365
        else:
            age = (datetime.now() - birth_date).days // 365
        return age if age >= 0 else "n/a"
    except:
        return "n/a"

# === Function to extract birth year suffix ===
def extract_birth_year_suffix(birth_date):
    try:
        birth_date = pd.to_datetime(birth_date)
        return str(birth_date.year)[-4:]  # Extract last 4 digits of year
    except:
        return None

# === Main Script ===
# Read clinical sheet
clinical_sheet = pd.read_excel("/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/kqcls/matched_patients_translated_clean.xlsx")
print(clinical_sheet.columns)
# Standardize names


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
clinical_sheet["PATIENT_NAME_STD"] = clinical_sheet["PATIENT_NAME"].apply(
    lambda x: standardize_name(x, remove_spaces=False)
)
# clinical_sheet['PATIENT_NAME_STD'] = clinical_sheet['PATIENT_NAME'].apply(lambda x: unidecode.unidecode(str(x)).upper())

# Try to create BIRTH_YEAR if BIRTH_DATE exists
if 'BIRTH_DATE' in clinical_sheet.columns:
    clinical_sheet['BIRTH_YEAR'] = clinical_sheet['BIRTH_DATE'].apply(lambda x: extract_birth_year_suffix(x))
else:
    clinical_sheet['BIRTH_YEAR'] = None  # Set to None if BIRTH_DATE is missing

# Aggregate rows with the same DOC_NO
clinical_sheet = clinical_sheet.groupby('DOC_NO').agg({
    'PATIENT_NAME_STD': 'first',
    'BIRTH_DATE': 'first',
    'GENDER': 'first',
    'HFL_NAME': 'first',
    'PARA_RESULT': lambda x: ';'.join(x.dropna()) if x.notnull().any() else 'n/a'  # Combine PARA_RESULT
}).reset_index()

# Process EDF file
edf_file = "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/EEG2100/edf_files/FA5550A0_1-1+.edf"
output_bids_dir = "./bids_database"  # Specify your BIDS output directory

# Extract EDF metadata
edf_name, edf_birth_suffix, edf_sex, sampling_rate, channel_types, recording_date = extract_edf_metadata(edf_file)
edf_name_std = unidecode.unidecode(edf_name).upper() if edf_name else None

# Match EDF with clinical sheet
if edf_name_std and edf_birth_suffix and 'BIRTH_YEAR' in clinical_sheet.columns and clinical_sheet['BIRTH_YEAR'].notnull().any():
    # Match with both name and birth year suffix
    matched_row = clinical_sheet[
        (clinical_sheet['PATIENT_NAME_STD'] == edf_name_std) &
        (clinical_sheet['BIRTH_YEAR'].str.endswith(str(edf_birth_suffix), na=False))
    ]
else:
    # Match with name only if birth year is unavailable
    matched_row = clinical_sheet[
        (clinical_sheet['PATIENT_NAME_STD'] == edf_name_std)
    ]

if not matched_row.empty:
    print("Matched patient:")
    print(matched_row.iloc[0])
    
    # Generate subject ID (e.g., based on DOC_NO)
    sub_id = matched_row['DOC_NO'].iloc[0] if 'DOC_NO' in matched_row else f"{len(os.listdir(output_bids_dir)) + 1:04d}"
    
    # Create BIDS structure
    create_bids_structure(output_bids_dir, sub_id, edf_file, matched_row, sampling_rate, channel_types, recording_date)
    print(f"BIDS dataset created for sub-{sub_id} in {output_bids_dir}")
else:
    print("No match found for EDF:", edf_file)