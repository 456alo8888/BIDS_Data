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
import argparse
import logging

# === Configuration ===
# Paths

##============= UNCOMMENT THIS TO RUN=============================

# xlsx_path = "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/kqcls/matched_patients_translated_clean.xlsx"  # Replace with your matched patients XLSX path
# edf_dir = "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/EEG2100/edf_files"  # Replace with your EDF folder path
# bids_dir = "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/database_bids"  # BIDS output directory
# anonymous_xlsx_path = "./anonymous_patients.xlsx"  # Anonymous XLSX output

# English column names
doc_no_col = 'DOC_NO'
patient_name_col = 'PATIENT_NAME'
birth_date_col = 'BIRTH_DATE'
GENDER_col = 'GENDER'
hfl_name_col = 'HFL_NAME'
para_result_col = 'PARA_RESULT'
unit_col = 'UNIT'  # Optional






def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--edf_dir",
        type=str,
        default= "./test",
        required=True,
        help="Folder directory containing folder having .edf/.EDF files"
    )
    parser.add_argument(
        "--bids_dir",
        type=str,
        default= "./database_bids",
        required=True,
        help="BIDS database output directory"
    )
    parser.add_argument(
        "--test_result_dir",
        type = str , 
        default= "./matched_patients_translated_clean.xlsx",
        required = True,
        help = "Directory of test result"   
    )

    parser.add_argument(
        "--anonymous_xlsx_path",
        type=str,
        default= "./anonymous_patients.xlsx",
        required=True,
        help="Anonymous XLSX output directory"
    )
    return parser.parse_args()

# def extract_edf_metadata(edf_file):
#     try:
#         raw = mne.io.read_raw_edf(edf_file, preload=True, verbose=False)  # Preload for anonymization
#         subject_info = raw.info.get('subject_info', {})

#         # Debugging: Verify raw object type
#         print(f"Loaded EDF {edf_file}: raw object type = {type(raw)}")

#         # Extract last_name and birth suffix
#         last_name_raw = subject_info.get('last_name', '')
#         match_num = re.search(r'^(.*?)[_]?(\d+)?$', last_name_raw)
#         name_only = match_num.group(1).replace("_", " ").strip() if match_num else None
#         birth_suffix = match_num.group(2) if match_num else None

#         # Extract additional metadata
#         sampling_rate = raw.info['sfreq']
#         channel_types = {ch: 'eeg' for ch in raw.ch_names}
#         recording_date = raw.info.get('meas_date')

#         return name_only, birth_suffix, sampling_rate, channel_types, recording_date, raw
#     except Exception as e:
#         print(f"Error reading EDF file {edf_file}: {e}")
#         return None, None, None, None, None, None
    


def extract_edf_metadata(edf_file):
    try:
        raw = mne.io.read_raw_edf(edf_file, preload=False, verbose=False)
        subject_info = raw.info.get('subject_info', {}) or {}

        print(f"Loaded EDF {edf_file}: raw object type = {type(raw)}")

        # Lấy last_name / first_name / his_id
        last_name_raw = (
            subject_info.get('last_name') or
            subject_info.get('first_name') or
            subject_info.get('his_id') or ''
        )
        sex = subject_info.get('sex')

        if not last_name_raw:  # fallback sang filename
            last_name_raw = os.path.splitext(os.path.basename(edf_file))[0]

        match_num = re.search(r'^(.*?)[_]?(\d+)?$', last_name_raw)
        name_only = match_num.group(1).replace("_", " ").strip() if match_num else last_name_raw
        birth_suffix = match_num.group(2) if match_num else None

        sampling_rate = raw.info['sfreq']
        channel_types = {ch: 'eeg' for ch in raw.ch_names}
        recording_date = raw.info.get('meas_date')

        return name_only, sex, birth_suffix, sampling_rate, channel_types, recording_date , raw
    except Exception as e:
        print(f"Error reading EDF file {edf_file}: {e}")
        return None,None, None, None, None, None , None

def extract_birth_year_suffix(birth_date):
    try:
        birth_date = pd.to_datetime(birth_date)
        return str(birth_date.year)[-4:]  # Last 4 digits of year
    except:
        return None



def calculate_age(birth_date, recording_date):
    try:
        # Convert birth_date sang datetime (naive)
        birth_date = pd.to_datetime(birth_date).to_pydatetime().replace(tzinfo=None)

        if recording_date:
            # Chuẩn hóa recording_date (bỏ tzinfo nếu có)
            if hasattr(recording_date, "tzinfo") and recording_date.tzinfo is not None:
                recording_date = recording_date.replace(tzinfo=None)

            # Tính tuổi chính xác (dựa trên ngày tháng năm)
            age = recording_date.year - birth_date.year - (
                (recording_date.month, recording_date.day) < (birth_date.month, birth_date.day)
            )

            return age if age >= 0 else "n/a"

        return "n/a"
    except Exception as e:
        print(f"⚠️ Error in calculate_age: {e}")
        return "n/a"

# === Main Script ===
# Load matched patients XLSX
def load_patient_xlsx(args):
    xlsx_path = args.test_result_dir
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
    return df


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


def create_bids(args):
    """
    Create a BIDS dataset from EDF files, assigning a unique sub-id to each EDF file.
    Ensure participants.tsv is sorted with no duplicates.
    """
    edf_dir = args.edf_dir
    bids_dir = args.bids_dir
    doc_no_col = 'DOC_NO'  # Adjust if column name differs
    birth_date_col = 'BIRTH_DATE'  # Adjust if column name differs
    GENDER_col = 'GENDER'  # Adjust if column name differs

    # Create BIDS root files
    os.makedirs(bids_dir, exist_ok=True)

    # Load existing participants.tsv to avoid duplicates
    participants_tsv = os.path.join(bids_dir, "participants.tsv")
    existing_participants = []
    if os.path.exists(participants_tsv):
        existing_df = pd.read_csv(participants_tsv, sep='\t')
        existing_participants = existing_df.to_dict('records')
        logging.info(f"Loaded existing participants.tsv with {len(existing_participants)} entries")

    anonymous_xlsx_path = args.anonymous_xlsx_path
    df = load_patient_xlsx(args)

    # Standardize PATIENT_NAME
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
    edf_files = sorted(glob.glob(os.path.join(edf_dir, "*.edf")))  # Sort for consistency

    # Track processed EDF files and sub-IDs
    processed_edf_files = set()
    existing_subs = [d for d in os.listdir(bids_dir) if d.startswith('sub-') and os.path.isdir(os.path.join(bids_dir, d))]
    next_sub_id = len(existing_subs) + 1
    processed_sub_ids = {p['participant_id'] for p in existing_participants}

    # Collect anonymous patient data
    anonymous_data = existing_participants  # Start with existing participants

    # Process EDF files with progress bar
    for edf_file in tqdm(edf_files, desc="Processing EDF files"):
        logging.info(f"Processing {edf_file}...")
        print(f"Processing {edf_file}...")

        # Skip if EDF file already processed
        if edf_file in processed_edf_files:
            logging.info(f"Skipping {edf_file}: Already processed")
            continue

        # Check if EDF file is already in BIDS structure
        skip = False
        for sub_dir in existing_subs:
            potential_edf = os.path.join(bids_dir, sub_dir, "eeg", f"{sub_dir}_task-rest_eeg.edf")
            if os.path.exists(potential_edf):
                try:
                    if os.path.samefile(edf_file, potential_edf):
                        logging.info(f"Skipping {edf_file}: Already processed as {sub_dir}")
                        processed_edf_files.add(edf_file)
                        skip = True
                        break
                except OSError:
                    continue
        if skip:
            continue

        # Extract EDF metadata
        name_only, sex, birth_suffix, sampling_rate, channel_types, recording_date, raw = extract_edf_metadata(edf_file)
        if name_only is None:
            logging.warning(f"Skipping {edf_file}: Could not extract metadata")
            with open(os.path.join(bids_dir, "unmatched_edf_files.txt"), 'a') as f:
                f.write(f"{edf_file}\n")
            continue

        edf_name_std = unidecode.unidecode(name_only).upper()

        # Debug matching inputs
        logging.info(f"EDF name: {name_only} (std: {edf_name_std}), birth_suffix: {birth_suffix}")
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
        logging.info(f"Number of matches: {len(matched_row)}")
        print(f"Number of matches: {len(matched_row)}")
        if not matched_row.empty:
            logging.info(f"Sample matched row: {matched_row.iloc[0].to_dict()}")
            print(f"Sample matched row: {matched_row.iloc[0].to_dict()}")

        if matched_row.empty:
            logging.warning(f"No match found for EDF: {edf_file}")
            print(f"No match found for EDF: {edf_file}")
            with open(os.path.join(bids_dir, "unmatched_edf_files.txt"), 'a') as f:
                f.write(f"{edf_file}\n")
            continue

        # Assign unique sub-ID for each EDF file
        sub_id = f"{next_sub_id:04d}"
        next_sub_id += 1

        # Create BIDS directory for subject
        sub_dir = os.path.join(bids_dir, f"sub-{sub_id}")
        eeg_dir = os.path.join(sub_dir, "eeg")
        os.makedirs(eeg_dir, exist_ok=True)

        # Export anonymized EDF
        bids_edf = os.path.join(eeg_dir, f"sub-{sub_id}_task-rest_eeg.edf")
        try:
            raw.anonymize()
            raw.export(bids_edf, fmt='EDF', overwrite=True)
            logging.info(f"Exported anonymized EDF to {bids_edf}")
        except Exception as e:
            logging.warning(f"Failed to export EDF {edf_file}: {e}")
            print(f"Warning: Failed to export EDF {edf_file}: {e}")
            print(f"Copying original EDF file instead.")
            shutil.copy(edf_file, bids_edf)
            logging.info(f"Copied original EDF to {bids_edf}")

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
            logging.info(f"Created eeg.json for sub-{sub_id}")

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
        logging.info(f"Created channels.tsv for sub-{sub_id}")

        # Add to anonymous data
        birth_date = matched_row[birth_date_col].iloc[0]
        age = calculate_age(birth_date, recording_date)
        anonymous_data.append({
            "participant_id": f"sub-{sub_id}",
            "age": age,
            "sex": matched_row[GENDER_col].iloc[0].lower() if pd.notnull(matched_row[GENDER_col].iloc[0]) else "n/a",
            "group": "n/a"
        })
        processed_sub_ids.add(f"sub-{sub_id}")
        processed_edf_files.add(edf_file)
        logging.info(f"Added participant sub-{sub_id} for EDF {edf_file} to anonymous_data")

    # Sort anonymous_data by participant_id
    anonymous_data = sorted(anonymous_data, key=lambda x: int(x["participant_id"].split('-')[1]))

    # Write participants.tsv
    participants_df = pd.DataFrame(anonymous_data)
    participants_tsv = os.path.join(bids_dir, "participants.tsv")
    participants_df.to_csv(participants_tsv, sep='\t', index=False)
    logging.info(f"Wrote sorted participants.tsv to {participants_tsv}")

    # Write participants.json
    participants_json = {
        "participant_id": {"Description": "Unique participant identifier"},
        "age": {"Description": "Age of the participant in years at the time of recording", "Units": "years"},
        "sex": {"Description": "Sex of the participant", "Levels": {"male": "Male", "female": "Female", "n/a": "Not available"}},
        "group": {"Description": "Group affiliation of the participant", "Levels": {"n/a": "Not available"}}
    }
    with open(os.path.join(bids_dir, "participants.json"), 'w') as f:
        json.dump(participants_json, f, indent=4)
        logging.info(f"Created participants.json")

    # Write dataset_description.json
    dataset_description = {
        "Name": "Anonymous EEG Dataset",
        "BIDSVersion": "1.8.0",
        "DatasetType": "raw",
        "Authors": ["Anonymous"],
        "Funding": []
    }
    with open(os.path.join(bids_dir, "dataset_description.json"), 'w') as f:
        json.dump(dataset_description, f, indent=4)
        logging.info(f"Created dataset_description.json")

    print(f"Completed. Participants saved to: {participants_tsv}")
    logging.info(f"Completed BIDS creation. Participants saved to: {participants_tsv}")

if __name__ == "__main__":
    args = get_args()
    create_bids(args)
    print("DONE!")
