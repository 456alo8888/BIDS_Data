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
from collections import defaultdict

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
        "--anonymous_xlsx_path",
        type=str,
        default= "./anonymous_patients.xlsx",
        required=True,
        help="Anonymous XLSX test_result directory"
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
    xlsx_path = args.anonymous_xlsx_path
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
    Create a BIDS dataset from EDF files, grouping files in the same parent folder as one subject.
    Match only one file per group with xlsx; if no match, use EDF metadata or placeholder.
    Save matched rows to phenotype/results.tsv with test results from HFL_NAME, PARA_RESULT, UNIT.
    Create phenotype/results.json. Copy original EDF files without anonymization or export.
    """
    edf_dir = args.edf_dir
    bids_dir = args.bids_dir
    doc_no_col = 'DOC_NO'
    patient_name_col = 'PATIENT_NAME'
    birth_date_col = 'BIRTH_DATE'
    GENDER_col = 'GENDER'
    hfl_name_col = 'HFL_NAME'
    para_result_col = 'PARA_RESULT'
    unit_col = 'UNIT'

    # Create BIDS root
    os.makedirs(bids_dir, exist_ok=True)

    # Load existing participants.tsv
    participants_tsv = os.path.join(bids_dir, "participants.tsv")
    existing_participants = []
    if os.path.exists(participants_tsv):
        existing_df = pd.read_csv(participants_tsv, sep='\t')
        existing_participants = existing_df.to_dict('records')
        logging.info(f"Loaded existing participants.tsv with {len(existing_participants)} entries")

    # Load Excel data
    df = load_patient_xlsx(args)
    df["PATIENT_NAME_STD"] = df[patient_name_col].apply(lambda x: standardize_name(x, remove_spaces=False))
    df['BIRTH_YEAR'] = df[birth_date_col].apply(extract_birth_year_suffix)

    # Keep all rows (no aggregation) to preserve multiple test results per DOC_NO
    # Filter relevant columns
    excel_data = df[[doc_no_col, 'PATIENT_NAME_STD', birth_date_col, GENDER_col, hfl_name_col, para_result_col]]

    # Collect EDF files recursively and group by parent folder
    edf_files = glob.glob(os.path.join(edf_dir, "**", "*.edf"), recursive=True)
    edf_files.extend(glob.glob(os.path.join(edf_dir, "**", "*.EDF"), recursive=True))
    edf_groups = defaultdict(list)
    for f in edf_files:
        parent = os.path.dirname(f)  # Folder cha trực tiếp chứa .edf
        edf_groups[parent].append(f)

    processed_edf_files = set()
    failed_files = []
    existing_subs = [d for d in os.listdir(bids_dir) if d.startswith('sub-') and os.path.isdir(os.path.join(bids_dir, d))]
    next_sub_id = len(existing_subs) + 1
    processed_sub_ids = {p['participant_id'] for p in existing_participants}
    anonymous_data = existing_participants

    # Process each group (folder cha)
    for folder, files in tqdm(edf_groups.items(), desc="Processing EDF folders"):
        if not files:
            continue

        # Check if group already processed
        if any(f in processed_edf_files for f in files):
            logging.info(f"Skipping group {folder}: Already processed")
            continue

        sub_id = f"{next_sub_id:04d}"
        next_sub_id += 1
        sub_dir = os.path.join(bids_dir, f"sub-{sub_id}")
        eeg_dir = os.path.join(sub_dir, "eeg")
        os.makedirs(eeg_dir, exist_ok=True)

        participant_info = None
        run_counter = 1
        scans_data = []
        matched_rows = None

        # Try to extract metadata from first valid file
        for edf_file in sorted(files):
            try:
                name_only, sex, birth_suffix, sampling_rate, channel_types, recording_date, raw = extract_edf_metadata(edf_file)
                if name_only is not None:
                    edf_name_std = unidecode.unidecode(name_only).upper()
                    # Match with Excel
                    if birth_suffix and 'BIRTH_YEAR' in excel_data.columns and excel_data['BIRTH_YEAR'].notnull().any():
                        matched_rows = excel_data[
                            (excel_data['PATIENT_NAME_STD'] == edf_name_std) &
                            (excel_data['BIRTH_YEAR'].str.endswith(str(birth_suffix), na=False))
                        ]
                    else:
                        matched_rows = excel_data[(excel_data['PATIENT_NAME_STD'] == edf_name_std)]

                    if not matched_rows.empty:
                        # Use matched info for participants.tsv
                        birth_date = matched_rows[birth_date_col].iloc[0]
                        age = calculate_age(birth_date, recording_date) if birth_date and recording_date else "n/a"
                        sex_str = matched_rows[GENDER_col].iloc[0].lower() if pd.notnull(matched_rows[GENDER_col].iloc[0]) else "n/a"
                        participant_info = {
                            "age": str(age) if age is not None else "n/a",
                            "sex": sex_str,
                            "group": "n/a"
                        }
                        # Save test results to phenotype/results.tsv
                        phenotype_dir = os.path.join(bids_dir, "phenotype")
                        os.makedirs(phenotype_dir, exist_ok=True)
                        results_tsv = os.path.join(phenotype_dir, "results.tsv")
                        test_data = []
                        for _, row in matched_rows.iterrows():
                            if pd.notnull(row[hfl_name_col]) and pd.notnull(row[para_result_col]):
                                test_data.append({
                                    'participant_id': f"sub-{sub_id}",
                                    'test_name': str(row[hfl_name_col]),
                                    'result': str(row[para_result_col]),
                                    'unit': str(row[unit_col]) if pd.notnull(row.get(unit_col)) else 'n/a'
                                })
                        matched_df = pd.DataFrame(test_data)
                        if os.path.exists(results_tsv):
                            existing_results = pd.read_csv(results_tsv, sep='\t')
                            matched_df = pd.concat([existing_results, matched_df], ignore_index=True)
                        matched_df.to_csv(results_tsv, sep='\t', index=False)
                        logging.info(f"Saved matched test results for sub-{sub_id} to {results_tsv}")
                    else:
                        # Use EDF metadata
                        logging.warning(f"No match for group {folder}. Using EDF metadata.")
                        with open(os.path.join(bids_dir, "unmatched_edf_groups.txt"), 'a') as f:
                            f.write(f"{folder}\n")
                        birth_year = extract_birth_year_suffix(raw.info.get('subject_info', {}), birth_suffix)
                        age = calculate_age(birth_year, recording_date) if birth_year else "n/a"
                        sex_str = ("male" if sex == 2 else "female") if sex is not None else "n/a"
                        participant_info = {
                            "age": str(age) if age is not None else "n/a",
                            "sex": sex_str,
                            "group": "n/a"
                        }
                    break  # Stop after finding first valid file
            except Exception as e:
                logging.warning(f"Failed to read metadata from {edf_file}: {e}")
                continue

        # If no valid metadata, use placeholder
        if participant_info is None:
            logging.warning(f"No valid metadata for group {folder}. Using placeholder.")
            with open(os.path.join(bids_dir, "unmatched_edf_groups.txt"), 'a') as f:
                f.write(f"{folder}\n")
            participant_info = {
                "age": "n/a",
                "sex": "n/a",
                "group": "n/a"
            }

        # Add participant info
        anonymous_data.append({
            "participant_id": f"sub-{sub_id}",
            **participant_info
        })
        processed_sub_ids.add(f"sub-{sub_id}")

        # Process all files in group as runs
        for edf_file in sorted(files):
            try:
                name_only, sex, birth_suffix, sampling_rate, channel_types, recording_date, raw = extract_edf_metadata(edf_file)
                success = True
            except Exception:
                name_only = None
                success = False
                failed_files.append(edf_file)

            run_id = f"{run_counter:03d}"
            bids_base = f"sub-{sub_id}_task-rest_run-{run_id}"
            bids_edf = os.path.join(eeg_dir, f"{bids_base}_eeg.edf")

            # Copy original file
            shutil.copy(edf_file, bids_edf)
            logging.info(f"Copied original EDF to {bids_edf}")

            # Create eeg.json and channels.tsv
            if name_only is None:
                eeg_metadata = {
                    "TaskName": "rest",
                    "EEGReference": "unknown",
                    "SamplingFrequency": "n/a",
                    "PowerLineFrequency": "n/a",
                    "EEGChannelCount": "n/a",
                    "SoftwareFilters": "n/a",
                    "RecordingDuration": "n/a",
                    "Note": "⚠️ EDF file could not be parsed"
                }
                channels_data = pd.DataFrame([{
                    "name": "n/a",
                    "type": "n/a",
                    "units": "n/a",
                    "description": "EDF file not readable",
                    "sampling_frequency": "n/a",
                    "reference": "unknown"
                }])
            else:
                eeg_metadata = {
                    "TaskName": "rest",
                    "EEGReference": "unknown",
                    "SamplingFrequency": sampling_rate,
                    "PowerLineFrequency": 50,
                    "EEGChannelCount": len(channel_types),
                    "SoftwareFilters": "n/a",
                    "RecordingDuration": raw.times[-1] if raw.times.size > 0 else "n/a"
                }
                channels_data = pd.DataFrame({
                    "name": list(channel_types.keys()),
                    "type": list(channel_types.values()),
                    "units": ["uV"] * len(channel_types),
                    "description": ["EEG channel"] * len(channel_types),
                    "sampling_frequency": [sampling_rate] * len(channel_types),
                    "reference": ["unknown"] * len(channel_types)
                })

            with open(os.path.join(eeg_dir, f"{bids_base}_eeg.json"), 'w') as f:
                json.dump(eeg_metadata, f, indent=4)
            channels_data.to_csv(os.path.join(eeg_dir, f"{bids_base}_channels.tsv"), sep='\t', index=False)

            # Add to scans.tsv
            acq_time = recording_date.strftime("%Y-%m-%dT%H:%M:%S") if success and recording_date and isinstance(recording_date, datetime) else "n/a"
            scans_data.append({
                "filename": os.path.relpath(bids_edf, start=sub_dir),
                "acq_time": acq_time
            })

            processed_edf_files.add(edf_file)
            run_counter += 1

        # Write scans.tsv
        scans_df = pd.DataFrame(scans_data)
        scans_tsv = os.path.join(sub_dir, f"sub-{sub_id}_scans.tsv")
        scans_df.to_csv(scans_tsv, sep='\t', index=False)

    # Write failed files
    if failed_files:
        pd.DataFrame({"failed_file": failed_files}).to_csv(os.path.join(bids_dir, "failed_files.tsv"), sep='\t', index=False)
        print(f"⚠️ {len(failed_files)} EDF files failed to parse. See failed_files.tsv")

    # Sort and write participants.tsv
    anonymous_data = sorted(anonymous_data, key=lambda x: int(x["participant_id"].split('-')[1]))
    participants_df = pd.DataFrame(anonymous_data)
    participants_df.to_csv(participants_tsv, sep='\t', index=False)

    # Write participants.json
    participants_json = {
        "participant_id": {"Description": "Unique participant identifier"},
        "age": {"Description": "Age of the participant in years", "Units": "years"},
        "sex": {"Description": "Sex of the participant (male, female, or n/a)"},
        "group": {"Description": "Clinical group"}
    }
    with open(os.path.join(bids_dir, "participants.json"), 'w') as f:
        json.dump(participants_json, f, indent=4)

    # Write phenotype/results.json
    results_json = {
        "participant_id": {"Description": "Unique identifier for each participant"},
        "test_name": {"Description": "Name of the lab test"},
        "result": {"Description": "Result of the lab test"},
        "unit": {"Description": "Unit of measurement for the result"}
    }
    phenotype_dir = os.path.join(bids_dir, "phenotype")
    os.makedirs(phenotype_dir, exist_ok=True)
    with open(os.path.join(phenotype_dir, "results.json"), 'w') as f:
        json.dump(results_json, f, indent=4)
        logging.info("Created phenotype/results.json")

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

    print(f"Completed. Participants saved to: {participants_tsv}")
    logging.info(f"Completed BIDS creation. Participants saved to: {participants_tsv}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Create BIDS dataset from EDF files")
    parser.add_argument('--edf_dir', type=str, required=True, help="Directory containing EDF files")
    parser.add_argument('--bids_dir', type=str, required=True, help="Output BIDS directory")
    parser.add_argument('--anonymous_xlsx_path', type=str, required=True, help="Path to Excel file with patient info")
    args = parser.parse_args()
    create_bids(args)