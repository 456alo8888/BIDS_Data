import os
import glob
import json
import pandas as pd
import shutil
import mne
from tqdm import tqdm
import unidecode
import logging
import re
import datetime

# Configure logging
logging.basicConfig(filename="create_bids_log.txt", level=logging.INFO, format="%(asctime)s - %(message)s")

def standardize_name(name, remove_spaces=False):
    """Standardize a name by removing special characters and optionally spaces."""
    if not name or pd.isna(name):
        return ""
    name = unidecode.unidecode(str(name)).upper()
    if remove_spaces:
        name = name.replace(" ", "")
    return name

def extract_birth_year_suffix(date):
    """Extract the last two digits of the birth year from a date string."""
    if pd.isna(date):
        return ""
    try:
        if isinstance(date, str):
            date = pd.to_datetime(date, errors='coerce')
        if isinstance(date, pd.Timestamp):
            return str(date.year)[-2:]
    except Exception:
        return ""
    return ""

def calculate_age(birth_date, recording_date):
    """Calculate age based on birth date and recording date."""
    try:
        if pd.isna(birth_date) or pd.isna(recording_date):
            return "n/a"
        birth_date = pd.to_datetime(birth_date, errors='coerce')
        recording_date = pd.to_datetime(recording_date, errors='coerce')
        if pd.isna(birth_date) or pd.isna(recording_date):
            return "n/a"
        age = recording_date.year - birth_date.year
        if (recording_date.month, recording_date.day) < (birth_date.month, birth_date.day):
            age -= 1
        return age if age >= 0 else "n/a"
    except Exception:
        return "n/a"

def extract_edf_metadata(edf_file):
    """Extract metadata from an EDF file."""
    try:
        raw = mne.io.read_raw_edf(edf_file, preload=True, verbose=False)
        header = raw.info
        name = header.get('subject_info', {}).get('his_id', None) or "Unknown"
        sex = header.get('subject_info', {}).get('sex', "n/a")
        birth_date = header.get('subject_info', {}).get('birthday', None)
        birth_suffix = extract_birth_year_suffix(birth_date)
        sampling_rate = header.get('sfreq', 0)
        channel_types = {ch: 'eeg' for ch in raw.ch_names}  # Adjust if needed
        recording_date = header.get('meas_date', None)
        return name, sex, birth_suffix, sampling_rate, channel_types, recording_date, raw
    except Exception as e:
        logging.error(f"Failed to read EDF {edf_file}: {e}")
        return None, None, None, None, None, None, None

def load_patient_xlsx(args):
    """Load patient data from XLSX file."""
    try:
        df = pd.read_excel(args.anonymous_xlsx_path)
        logging.info(f"Loaded patient data from {args.anonymous_xlsx_path}")
        return df
    except Exception as e:
        logging.error(f"Failed to load XLSX {args.anonymous_xlsx_path}: {e}")
        raise

def create_bids(args):
    """
    Create a BIDS dataset from EDF files, matching with patient data.
    Ensure participants.tsv is sorted by participant_id with no duplicates.
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
    edf_files = glob.glob(os.path.join(edf_dir, "*.edf"))

    # Map DOC_NO to anonymous sub-ID
    sub_id_mapping = {}
    existing_subs = [d for d in os.listdir(bids_dir) if d.startswith('sub-') and os.path.isdir(os.path.join(bids_dir, d))]
    next_sub_id = len(existing_subs) + 1

    # Track processed sub-IDs to avoid duplicates in anonymous_data
    processed_sub_ids = {p['participant_id'] for p in existing_participants}

    # Collect anonymous patient data
    anonymous_data = existing_participants  # Start with existing participants

    # Process EDF files with progress bar
    for edf_file in tqdm(edf_files, desc="Processing EDF files"):
        logging.info(f"Processing {edf_file}...")
        print(f"Processing {edf_file}...")

        # Skip if EDF file already processed (check if sub-id folder exists)
        sub_dir = None
        for sub_id in sub_id_mapping.values():
            potential_edf = os.path.join(bids_dir, f"sub-{sub_id}", "eeg", f"sub-{sub_id}_task-rest_eeg.edf")
            if os.path.exists(potential_edf) and os.path.samefile(edf_file, potential_edf):
                logging.info(f"Skipping {edf_file}: Already processed as sub-{sub_id}")
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

        # Assign anonymous sub-ID
        doc_no = matched_row[doc_no_col].iloc[0]
        if doc_no not in sub_id_mapping:
            sub_id = f"{next_sub_id:04d}"
            sub_id_mapping[doc_no] = sub_id
            next_sub_id += 1
        sub_id = sub_id_mapping[doc_no]

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

        # Add to anonymous data only if sub-id not yet processed
        if f"sub-{sub_id}" not in processed_sub_ids:
            birth_date = matched_row[birth_date_col].iloc[0]
            age = calculate_age(birth_date, recording_date)
            anonymous_data.append({
                "participant_id": f"sub-{sub_id}",
                "age": age,
                "sex": matched_row[GENDER_col].iloc[0].lower() if pd.notnull(matched_row[GENDER_col].iloc[0]) else "n/a",
                "group": "n/a"
            })
            processed_sub_ids.add(f"sub-{sub_id}")
            logging.info(f"Added participant sub-{sub_id} to anonymous_data")

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
    import argparse
    parser = argparse.ArgumentParser(description="Convert EDF files to BIDS format.")
    parser.add_argument('--edf_dir', type=str, required=True, help="Directory containing EDF files")
    parser.add_argument('--bids_dir', type=str, required=True, help="Output BIDS directory")
    parser.add_argument('--anonymous_xlsx_path', type=str, required=True, help="Path to XLSX file with anonymous patient data")
    args = parser.parse_args()
    create_bids(args)