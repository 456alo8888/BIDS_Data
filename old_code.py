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
    # parser.add_argument(
    #     "--data_name",
    #     type=str,
    #     default= "CMH",
    #     required=True,
    #     help="Type of dataset"
    # )
    return parser.parse_args()

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


def extract_birth_year(subject_info: dict, birth_suffix: str | None = None) -> int | None:
    """
    Trích năm sinh từ subject_info hoặc fallback bằng birth_suffix.
    - Nếu có birthday -> parse lấy năm.
    - Nếu không có -> thử birth_suffix, convert sang int, nếu >=1900 thì lấy làm năm sinh.
    """
    # Ưu tiên birthday trong subject_info
    if subject_info and isinstance(subject_info, dict):
        birthday = subject_info.get("birthday")
        if birthday:
            try:
                dt = pd.to_datetime(birthday, errors="coerce")
                if dt and not pd.isna(dt):
                    return dt.year
            except Exception:
                pass

    # Nếu không có birthday thì fallback sang birth_suffix
    if birth_suffix:
        try:
            year_candidate = int(birth_suffix)
            if year_candidate >= 1900 and year_candidate <= datetime.now().year:
                return year_candidate
        except ValueError:
            pass

    return None


def calculate_age(birth_year: int | None, recording_date: datetime | None) -> int | None:
    """
    Tính tuổi dựa vào birth_year và recording_date.
    Nếu không có birth_year hoặc recording_date thì trả về None.
    """
    if not birth_year or not recording_date:
        return None
    try:
        return recording_date.year - birth_year
    except Exception:
        return None



# def calculate_age(birth_year, recording_date):
#     try:
#         # Convert birth_date sang datetime (naive)
#         birth_date = pd.to_datetime(birth_date).to_pydatetime().replace(tzinfo=None)

#         if recording_date:
#             # Chuẩn hóa recording_date (bỏ tzinfo nếu có)
#             if hasattr(recording_date, "tzinfo") and recording_date.tzinfo is not None:
#                 recording_date = recording_date.replace(tzinfo=None)

#             # Tính tuổi chính xác (dựa trên ngày tháng năm)
#             age = recording_date.year - birth_date.year - (
#                 (recording_date.month, recording_date.day) < (birth_date.month, birth_date.day)
#             )

#             return age if age >= 0 else "n/a"

#         return "n/a"
#     except Exception as e:
#         print(f"⚠️ Error in calculate_age: {e}")
#         return "n/a"

# === Main Script ===



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


def build_database(args):
    edf_dir = args.edf_dir
    bids_dir = args.bids_dir
    # Create BIDS root files
    os.makedirs(bids_dir, exist_ok=True)

    # Lấy tất cả file .edf và .EDF trong edf_dir và các folder con
    edf_files = glob.glob(os.path.join(edf_dir, "**", "*.edf"), recursive=True)
    edf_files.extend(glob.glob(os.path.join(edf_dir, "**", "*.EDF"), recursive=True))



    # Create anonymous sub-ID for each .edf/.EDF files
    existing_subs = [d for d in os.listdir(bids_dir) if d.startswith('sub-') and os.path.isdir(os.path.join(bids_dir, d))]
    next_sub_id = len(existing_subs) + 1

    # Collect anonymous patient data
    anonymous_data = []
    failed_files = []
    # Process EDF files with progress bar
    for edf_file in tqdm(edf_files, desc="Processing EDF files"):
        print(f"Processing {edf_file}...")

        # Extract EDF metadata
        name_only, sex, birth_suffix, sampling_rate, channel_types, recording_date, raw = extract_edf_metadata(edf_file)
    
        # Assign anonymous sub-ID
        sub_id = f"{next_sub_id:04d}"
        next_sub_id += 1



        # Create BIDS directory for subject
        sub_dir = os.path.join(bids_dir, f"sub-{sub_id}")
        eeg_dir = os.path.join(sub_dir, "eeg")
        os.makedirs(eeg_dir, exist_ok=True)

        # Export anonymized EDF
        bids_edf = os.path.join(eeg_dir, f"sub-{sub_id}_task-rest_eeg.edf")
        shutil.copy(edf_file, bids_edf)


        if name_only is None:
            print(f"⚠️ Failed to read {edf_file}, creating placeholder metadata.")
            

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
            with open(os.path.join(eeg_dir, f"sub-{sub_id}_task-rest_eeg.json"), 'w') as f:
                json.dump(eeg_metadata, f, indent=4)

            # channels.tsv placeholder
            pd.DataFrame([{
                "name": "n/a",
                "type": "n/a",
                "units": "n/a",
                "description": "EDF file not readable",
                "sampling_frequency": "n/a",
                "reference": "unknown"
            }]).to_csv(os.path.join(eeg_dir, f"sub-{sub_id}_task-rest_channels.tsv"), sep='\t', index=False)
            # Luôn thêm vào participants.tsv
            anonymous_data.append({
                "participant_id": f"sub-{sub_id}",
                "age": "n/a",
                "sex": "n/a",
                "group": "n/a"
            })
            failed_files.append(sub_dir)

        else:
            #If metadata extracted from EDF file 
            edf_name_std = unidecode.unidecode(name_only).upper()
            print(f"EDF name: {name_only} (std: {edf_name_std}), birth_suffix: {birth_suffix} , sex: {sex}")

            birth_year = extract_birth_year(raw.info.get('subject_info', {}), birth_suffix)
            age = calculate_age(birth_year, recording_date)

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
            # if args.data_name == "CMH":
            #     # sex_info = ("male" if sex == 2 else "female") if sex is not None else "n/a"
            #     sex_info = "n/a"
            # elif args.data_name == "CMH_A7":
            #     sex_info = ("male" if sex == 2 else "female") if sex is not None else "n/a"
            # elif args.data_name == "CMH_C2B":
            #     # sex_info = ("male" if sex == 2 else "female") if sex is not None else "n/a"
            #     sex_info = "n/a"
            # elif args.data_name == "108_Only":
            #     # sex_info = ("male" if sex == 2 else "female") if sex is not None else "n/a"
            #     sex_info = "n/a"
            # else:
            #     sex_info = "n/a"

            # Add to anonymous data for participants.tsv
            anonymous_data.append({
                "participant_id": f"sub-{sub_id}",
                "age": str(age) if age is not None else "n/a",
                "sex": ("male" if sex == 2 else "female") if sex is not None else "n/a",
                "group": "n/a"  # Adjust if group info available
            })

        # Create scans_file storing metadata of the recording session
        scans_file = os.path.join(sub_dir, f"sub-{sub_id}_scans.tsv")
        if recording_date and isinstance(recording_date, (datetime , )):
            acq_time = recording_date.strftime("%Y-%m-%dT%H:%M:%S")
        else:
            acq_time = "n/a"
        
        # Tạo dataframe với thông tin file ghi
        scans_data = pd.DataFrame([{
            "filename": os.path.relpath(bids_edf, start=sub_dir),  # relative path theo BIDS
            "acq_time": acq_time
        }])

        # Nếu file scans.tsv đã tồn tại thì append thêm, tránh ghi đè
        if os.path.exists(scans_file):
            existing_scans = pd.read_csv(scans_file, sep='\t')
            scans_data = pd.concat([existing_scans, scans_data], ignore_index=True)

        # Ghi ra file
        scans_data.to_csv(scans_file, sep='\t', index=False)

    # Sau vòng lặp: lưu danh sách file lỗi
    if failed_files:
        pd.DataFrame({"failed_file": failed_files}).to_csv(os.path.join(bids_dir, "failed_files.tsv"), sep='\t', index=False)
        print(f"⚠️ {len(failed_files)} EDF files failed to parse. See failed_files.tsv")
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
    # Đường dẫn tới file participants.tsv và participants.json
    participants_file = os.path.join(bids_dir, "participants.tsv")
    participants_json_file = os.path.join(bids_dir, "participants.json")

    # Tạo DataFrame từ dữ liệu mới
    new_data_df = pd.DataFrame(anonymous_data)

    # Nếu participants.tsv đã tồn tại, đọc và nối dữ liệu mới
    if os.path.exists(participants_file):
        existing_df = pd.read_csv(participants_file, sep='\t')
        # Kiểm tra để tránh trùng lặp participant_id
        if 'participant_id' in new_data_df.columns and 'participant_id' in existing_df.columns:
            new_data_df = new_data_df[~new_data_df['participant_id'].isin(existing_df['participant_id'])]
        participants_df = pd.concat([existing_df, new_data_df], ignore_index=True)
    else:
        participants_df = new_data_df

    # Ghi lại participants.tsv (bao gồm cả dữ liệu cũ và mới)
    participants_df.to_csv(participants_file, sep='\t', index=False, na_rep='n/a')

    # participants.json chỉ tạo nếu chưa có
    if not os.path.exists(participants_json_file):
        participants_json = {
            "participant_id": {"Description": "Unique identifier for each participant"},
            "age": {"Description": "Age of the participant in years", "Units": "years"},
            "sex": {"Description": "Sex of the participant (male, female, or n/a)"},
            "group": {"Description": "Clinical group"}
        }
        with open(participants_json_file, 'w') as f:
            json.dump(participants_json, f, indent=4)

    print(f"BIDS dataset updated in: {bids_dir}")

if __name__ == "__main__":
    args = get_args()
    build_database(args)
    print("DONE!!")
    