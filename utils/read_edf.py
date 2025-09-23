import mne
import re

def extract_edf_metadata(edf_file):
    """
    Đọc file EDF và extract thông tin:
    - sex (0=nữ, 1=nam)
    - last_name -> tách thành tên đầy đủ và số cuối
    """
    try:
        raw = mne.io.read_raw_edf(edf_file, preload=False, verbose=False)
    except Exception as e:
        print(f"Failed to read EDF: {e}")
        return

    subject_info = raw.info.get('subject_info', {})
    sex = subject_info.get('sex')
    last_name_raw = subject_info.get('last_name')
    print(f"Loaded EDF {edf_file}: subject_info = {subject_info}")

    # convert sex code
    sex_str = 0 if sex == 0 else 1 if sex == 1 else "Unknown"

    # tách tên và số cuối
    name_only = None
    number = None
    if last_name_raw:
        match = re.search(r'^(.*?)[_]?(\d+)?$', last_name_raw)
        if match:
            name_part = match.group(1)
            number_part = match.group(2)
            # bỏ dấu _, thay _ bằng space
            name_only = name_part.replace("_", " ").strip()
            number = int(number_part) if number_part else None

    # in ra
    print(f"File: {edf_file}")
    print(f"Sex: {sex_str}")
    print(f"Full Name: {name_only}")
    print(f"Number / Birth suffix: {number}")

# === Sử dụng ===
edf_file = "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/test/FA5550A0_1-1+.edf"
extract_edf_metadata(edf_file)
