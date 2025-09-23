import os
import glob
import re

folder = "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/EEG2100"
# log_files = glob.glob(os.path.join(folder, "*.log"))
# cmt_files = glob.glob(os.path.join(folder, "*.cmt"))

log_files = ["/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/EEG2100/FA5550A0.log"]
cmt_files = ["/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/EEG2100/FA5550A0.CMT"]

def extract_metadata_from_text(file_path):
    meta = {}
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            # DOC_NO
            m = re.search(r'DOC_NO[:=]\s*(\d+)', line)
            if m:
                meta['DOC_NO'] = m.group(1)
            # Name
            m = re.search(r'Patient[:=]\s*(.+)', line)
            if m:
                meta['PATIENT_NAME'] = m.group(1)
            # Birth date
            m = re.search(r'BIRTH_DATE[:=]\s*(\d{2}/\d{2}/\d{4})', line)
            if m:
                meta['BIRTH_DATE'] = m.group(1)
            # Sex
            m = re.search(r'Sex[:=]\s*(Male|Female|Nam|Nữ)', line, re.IGNORECASE)
            if m:
                meta['GENDER'] = m.group(1)
    return meta


# 2️⃣ In nội dung log / cmt
# text_files = glob.glob(os.path.join(folder, "*.log")) + glob.glob(os.path.join(folder, "*.cmt"))
text_files = glob.glob(os.path.join(folder, "*.CMT"))

print("\n=== LOG / CMT FILES ===")
for f in text_files:
    print(f"\n--- {os.path.basename(f)} ---")
    try:
        with open(f, "r", encoding="utf-8", errors="ignore") as file:
            lines = file.readlines()
        # in 20 dòng đầu để quan sát
        for line in lines[:20]:
            print(line.strip())
    except Exception as e:
        print(f"Failed to read {f}: {e}")