import os
import shutil
import csv
import numpy as np
from pyedflib import EdfReader, EdfWriter, FILETYPE_EDFPLUS

bids_root = "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/database_bids"
mapping_csv = os.path.join("mapping_original_to_sub.csv")
overwrite = False  # True = ghi đè EDF gốc (có backup), False = tạo file *_anon.edf

def get_patient_name(edf_path):
    """Đọc tên bệnh nhân từ header EDF (cố gắng lấy từ nhiều trường)."""
    name = None
    try:
        with EdfReader(edf_path) as r:
            hdr = r.getHeader()
            for k in ('patientname', 'patient', 'patientcode'):
                if k in hdr and hdr[k]:
                    name = hdr[k]
                    break
            if not name:
                try:
                    name = r.getPatient()
                except Exception:
                    pass
            if not name:
                try:
                    name = r.getPatientCode()
                except Exception:
                    pass
    except Exception as e:
        print(f"Không đọc header được {edf_path}: {e}")
    return str(name) if name else os.path.basename(edf_path)

def anonymize_edf(edf_path, out_path, new_patient_name):
    """Sửa header EDF, thay patientname bằng ID ẩn danh, rồi ghi file mới."""
    with EdfReader(edf_path) as r:
        n_channels = r.signals_in_file
        sig_headers = r.getSignalHeaders()
        header = r.getHeader()
        signals = [r.readSignal(i) for i in range(n_channels)]

    # chỉnh header
    header['patientname'] = new_patient_name
    header['patientcode'] = new_patient_name
    header['birthdate'] = ''
    header['gender'] = ''
    header['admincode'] = ''
    header['technician'] = ''
    header['equipment'] = ''

    writer = EdfWriter(out_path, n_channels=n_channels, file_type=FILETYPE_EDFPLUS)
    writer.setHeader(header)
    writer.setSignalHeaders(sig_headers)
    for sig in signals:
        writer.writePhysicalSamples(np.array(sig))
    writer.close()

def process_bids(bids_root, overwrite=False):
    rows = []
    for sub in sorted(os.listdir(bids_root)):
        subdir = os.path.join(bids_root, sub, "eeg")
        if not os.path.isdir(subdir):
            continue
        for fname in sorted(os.listdir(subdir)):
            if not fname.lower().endswith(".edf"):
                continue
            edf_path = os.path.join(subdir, fname)
            original_name = get_patient_name(edf_path)
            anon_name = sub  # dùng luôn sub-XX làm ID

            if overwrite:
                backup = edf_path + ".bak"
                shutil.copy2(edf_path, backup)
                tmp_out = edf_path + ".tmp"
                anonymize_edf(edf_path, tmp_out, anon_name)
                os.replace(tmp_out, edf_path)
                out_path = edf_path
                print(f"Overwrote {edf_path} (backup ở {backup})")
            else:
                out_fname = fname.replace(".edf", "_anon.edf")
                out_path = os.path.join(subdir, out_fname)
                anonymize_edf(edf_path, out_path, anon_name)
                print(f"Wrote anonymized file: {out_path}")

            rows.append({
                "sub": sub,
                "orig_edf": edf_path,
                "anon_edf": out_path,
                "original_patient_name": original_name,
                "anon_patient_name": anon_name
            })

    with open(mapping_csv, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["sub", "orig_edf", "anon_edf",
                                               "original_patient_name", "anon_patient_name"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Hoàn tất. Mapping lưu ở: {mapping_csv}")

if __name__ == "__main__":
    process_bids(bids_root, overwrite=overwrite)
