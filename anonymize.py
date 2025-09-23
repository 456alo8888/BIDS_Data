import os
import shutil
import csv
import numpy as np
from pyedflib import EdfReader, EdfWriter, FILETYPE_EDFPLUS
import re 


bids_root = "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/bids_testing"
mapping_csv = os.path.join("mapping_original_to_sub_1.csv")
overwrite = True  # True = ghi đè EDF gốc (có backup), False = tạo file *_anon.edf

def get_patient_name(edf_path):
    """
    Đọc tên bệnh nhân từ header EDF, cố gắng gom thông tin từ nhiều trường khác nhau.
    Trả về chuỗi nguyên bản (có thể lộn xộn, nhưng để mapping).
    """
    name = None
    try:
        with EdfReader(edf_path) as r:
            hdr = r.getHeader()

            # Ưu tiên các trường phổ biến
            for k in ('patientname', 'patient', 'patientcode'):
                if k in hdr and hdr[k]:
                    name = hdr[k]
                    break

            # Nếu chưa có, thử lấy bằng API
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

            # Nếu vẫn chưa có, gom tất cả subject_info thành string
            if not name and "subject_info" in hdr:
                subj = hdr["subject_info"]
                if isinstance(subj, dict):
                    parts = []
                    for k, v in subj.items():
                        if v:
                            parts.append(f"{k}:{v}")
                    name = "_".join(parts)

    except Exception as e:
        print(f"Không đọc header được {edf_path}: {e}")

    # Nếu cuối cùng vẫn None thì fallback sang tên file
    return str(name) if name else os.path.basename(edf_path)

def anonymize_edf(edf_path, out_path, new_patient_name):
    """
    Tạo file EDF ẩn danh: replace toàn bộ thông tin định danh bằng sub-ID.
    Ưu tiên dùng pyedflib -> fallback sang mne -> cuối cùng copy gốc.
    """
    try:
        # --- Cách 1: PyEDFlib (chuẩn EDF+)
        with EdfReader(edf_path) as r:
            n_channels = r.signals_in_file
            sig_headers = r.getSignalHeaders()
            header = r.getHeader()
            signals = [r.readSignal(i) for i in range(n_channels)]

        # Chỉnh header: xóa mọi thông tin cá nhân
        header['patientname'] = new_patient_name
        header['patientcode'] = new_patient_name
        header['birthdate'] = ''
        header['gender'] = ''
        header['admincode'] = ''
        header['technician'] = ''
        header['equipment'] = ''
        if 'subject_info' in header:
            header['subject_info'] = {}

        writer = EdfWriter(out_path, n_channels=n_channels, file_type=FILETYPE_EDFPLUS)
        writer.setHeader(header)
        writer.setSignalHeaders(sig_headers)
        for sig in signals:
            writer.writePhysicalSamples(np.array(sig))
        writer.close()
        return

    except Exception as e1:
        print(f"[WARN] PyEDFlib anonymization failed for {edf_path}: {e1}. Trying with MNE...")

    # --- Cách 2: MNE
    try:
        import mne
        raw = mne.io.read_raw_edf(edf_path, preload=True, verbose=False)
        raw.anonymize(subject=new_patient_name)
        raw.export(out_path, fmt="edf", physical_range=(-32768, 32767), overwrite=True)
        return
    except Exception as e2:
        print(f"[WARN] MNE anonymization failed for {edf_path}: {e2}. Copying original file...")

    # --- Cách 3: Copy nguyên bản (không ẩn danh)
    import shutil
    shutil.copy2(edf_path, out_path)
    print(f"[FALLBACK] Copied original (not anonymized): {edf_path} -> {out_path}")

def extract_sub_num(sub_name: str) -> int:
    """
    Lấy số ID từ tên thư mục sub-XXX.
    Ví dụ: sub-001 -> 1, sub-11 -> 11, sub-110 -> 110.
    Nếu không match thì trả về giá trị lớn để đẩy ra cuối.
    """
    m = re.search(r"sub-(\d+)", sub_name)
    return int(m.group(1)) if m else 999999

def process_bids(bids_root, overwrite=False):
    rows = []
    subs = [d for d in os.listdir(bids_root) if d.startswith("sub-")]
    subs_sorted = sorted(subs, key=extract_sub_num)

    for sub in subs_sorted:
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
                # backup = edf_path + ".bak"
                # shutil.copy2(edf_path, backup)
                tmp_out = edf_path + ".tmp"
                anonymize_edf(edf_path, tmp_out, anon_name)
                os.replace(tmp_out, edf_path)
                out_path = edf_path
                print(f"Overwrote {edf_path}")
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
