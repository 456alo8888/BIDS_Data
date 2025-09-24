import os
import shutil
import csv
import numpy as np
from pyedflib import EdfReader, EdfWriter, FILETYPE_EDFPLUS
import re 
import pyedflib
import mne
import logging

# bids_root = "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/bids_testing"
# mapping_csv = os.path.join("mapping_original_to_sub_1.csv")
# overwrite = False  # True = ghi đè EDF gốc (có backup), False = tạo file *_anon.edf
# Configure logging
logging.basicConfig(filename="anonymize_log.txt", level=logging.INFO, format="%(asctime)s - %(message)s")

bids_root = "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/bids_testing"
mapping_csv = os.path.join(bids_root, "mapping_original_to_sub_1.csv")
skipped_csv = os.path.join(bids_root, "skipped_files.csv")
overwrite = True  # False = create *_anon.edf, True = overwrite original (with backup)
size_threshold = 500  # 1 GB in bytes; adjust as needed

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
    Anonymize an EDF file while preserving EEG signal data.
    Compatible with older pyedflib and MNE versions.
    """
    try:
        # Read input file
        with pyedflib.EdfReader(edf_path) as r:
            n_channels = r.signals_in_file
            sig_headers = r.getSignalHeaders()
            header = r.getHeader()
            sample_rate = r.getSampleFrequency(0)  # Assume same sample rate for all channels
            
            # Get number of samples (handle older pyedflib versions)
            try:
                n_samples = r.getNSamples(0)  # Newer versions
            except TypeError:
                n_samples = r.samples_in_file(0)  # Older versions fallback
            signals = [r.readSignal(i) for i in range(n_channels)]
            
            # Verify signal lengths
            if any(len(sig) != n_samples for sig in signals):
                raise ValueError(f"Signal length mismatch: expected {n_samples}, got {[len(sig) for sig in signals]}")
            print(f"Read {n_channels} channels, {n_samples} samples each at {sample_rate} Hz")

        # Modify header to anonymize
        header['patientname'] = new_patient_name
        header['patientcode'] = new_patient_name
        header['birthdate'] = ''
        header['gender'] = ''
        header['admincode'] = ''
        header['technician'] = ''
        header['equipment'] = ''
        if 'subject_info' in header:
            header['subject_info'] = {}

        # Write to temporary file
        tmp_out = out_path + ".tmp"
        writer = pyedflib.EdfWriter(tmp_out, n_channels=n_channels, file_type=pyedflib.FILETYPE_EDFPLUS)
        writer.setHeader(header)
        writer.setSignalHeaders(sig_headers)
        
        # Write signals in chunks to avoid truncation
        chunk_size = int(sample_rate)  # Write 1 second at a time (e.g., 500 samples at 500 Hz)
        for i in range(0, n_samples, chunk_size):
            for ch in range(n_channels):
                chunk = signals[ch][i:i + chunk_size]
                if len(chunk) > 0:
                    writer.writePhysicalSamples(np.array(chunk, dtype=np.float64))
            print(f"Wrote samples {i} to {i + chunk_size - 1}")
        
        writer.close()

        # Verify output file
        with pyedflib.EdfReader(tmp_out) as r:
            try:
                out_n_samples = r.getNSamples(0)
            except TypeError:
                out_n_samples = r.samples_in_file(0)
            if out_n_samples != n_samples:
                raise ValueError(f"Output truncated: expected {n_samples} samples, got {out_n_samples}")
        
        # Move temporary file to final output
        shutil.move(tmp_out, out_path)
        print(f"Successfully anonymized {edf_path} -> {out_path}")
        return True

    except Exception as e1:
        print(f"[WARN] PyEDFlib failed for {edf_path}: {e1}. Trying MNE...")
        try:
            raw = mne.io.read_raw_edf(edf_path, preload=True, verbose=False)
            # Handle older MNE versions (anonymize without subject parameter)
            try:
                raw.anonymize(subject=new_patient_name)
            except TypeError:
                raw.anonymize()  # Older versions don't support subject
                raw.info['subject_info'] = {'id': new_patient_name}  # Manually set subject ID
            raw.export(out_path, fmt="edf", physical_range="auto", overwrite=True)
            # Verify MNE output
            new_raw = mne.io.read_raw_edf(out_path, preload=True)
            if raw.get_data().shape != new_raw.get_data().shape:
                raise ValueError(f"MNE output shape mismatch: expected {raw.get_data().shape}, got {new_raw.get_data().shape}")
            print(f"Successfully anonymized with MNE: {edf_path} -> {out_path}")
            return True
        except Exception as e2:

            #Clear .tmp file (temporary store of signal data)
            # if os.path.exists(tmp_out):
            #     try:
            #         os.remove(tmp_out)
            #         logging.info(f"Cleaned up temporary file {tmp_out}")
            #     except Exception as e:
            #         logging.error(f"Failed to clean up {tmp_out}: {e}")

            print(f"[WARN] MNE failed: {e2}. Copying original...")
            shutil.copy2(edf_path, out_path)
            print(f"[FALLBACK] Copied original: {edf_path} -> {out_path}")
            return False 

def extract_sub_num(sub_name: str) -> int:
    """
    Lấy số ID từ tên thư mục sub-XXX.
    Ví dụ: sub-001 -> 1, sub-11 -> 11, sub-110 -> 110.
    Nếu không match thì trả về giá trị lớn để đẩy ra cuối.
    """
    m = re.search(r"sub-(\d+)", sub_name)
    return int(m.group(1)) if m else 999999

def process_bids(bids_root, overwrite=False, size_threshold=500):
    """
    Process BIDS dataset, anonymize EDF files, skip large/unreadable files, and log skipped files.
    """
    rows = []
    skipped_rows = []
    subs = [d for d in os.listdir(bids_root) if d.startswith("sub-")]
    subs_sorted = sorted(subs, key=extract_sub_num)
    # backup_dir = os.path.join(bids_root, "backups")
    # os.makedirs(backup_dir, exist_ok=True)

    for sub in subs_sorted:
        subdir = os.path.join(bids_root, sub, "eeg")
        if not os.path.isdir(subdir):
            continue
        for fname in sorted(os.listdir(subdir)):
            if not fname.lower().endswith(".edf"):
                continue
            edf_path = os.path.join(subdir, fname)
            
            # Check file size
            try:
                file_size = os.path.getsize(edf_path)
                if file_size > size_threshold*1024*1024:
                    skipped_rows.append({
                        "file": edf_path,
                        "size_mb": file_size / (1024 * 1024),
                        "reason": f"File size ({file_size / (1024 * 1024):.2f} MB) exceeds threshold ({size_threshold} MB)"
                    })
                    logging.warning(f"Skipped {edf_path}: File size ({file_size / (1024 * 1024):.2f} MB) exceeds threshold")
                    continue
            except Exception as e:
                skipped_rows.append({
                    "file": edf_path,
                    "size_mb": "Unknown",
                    "reason": f"Cannot access file: {e}"
                })
                logging.warning(f"Skipped {edf_path}: Cannot access file: {e}")
                continue

            # Get patient name
            original_name = get_patient_name(edf_path)
            anon_name = sub

            # Create backup
            # backup = os.path.join(backup_dir, f"{sub}_{fname}.bak")
            # shutil.copy2(edf_path, backup)
            # logging.info(f"Backed up {edf_path} to {backup}")

            # Anonymize file
            if overwrite:
                tmp_out = edf_path + ".tmp"
                success = anonymize_edf(edf_path, tmp_out, anon_name)
                if success:
                    os.replace(tmp_out, edf_path)
                    out_path = edf_path
                    logging.info(f"Overwrote {edf_path}")
                else:
                    out_path = edf_path
                    skipped_rows.append({
                        "file": edf_path,
                        "size_mb": file_size / (1024 * 1024),
                        "reason": "Anonymization failed (fallback to copy)"
                    })
                    logging.warning(f"Skipped anonymization for {edf_path}: Failed and copied original")
            else:
                out_fname = fname.replace(".edf", "_anon.edf")
                out_path = os.path.join(subdir, out_fname)
                success = anonymize_edf(edf_path, out_path, anon_name)
                if not success:
                    skipped_rows.append({
                        "file": edf_path,
                        "size_mb": file_size / (1024 * 1024),
                        "reason": "Anonymization failed (fallback to copy)"
                    })
                    logging.warning(f"Skipped anonymization for {edf_path}: Failed and copied original")
                else:
                    logging.info(f"Wrote anonymized file: {out_path}")

            rows.append({
                "sub": sub,
                "orig_edf": edf_path,
                "anon_edf": out_path,
                "original_patient_name": original_name,
                "anon_patient_name": anon_name
            })

    # Write mapping CSV
    with open(mapping_csv, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["sub", "orig_edf", "anon_edf",
                                              "original_patient_name", "anon_patient_name"])
        writer.writeheader()
        writer.writerows(rows)

    # Write skipped files CSV
    with open(skipped_csv, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["file", "size_mb", "reason"])
        writer.writeheader()
        writer.writerows(skipped_rows)

    print(f"Completed. Mapping saved to: {mapping_csv}")
    print(f"Skipped files logged to: {skipped_csv}")
    logging.info(f"Completed. Mapping saved to: {mapping_csv}, Skipped files: {skipped_csv}")

if __name__ == "__main__":
    process_bids(bids_root, overwrite=overwrite)