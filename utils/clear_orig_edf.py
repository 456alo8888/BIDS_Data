import os
import glob

root = "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/Test_Small_BIDS"

def delete_original_edf(root):
    # Tìm tất cả file .edf đúng định dạng
    pattern = os.path.join(root, "sub-*", "eeg", "sub-*_task-rest*_eeg.edf.tmp")
    edf_files = glob.glob(pattern)

    print(f"Tìm thấy {len(edf_files)} file EDF cần xoá.")
    for f in edf_files:
        try:
            os.remove(f)
            print(f"Đã xoá: {f}")
        except Exception as e:
            print(f"Lỗi khi xoá {f}: {e}")

delete_original_edf(root)
