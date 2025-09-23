import pandas as pd

# đọc file
clinical_sheet = pd.read_excel(
    "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/kqcls/matched_patients_translated.xlsx"
)

# strip khoảng trắng trong tên cột
clinical_sheet.columns = clinical_sheet.columns.str.upper().str.strip()

# in ra để kiểm tra
print(clinical_sheet.columns.tolist())

# lưu lại (ghi đè hoặc tạo file mới)
out_path = "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/kqcls/matched_patients_translated_clean.xlsx"
clinical_sheet.to_excel(out_path, index=False)
print(f"✅ Saved cleaned file to {out_path}")