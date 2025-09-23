import pandas as pd

def fix_participants_tsv(input_tsv, output_tsv):
    # Đọc file TSV
    df = pd.read_csv(input_tsv, sep="\t")

    # Tạo lại participant_id theo số dòng
    df["participant_id"] = [f"sub-{i:02d}" for i in range(1, len(df) + 1)]

    # Ghi ra file mới
    df.to_csv(output_tsv, sep="\t", index=False)

    print(f"Đã chuẩn hoá participants.tsv -> {output_tsv}")


# Ví dụ chạy
fix_participants_tsv(
    "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/database_bids/participants.tsv",
    "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/database_bids/participants_fixed.tsv"
)
