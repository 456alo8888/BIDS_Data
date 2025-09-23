# import pandas as pd
# from deep_translator import GoogleTranslator

# # Đọc file TSV
# df = pd.read_csv("/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/bids_dataset/participants.tsv", sep="\t")

# # Hàm dịch từng ô
# def translate_text(text):
#     if pd.isna(text):  # Nếu ô rỗng thì bỏ qua
#         return text
#     try:
#         return GoogleTranslator(source="vi", target="en").translate(str(text))
#     except:
#         return text

# # Dịch toàn bộ DataFrame
# df_translated = df.applymap(translate_text)

# # Ghi ra file TSV mới
# df_translated.to_csv("output.tsv", sep="\t", index=False)

import pandas as pd
from deep_translator import GoogleTranslator

# Đọc file Excel
df = pd.read_excel("/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/kqcls.xlsx")

# Hàm dịch từng ô
def translate_text(text):
    if pd.isna(text):  # Bỏ qua ô rỗng
        return text
    try:
        return GoogleTranslator(source="vi", target="en").translate(str(text))
    except:
        return text

# Dịch toàn bộ DataFrame
df_translated = df.applymap(translate_text)

# Ghi ra file Excel mới
df_translated.to_excel("translated_kqcls.xlsx", index=False)