import pandas as pd

# Paths for input and output Excel files
input_xlsx = "/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/kqcls/test_result_classification.xlsx"  # Replace with your actual file path
output_xlsx = "./clinical_sheet_updated.xlsx"  # Output file with updated Result_Type

# Read the Excel file
try:
    df = pd.read_excel(input_xlsx)
except Exception as e:
    print(f"Error reading Excel file {input_xlsx}: {e}")
    exit()

# Strip leading/trailing spaces from column names
df.columns = df.columns.str.strip()

# Print column names for debugging
print("Columns in clinical sheet:", df.columns.tolist())

# Check for required columns
required_columns = ['HFL_NAME', 'Result_Type', 'Count']
missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    print(f"Error: Missing required columns: {missing_columns}")
    exit()

# Function to reclassify Result_Type
def reclassify_Result_Type(value):
    if pd.isna(value):
        return 'n/a'  # Handle NaN values
    # Convert to string and lowercase for case-insensitive comparison
    value_str = str(value).lower()
    # Check for text-like values (text, negative, positive, normal, or combinations)
    text_keywords = ['text', 'negative', 'positive', 'normal']
    if any(keyword in value_str for keyword in text_keywords):
        return 'number'
    # Check for number/unknown combinations
    if 'number' in value_str and 'unknown' in value_str:
        return 'number'
    # Keep original value if no text or number/unknown
    return value

# Apply reclassification to Result_Type column
df['Result_Type'] = df['Result_Type'].apply(reclassify_Result_Type)

# Print summary of Result_Type values after reclassification
print("Summary of Result_Type values after reclassification:")
print(df['Result_Type'].value_counts(dropna=False))

# Save the updated DataFrame to Excel
try:
    df.to_excel(output_xlsx, index=False)
    print(f"Excel file with updated Result_Type saved to: {output_xlsx}")
except Exception as e:
    print(f"Error saving Excel file {output_xlsx}: {e}")
    exit()