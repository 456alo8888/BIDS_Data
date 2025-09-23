import pandas as pd
import re

# Load the XLSX file
df = pd.read_excel('/mnt/disk1/aiotlab/hieupc/New_CBraMod/BIDS/clinical_sheet_cleaned.xlsx')
print(df.columns)

# Function to classify result type
def classify_result(result):
    result = str(result).strip()
    
    # Numeric: only numbers (integers or decimals)
    if re.match(r'^-?\d*\.?\d+$', result):
        return 'Numeric'
    
    # Textual: contains letters, spaces, or specific symbols, no standalone numbers
    elif re.match(r'^[A-Za-z\s\.\,\-\(\)\:]+$', result) or any(keyword in result.lower() for keyword in ['positive', 'negative', 'normal', 'abnormal', 'tirads']):
        return 'Textual'
    
    # Mixed: contains numbers and letters (e.g., "4.82 mmol/L")
    elif re.match(r'.*\d.*[A-Za-z].*', result):
        return 'Mixed'
    
    # Handle other cases
    else:
        return 'Unknown'

# Apply classification
df['Result_Type'] = df['PARA_RESULT'].apply(classify_result)

# Group by HFL_NAME and Result_Type
result_summary = df.groupby(['HFL_NAME', 'Result_Type']).size().reset_index(name='Count')

# Display the summary
print(result_summary)

# Save to a new Excel file
result_summary.to_excel('test_result_classification.xlsx', index=False)
