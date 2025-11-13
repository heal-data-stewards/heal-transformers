#!/usr/bin/env python

import pandas as pd
import argparse
import os

def clean_floats_to_ints(df):
    for col in df.select_dtypes(include=["float"]):
        # Only convert if all values that aren't NaN are whole numbers
        if (df[col].dropna() % 1 == 0).all():
            df[col] = df[col].astype("Int64")  # Use pandas nullable integer type
    return df

def excel_to_csv(input_file, output_file, sheet_name=0):
    df = pd.read_excel(input_file, sheet_name=sheet_name)
    df = clean_floats_to_ints(df)
    df.to_csv(output_file, index=False)

def main():
    parser = argparse.ArgumentParser(description='Convert Excel file to CSV.')
    parser.add_argument('input_file', help='Path to the input Excel file')
    parser.add_argument('--filename', help='Optional output CSV filename')
    parser.add_argument('--sheet', default=0, help='Sheet name or index to read from (default is first sheet)')

    args = parser.parse_args()

    input_file = args.input_file
    output_file = args.filename or os.path.splitext(input_file)[0] + '.csv'
    sheet = args.sheet
    # Try to parse sheet as int if possible
    try:
        sheet = int(sheet)
    except ValueError:
        pass

    excel_to_csv(input_file, output_file, sheet)

if __name__ == '__main__':
    main()
