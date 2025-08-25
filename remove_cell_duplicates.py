"""
Excel Cell Duplicate Remover

This script reads an Excel file and removes duplicates from cells that contain
comma-separated data. It processes all sheets in the workbook and creates a
new cleaned version of the file.

Author: GitHub Copilot
Date: August 5, 2025
"""

import pandas as pd
import os
import sys
from typing import Any, Optional
import argparse
from pathlib import Path


def remove_duplicates_from_cell(cell_value: Any, separator: str = ',') -> tuple[str, int]:
    """
    Remove duplicates from a comma-separated string while preserving order.
    
    Args:
        cell_value: The cell value to process
        separator: The separator used in the data (default: comma)
    
    Returns:
        Tuple of (clean string with duplicates removed, count of duplicates removed)
    """
    if pd.isna(cell_value) or cell_value == '':
        return cell_value, 0
    
    # Convert to string if not already
    if not isinstance(cell_value, str):
        cell_value = str(cell_value)
    
    # Split by separator, strip whitespace, and remove duplicates while preserving order
    items = [item for item in cell_value.split(separator)]
    original_count = len([item for item in items if item])  # Count non-empty items
    
    # Remove duplicates while preserving order
    seen = set()
    unique_items = []
    for item in items:
        if item and item not in seen:  # Skip empty strings
            seen.add(item)
            unique_items.append(item)
    
    unique_count = len(unique_items)
    duplicates_removed = original_count - unique_count
    
    # Join back with separator and space for readability
    cleaned_value = f'{separator}'.join(unique_items)
    return cleaned_value, duplicates_removed


def process_excel_file(input_file: str, output_file: Optional[str] = None, 
                      separator: str = ',', columns: Optional[list] = None) -> None:
    """
    Process an Excel file to remove duplicates from comma-separated cell data.
    
    Args:
        input_file: Path to the input Excel file
        output_file: Path to the output Excel file (optional)
        separator: The separator used in the data (default: comma)
        columns: List of specific columns to process (optional, processes all if None)
    """
    try:
        print(f"Reading Excel file: {input_file}")
        
        # Read all sheets from the Excel file
        excel_data = pd.read_excel(input_file, sheet_name=None, dtype=str)
        
        if not excel_data:
            print("No sheets found in the Excel file.")
            return
        
        # Process each sheet
        processed_sheets = {}
        total_duplicates_removed = 0
        
        for sheet_name, df in excel_data.items():
            print(f"Processing sheet: {sheet_name}")
            sheet_duplicates_removed = 0
            
            # Create a copy of the dataframe
            df_cleaned = df.copy()
            
            # Determine which columns to process
            cols_to_process = columns if columns else df.columns
            
            # Process each specified column
            for col in cols_to_process:
                if col in df.columns:
                    print(f"  Processing column: {col}")
                    column_duplicates_removed = 0
                    
                    # Apply the function and collect duplicate counts
                    results = df_cleaned[col].apply(
                        lambda x: remove_duplicates_from_cell(x, separator)
                    )
                    
                    # Separate cleaned values and duplicate counts
                    cleaned_values = []
                    for result in results:
                        if isinstance(result, tuple):
                            cleaned_value, dup_count = result
                            cleaned_values.append(cleaned_value)
                            column_duplicates_removed += dup_count
                        else:
                            # Handle cases where no duplicates were found
                            cleaned_values.append(result)
                    
                    df_cleaned[col] = cleaned_values
                    
                    if column_duplicates_removed > 0:
                        print(f"    → Removed {column_duplicates_removed} duplicates from column '{col}'")
                    else:
                        print(f"    → No duplicates found in column '{col}'")
                    
                    sheet_duplicates_removed += column_duplicates_removed
                else:
                    print(f"  Warning: Column '{col}' not found in sheet '{sheet_name}'")
            
            print(f"  Sheet '{sheet_name}' total: {sheet_duplicates_removed} duplicates removed")
            total_duplicates_removed += sheet_duplicates_removed
            processed_sheets[sheet_name] = df_cleaned
        
        # Generate output filename if not provided
        if output_file is None:
            input_path = Path(input_file)
            output_file = input_path.parent / f"{input_path.stem}_cleaned{input_path.suffix}"
        
        print(f"Writing cleaned data to: {output_file}")
        
        # Write all sheets to the output file
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            for sheet_name, df in processed_sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print("Processing completed successfully!")
        print(f"Original file: {input_file}")
        print(f"Cleaned file: {output_file}")
        print(f"Total duplicates removed: {total_duplicates_removed}")
        
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        sys.exit(1)


def main():
    """Main function to handle command line arguments and execute the script."""
    parser = argparse.ArgumentParser(
        description="Remove duplicates from comma-separated data in Excel cells",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python remove_cell_duplicates.py input.xlsx
  python remove_cell_duplicates.py input.xlsx -o output.xlsx
  python remove_cell_duplicates.py input.xlsx -s ";" -c "Column1,Column2"
  python remove_cell_duplicates.py input.xlsx --separator "|" --columns "Names,Items"
        """
    )
    
    parser.add_argument('input_file', 
                       help='Path to the input Excel file')
    
    parser.add_argument('-o', '--output', 
                       help='Path to the output Excel file (default: input_file_cleaned.xlsx)')
    
    parser.add_argument('-s', '--separator', 
                       default=',',
                       help='Separator used in the data (default: comma)')
    
    parser.add_argument('-c', '--columns',
                       help='Comma-separated list of column names to process (default: all columns)')
    
    parser.add_argument('--preview', 
                       action='store_true',
                       help='Preview the first few rows without making changes')
    
    args = parser.parse_args()
    
    # Validate input file
    if not os.path.exists(args.input_file):
        print(f"Error: Input file '{args.input_file}' does not exist.")
        sys.exit(1)
    
    # Parse columns if provided
    columns = None
    if args.columns:
        columns = [col for col in args.columns.split(',')]
        print(f"Will process columns: {columns}")
    
    # Preview mode
    if args.preview:
        print("Preview mode - showing first 5 rows of each sheet:")
        try:
            excel_data = pd.read_excel(args.input_file, sheet_name=None, dtype=str)
            for sheet_name, df in excel_data.items():
                print(f"\nSheet: {sheet_name}")
                print(df.head())
        except Exception as e:
            print(f"Error reading file: {str(e)}")
        return
    
    # Process the file
    process_excel_file(
        input_file=args.input_file,
        output_file=args.output,
        separator=args.separator,
        columns=columns
    )


if __name__ == "__main__":
    main()
