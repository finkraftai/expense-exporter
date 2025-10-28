"""
Main Entry Point for Expense Exporter

Processes CSV/Excel files containing HOTEL_INVOICE_PATH, downloads PDFs,
uploads to cloud storage (S3/Azure), generates links, and updates the file.
"""

import sys
import argparse
from utils.logger import logger

def run_csv_excel_processing():
    """Run CSV/Excel file processing"""
    from utils.file_process import FileProcessor
    from utils.config import INPUT_FILE_PATH, OUTPUT_FILE_PATH

    processor = FileProcessor()
    success = processor.process_file(INPUT_FILE_PATH, OUTPUT_FILE_PATH)

    if success:
        logger.info("CSV/Excel processing completed successfully")
    else:
        logger.error("CSV/Excel processing failed")
        sys.exit(1)

def main():
    """
    Main entry point with command-line argument parsing.
    """
    parser = argparse.ArgumentParser(description="Expense Exporter")
    parser.add_argument(
        "--mode",
        choices=["csv"],
        default="csv",
        help="Processing mode: 'csv' for CSV/Excel processing"
    )

    args = parser.parse_args()

    try:
        if args.mode == "csv":
            run_csv_excel_processing()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
