"""
Main Entry Point for Expense Exporter
Processes CSV/Excel files: - Downloads PDFs from URLs - Uploads them to cloud storage (S3/Azure) - Updates the file with generated links and statuses
"""

import sys
import os
import argparse
from utils.logger import logger

# Ensure absolute imports work when running from project root
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

def run_csv_excel_processing(input_path, output_path):
    """Run CSV/Excel file processing"""
    from utils.file_process import FileProcessor

    # Validate input file path
    if not input_path or not os.path.exists(input_path):
        logger.error(f"Input file not found: {input_path}")
        logger.info("Please provide a valid input file path.")
        logger.info("Usage: python main.py <input_file_path> [output_file_path]")
        sys.exit(1)

    processor = FileProcessor()
    
    success = processor.process_file(input_path, output_path)

    if success:
        logger.info("Expense Exporter processing completed successfully")
    else:
        logger.error("Expense Exporter processing failed")
        sys.exit(1)

def main():
    from utils.config import INPUT_FILE_PATH, OUTPUT_FILE_PATH

    parser = argparse.ArgumentParser(description="Expense Exporter")

    # Mode argument (for future extensibility)
    parser.add_argument(
        "--mode",
        choices=["csv"],
        default="csv",
        help="Processing mode: 'csv' for CSV/Excel processing"
    )

    # Optional CLI arguments for overriding .env paths
    parser.add_argument(
        "input_file",
        nargs="?",
        default=INPUT_FILE_PATH,
        help="Path to the input CSV/Excel file (overrides INPUT_FILE_PATH in .env)."
    )
    parser.add_argument(
        "output_file",
        nargs="?",
        default=OUTPUT_FILE_PATH,
        help="Path for the output file (overrides OUTPUT_FILE_PATH in .env)."
    )

    args = parser.parse_args()

    try:
        if args.mode == "csv":
            run_csv_excel_processing(args.input_file, args.output_file)
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()































# """
# Main Entry Point for Expense Exporter

# Processes CSV/Excel files containing HOTEL_INVOICE_PATH, downloads PDFs,
# uploads to cloud storage (S3/Azure), generates links, and updates the file.
# """

# import sys
# import argparse
# import os
# from utils.logger import logger

# # This ensures the script can be run from the project root or directly from 'src'
# current_dir = os.path.dirname(os.path.abspath(__file__))
# if current_dir not in sys.path:
#     sys.path.insert(0, current_dir)
    
# def run_csv_excel_processing():
#     """Run CSV/Excel file processing"""
#     from utils.file_process import FileProcessor
#     from utils.config import INPUT_FILE_PATH, OUTPUT_FILE_PATH

#     processor = FileProcessor()
#     success = processor.process_file(INPUT_FILE_PATH, OUTPUT_FILE_PATH)

#     if success:
#         logger.info("Expense Exporter processing completed successfully")
#     else:
#         logger.error("Expense Exporter processing failed")
#         sys.exit(1)

# def main():
#     """
#     Main entry point with command-line argument parsing.
#     """
#     parser = argparse.ArgumentParser(description="Expense Exporter")
#     parser.add_argument(
#         "--mode",
#         choices=["csv"],
#         default="csv",
#         help="Processing mode: 'csv' for CSV/Excel processing"
#     )

#     args = parser.parse_args()

#     try:
#         if args.mode == "csv":
#             run_csv_excel_processing()
#     except KeyboardInterrupt:
#         logger.info("Process interrupted by user")
#         sys.exit(1)
#     except Exception as e:
#         logger.error(f"Unexpected error: {e}", exc_info=True)
#         sys.exit(1)

# if __name__ == "__main__":
#     main()

