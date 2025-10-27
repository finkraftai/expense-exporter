import sys
import argparse
import os
from utils.file_process import FileProcessor
from utils.config import INPUT_FILE_PATH, OUTPUT_FILE_PATH
from loguru import logger

def premain():
    
    processor = FileProcessor()
    success = processor.process_file(INPUT_FILE_PATH, OUTPUT_FILE_PATH)

    if success:
        logger.info("CSV/Excel processing completed successfully")
    else:
        logger.error("CSV/Excel processing failed")
        sys.exit(1)
        
def main():
    
    try:
        premain()
    except KeyboardInterrupt:
        logger.info("Process interrupted by end-user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()