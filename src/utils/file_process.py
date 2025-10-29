# import os
# import sys
# import time
# import hashlib
# import pandas as pd
# import requests
# from urllib.parse import urlparse
# from .postgres_process import PostgresProcess
# from .mongodb_process import MongoDBProcess
# from .logger import logger 
# from .cloud_helper import CloudHelperFactory
# from .config import AWS_BUCKET_NAME,CLIENT,SOURCE,DOWNLOAD_DIR

# def calculate_md5(file_path, chunk_size=4096):
#     """ Calculate the MD5 hash of a file. """
#     md5 = hashlib.md5()
#     with open(file_path, "rb") as f:
#         for chunk in iter(lambda: f.read(chunk_size), b""):
#             md5.update(chunk)
#     return md5.hexdigest()

# class FileProcessor:
#     """ Processor for CSV/Excel files containing hotel expense data. """

#     def __init__(self):
#         """Initialize the processor with AWS helper."""
#         self.cloud_helper = CloudHelperFactory.create()

#     def process_file(self, input_file_path, output_file_path):
#         """ Process CSV or Excel file containing invoice data. """
#         logger.info("==>Starting Expense Exporter Processor<==")
#         start_time = time.time()

#         # --- Input File Validation ---
#         if not input_file_path:
#             logger.error("Input file path is not configured. Please set INPUT_FILE_PATH in your .env file.")
#             return False
        
#         if not os.path.exists(input_file_path):
#             logger.error(f"Input file not found at the configured path: {input_file_path}")
#             return False

#         # Determine file type and read data
#         if input_file_path.lower().endswith('.csv'):
#             df = pd.read_csv(input_file_path)
#         elif input_file_path.lower().endswith(('.xlsx', '.xls')):
#             df = pd.read_excel(input_file_path)
#         else:
#             logger.error(f"Unsupported file format: {input_file_path}")
#             return False
        
#         logger.info(f"Loaded {len(df)} rows from {input_file_path}")


#       # Step 1: Handle multiple links per row
#         expanded_rows = []
#         for idx, row in df.iterrows():
#             if 'HOTEL_INVOICE_PATH' not in row or pd.isna(row['HOTEL_INVOICE_PATH']):
#                 # If no link, keep the row as is
#                 expanded_rows.append(row)
#                 continue

#             # Split links by common separators (comma, semicolon, pipe)
#             links_str = str(row['HOTEL_INVOICE_PATH']).strip()
#             links = [link.strip() for link in links_str.replace(';', ',').replace('|', ',').split(',') if link.strip()]

#             if len(links) <= 1:
#                 # Single link or empty, keep as is
#                 expanded_rows.append(row)
#             else:
#                 # Multiple links - duplicate row for each link
#                 logger.info(f"Row {idx + 1}: Found {len(links)} links, duplicating row")
#                 for link in links:
#                     new_row = row.copy()
#                     new_row['HOTEL_INVOICE_PATH'] = link
#                     expanded_rows.append(new_row)

#         # Create new dataframe with expanded rows
#         df = pd.DataFrame(expanded_rows)
#         logger.info(f"After link expansion: {len(df)} rows")

#         # Add new columns if they don't exist
#         if 's3_link' not in df.columns:
#             df['s3_link'] = None
#         if 'status' not in df.columns:
#             df['status'] = None
#         if 'file_hash' not in df.columns:
#             df['file_hash'] = None
            
#         processed_count = 0
#         success_count = 0
#         failed_count = 0

#         # Step 2: Process each row (now each row has exactly one link)
#         for idx, row in df.iterrows():
#             try:
#                 logger.debug(f"Processing row {idx + 1}/{len(df)}")

#                 # Check if HOTEL_INVOICE_PATH exists
#                 if 'HOTEL_INVOICE_PATH' not in row or pd.isna(row['HOTEL_INVOICE_PATH']):
#                     logger.warning(f"Row {idx + 1}: Missing HOTEL_INVOICE_PATH")
#                     df.at[idx, 'status'] = "FAILED: Missing HOTEL_INVOICE_PATH"
#                     failed_count += 1
#                     continue

#                 invoice_url = f"https://files.finkraft.ai/{str(row['HOTEL_INVOICE_PATH']).strip()}"

#                 # Step 2a: Download the PDF from the link
#                 local_file_path = self._download_pdf(invoice_url, idx + 1)
#                 if not local_file_path:
#                     logger.warning(f"Row {idx + 1}: PDF download failed for {invoice_url}")
#                     df.at[idx, 'status'] = "FAILED: PDF download failed"
#                     failed_count += 1
#                     continue

#                 # Calculate file hash
#                 file_hash = calculate_md5(local_file_path)
#                 df.at[idx, 'file_hash'] = file_hash

#                 # Step 2b: Upload to S3 with correct path structure
#                 filename = os.path.basename(local_file_path)
#                 s3_key = f"{AWS_BUCKET_NAME}/tmc-portal/{CLIENT}/{filename}"

#                 upload_success = self.cloud_helper.upload_blob(local_file_path, s3_key)
#                 if not upload_success:
#                     logger.warning(f"Row {idx + 1}: S3 upload failed for {local_file_path}")
#                     df.at[idx, 'status'] = "FAILED: S3 upload failed"
#                     failed_count += 1
#                     continue

#                 # Generate a public/pre-signed URL for the file
#                 s3_link = self.cloud_helper.get_file_url(s3_key)
#                 df.at[idx, 'file_hash'] = file_hash
#                 df.at[idx, 's3_link'] = s3_link
#                 logger.info(f"Row {idx + 1}: Generated Cloud link: {s3_link}")
#                 df.at[idx, 'status'] = "SUCCESS"

#                 # Prepare data for MongoDB - include ALL columns from the row plus new ones
#                 mongo_data = row.to_dict()  # Start with all original columns
#                 mongo_data.update({
#                     'corp_name': CLIENT,
#                     'hotel_invoice_path': invoice_url,
#                     's3_link': s3_link,
#                     'file_hash': file_hash,
#                     'status': 'SUCCESS',
#                     'processed_at': pd.Timestamp.now(),
#                     'source': SOURCE,
#                     'client_name': CLIENT
#                 })
                
#                 # Step 2c: Push row data to MongoDB
#                 with MongoDBProcess() as mongo_helper:
#                     mongo_id = mongo_helper.insert_invoice_data(mongo_data)
#                     if mongo_id:
#                         logger.info(f"✓ [{idx + 1}/{len(df)}] MongoDB insert successful (ID: {mongo_id})")
#                         source_id = str(mongo_id)  # Use MongoDB document ID as source_i
#                     else:
#                         logger.warning(f"Row {idx + 1}: MongoDB insert failed")
#                         df.at[idx, 'status'] = "FAILED: MongoDB insert failed"
#                         failed_count += 1
#                         continue

#                 # Step 2d: Insert metadata to PostgreSQL hotel_invoice table
#                 pg_data = {
#                     'source': 'tmc-portal',  # As specified
#                     'source_id': source_id,  # MongoDB document ID
#                     'client_name': CLIENT,  # From env
#                     'file_url': s3_link,  # S3 URL
#                     'file_hash': file_hash,
#                     'status': 'PENDING',  # As specified
#                     'match_status': None,  # Keep null
#                     '2b_id': None,  # Keep null
#                     'booking_id': None,  # Keep null
#                     'client_gstin': None,
#                     'hotel_gstin': None,
#                     'invoice_number': None,
#                     'invoice_date': None,
#                     'gst_amount': None,
#                     'remarks': f"Processed from {CLIENT}",
#                     'followup_tracking_id': None,  
#                     'updated_on':
#                 }

#                 # Updated mapping from Excel column names to PostgreSQL field names
#                 column_mapping = {
#                     'CLIENT_GST_NO': 'client_gstin',
#                     'HOTEL_GST_NUMBER': 'hotel_gstin',
#                     'Q2T_INVOICE_NO': 'invoice_number',
#                     'HOTEL_INVOICE_DATE': 'invoice_date',
#                     'TOTAL INVOICE AMOUNT': 'gst_amount',
#                     # Other fields like BOOKING_ID, SOURCE_ID, etc. can be added if needed
#                 }

#                   # Extract fields from the row using the mapping
#                 for excel_col, pg_field in column_mapping.items():
#                     if excel_col in row and not pd.isna(row[excel_col]):
#                         if pg_field == 'gst_amount':
#                             pg_data[pg_field] = float(row[excel_col])
#                         elif pg_field == 'invoice_date':
#                             # Convert to datetime object, which psycopg2 can handle
#                             pg_data[pg_field] = pd.to_datetime(row[excel_col])
#                         else:
#                             pg_data[pg_field] = str(row[excel_col])

#                 pg_result = PostgresProcess.insert_full_invoice_data(pg_data)
#                 if pg_result:
#                     logger.info(f"✓ [{idx + 1}/{len(df)}] PostgreSQL insert successful (ID: {pg_result.get('id', 'N/A')})")
#                 else:
#                     logger.warning(f"Row {idx + 1}: PostgreSQL insert failed")
#                     df.at[idx, 'status'] = "FAILED: PostgreSQL insert failed"
#                     failed_count += 1
#                     continue
                   
#                 logger.info(f"✓ [{idx + 1}/{len(df)}] Successfully processed: {CLIENT} - {os.path.basename(local_file_path)} (Hash: {file_hash[:8]}...)")
#                 # Clean up local file after successful processing
#                 try:
#                     os.remove(local_file_path)
#                     logger.debug(f"Cleaned up local file: {local_file_path}")
#                 except Exception as cleanup_e:
#                     logger.warning(f"Failed to clean up local file {local_file_path}: {cleanup_e}")

#             except Exception as e:
#                 logger.error(f"✗ [{idx + 1}/{len(df)}] Failed processing row: {e}", exc_info=True)
#                 df.at[idx, 'status'] = f"FAILED: {str(e)}"
#                 failed_count += 1
#                 continue
        
#         # Step 3: Save updated file with processing status and generated links
#         if output_file_path:
#             try:
#                 if output_file_path.lower().endswith('.csv'):
#                     df.to_csv(output_file_path, index=False)
#                 elif output_file_path.lower().endswith(('.xlsx', '.xls')):
#                     df.to_excel(output_file_path, index=False)
#                 logger.info(f"Updated file saved to: {output_file_path}")
#             except Exception as e:
#                 logger.error(f"Failed to save updated file: {e}")
#                 return False
            
#         #Upload output file to cloud storage
#         if self.cloud_helper and os.path.exists(output_file_path):
#             try:
#                 upload_url = self.cloud_helper.upload_output_file(output_file_path)
#                 logger.info(f"✅ Output file uploaded successfully to: {upload_url}")
#             except Exception as e:
#                 logger.error(f"Failed to upload output file: {e}", exc_info=True)

#         elapsed = time.time() - start_time
#         logger.info("="*80)
#         logger.info("Expense Exporter Summary:")
#         logger.info(f"  Total rows: {len(df)}")
#         logger.info(f"  Processed: {processed_count}")
#         logger.info(f"  Successful: {success_count}")
#         logger.info(f"  Failed: {failed_count}")
#         logger.info(f"  Success rate: {(success_count / len(df) * 100):.1f}%" if len(df) > 0 else "0%")
#         logger.info(f"  Completed in {elapsed:.2f}s")
#         logger.info("="*80)

#         return True
    

#     def _download_pdf(self, url, row_num):
#         """ Download PDF from URL. """
#         try:
#             logger.debug(f"Downloading PDF from: {url}")
#             os.makedirs(DOWNLOAD_DIR, exist_ok=True)
#             parsed_url = urlparse(url)
#             filename = os.path.basename(parsed_url.path)
#             if not filename or '.' not in filename:
#                 filename = f"invoice_{row_num}.pdf"
#             local_path = os.path.join(DOWNLOAD_DIR, filename)

#             # Download file with retry logic
#             max_retries = 2
#             for attempt in range(max_retries):
#                 try:
#                     response = requests.get(url, timeout=30, stream=True)
#                     response.raise_for_status()

#                     with open(local_path, 'wb') as f:
#                         for chunk in response.iter_content(chunk_size=8192):
#                             f.write(chunk)

#                     # Verify file was downloaded
#                     if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
#                         logger.info(f"Downloaded PDF: {url} → {local_path} ({os.path.getsize(local_path)} bytes)")
#                         return local_path
#                     else:
#                         logger.warning(f"Downloaded file is empty or missing: {local_path}")
#                         return None

#                 except requests.exceptions.RequestException as e:
#                     if attempt < max_retries - 1:
#                         logger.warning(f"Download attempt {attempt + 1} failed for {url}: {e}. Retrying...")
#                         time.sleep(2 ** attempt)
#                     else:
#                         logger.error(f"All download attempts failed for {url}: {e}")
#                         return None

#         except Exception as e:
#             logger.error(f"Failed to download PDF from {url}: {e}", exc_info=True)
#             return None




# import os
# import sys
# import time
# import hashlib
# import pandas as pd
# import requests
# from urllib.parse import urlparse
# from .postgres_process import PostgresProcess
# from .mongodb_process import MongoDBProcess
# from .logger import logger 
# from .cloud_helper import CloudHelperFactory
# from .config import S3_UPLOAD_BUCKET, S3_UPLOAD_PREFIX, CLIENT, SOURCE, DOWNLOAD_DIR


# def calculate_md5(file_path, chunk_size=4096):
#     """Calculate the MD5 hash of a file."""
#     md5 = hashlib.md5()
#     with open(file_path, "rb") as f:
#         for chunk in iter(lambda: f.read(chunk_size), b""):
#             md5.update(chunk)
#     return md5.hexdigest()

# class FileProcessor:
#     """Processor for CSV/Excel files containing hotel expense data."""

#     def __init__(self):
#         """Initialize the processor with Cloud Helper (AWS/Azure, etc.)"""
#         self.cloud_helper = CloudHelperFactory.create()

#     def process_file(self, input_file_path, output_file_path=None):
#         """Process CSV or Excel file containing invoice data."""
#         logger.info("==> Starting Expense Exporter Processor <==")
#         start_time = time.time()

#         # --- Input File Validation ---
#         if not input_file_path:
#             logger.error("Input file path is not configured. Please provide a valid file.")
#             return False

#         if not os.path.exists(input_file_path):
#             logger.error(f"Input file not found: {input_file_path}")
#             return False

#         # --- Determine File Type ---
#         if input_file_path.lower().endswith('.csv'):
#             df = pd.read_csv(input_file_path)
#         elif input_file_path.lower().endswith(('.xlsx', '.xls')):
#             df = pd.read_excel(input_file_path, engine='openpyxl')
#         else:
#             logger.error(f"Unsupported file format: {input_file_path}")
#             return False

#         logger.info(f"Loaded {len(df)} rows from {input_file_path}")

#         # --- Step 1: Handle multiple links per row ---
#         expanded_rows = []
#         for idx, row in df.iterrows():
#             if 'HOTEL_INVOICE_PATH' not in row or pd.isna(row['HOTEL_INVOICE_PATH']):
#                 expanded_rows.append(row)
#                 continue

#             links_str = str(row['HOTEL_INVOICE_PATH']).strip()
#             links = [link.strip() for link in links_str.replace(';', ',').replace('|', ',').split(',') if link.strip()]

#             if len(links) <= 1:
#                 expanded_rows.append(row)
#             else:
#                 logger.info(f"Row {idx + 1}: Found {len(links)} links, duplicating row")
#                 for link in links:
#                     new_row = row.copy()
#                     new_row['HOTEL_INVOICE_PATH'] = link
#                     expanded_rows.append(new_row)

#         df = pd.DataFrame(expanded_rows)
#         logger.info(f"After link expansion: {len(df)} rows")

#         # --- Add necessary columns ---
#         for col in ['s3_link', 'status', 'file_hash']:
#             if col not in df.columns:
#                 df[col] = None

#         processed_count = 0
#         success_count = 0
#         failed_count = 0

#         # --- Step 2: Process each row ---
#         for idx, row in df.iterrows():
#             try:
#                 logger.debug(f"Processing row {idx + 1}/{len(df)}")

#                 # Validate hotel invoice path
#                 if 'HOTEL_INVOICE_PATH' not in row or pd.isna(row['HOTEL_INVOICE_PATH']):
#                     df.at[idx, 'status'] = "FAILED: Missing HOTEL_INVOICE_PATH"
#                     failed_count += 1
#                     continue

#                 invoice_url = f"https://files.finkraft.ai/{str(row['HOTEL_INVOICE_PATH']).strip()}"

#                 # Step 2a: Download PDF
#                 local_file_path = self._download_pdf(invoice_url, idx + 1)
#                 if not local_file_path:
#                     df.at[idx, 'status'] = "FAILED: PDF download failed"
#                     failed_count += 1
#                     continue

#                 # Step 2b: Calculate MD5 hash
#                 file_hash = calculate_md5(local_file_path)
#                 df.at[idx, 'file_hash'] = file_hash

#                 # Step 2c: Upload to cloud (S3 or equivalent)
#                 filename = os.path.basename(local_file_path)
#                 # ✅ FIXED: Removed bucket name from key (was duplicated)
#                 s3_key = f"tmc-portal/{CLIENT}/{filename}"

#                 upload_success = self.cloud_helper.upload_blob(local_file_path, s3_key)
#                 if not upload_success:
#                     df.at[idx, 'status'] = "FAILED: Upload failed"
#                     failed_count += 1
#                     continue

#                 # Generate link
#                 s3_link = self.cloud_helper.get_file_url(s3_key)
#                 df.at[idx, 's3_link'] = s3_link
#                 df.at[idx, 'status'] = "SUCCESS"

#                 # Step 2d: Prepare MongoDB data
#                 mongo_data = row.to_dict()
#                 mongo_data.update({
#                     'corp_name': CLIENT,
#                     'hotel_invoice_path': invoice_url,
#                     's3_link': s3_link,
#                     'file_hash': file_hash,
#                     'status': 'SUCCESS',
#                     'processed_at': pd.Timestamp.now(),
#                     'source': SOURCE,
#                     'client_name': CLIENT
#                 })

#                 # Insert into MongoDB
#                 with MongoDBProcess() as mongo_helper:
#                     mongo_id = mongo_helper.insert_invoice_data(mongo_data)
#                     if not mongo_id:
#                         df.at[idx, 'status'] = "FAILED: MongoDB insert failed"
#                         failed_count += 1
#                         continue

#                 # Step 2e: Prepare PostgreSQL data
#                 pg_data = {
#                     'source': 'tmc-portal',
#                     'source_id': str(mongo_id),
#                     'client_name': CLIENT,
#                     'file_url': s3_link,
#                     'file_hash': file_hash,
#                     'status': 'PENDING',
#                     'match_status': None,
#                     '2b_id': None,
#                     'booking_id': None,
#                     'client_gstin': None,
#                     'hotel_gstin': None,
#                     'invoice_number': None,
#                     'invoice_date': None,
#                     'gst_amount': None,
#                     'remarks': f"Processed from {CLIENT}",
#                     'followup_tracking_id': None,
#                     'updated_on': pd.Timestamp.now(),  # ✅ Added back
#                 }

#                 # ✅ Added BOOKING_ID mapping (was missing)
#                 column_mapping = {
#                     'CLIENT_GST_NO': 'client_gstin',
#                     'HOTEL_GST_NUMBER': 'hotel_gstin',
#                     'Q2T_INVOICE_NO': 'invoice_number',
#                     'HOTEL_INVOICE_DATE': 'invoice_date',
#                     'TOTAL INVOICE AMOUNT': 'gst_amount',
#                     'BOOKING_ID': 'booking_id'
#                 }

#                 for excel_col, pg_field in column_mapping.items():
#                     if excel_col in row and not pd.isna(row[excel_col]):
#                         if pg_field == 'gst_amount':
#                             pg_data[pg_field] = float(row[excel_col])
#                         elif pg_field == 'invoice_date':
#                             pg_data[pg_field] = pd.to_datetime(row[excel_col])
#                         else:
#                             pg_data[pg_field] = str(row[excel_col])

#                 pg_result = PostgresProcess.insert_full_invoice_data(pg_data)
#                 if not pg_result:
#                     df.at[idx, 'status'] = "FAILED: PostgreSQL insert failed"
#                     failed_count += 1
#                     continue

#                 # ✅ Add counters (missing previously)
#                 success_count += 1
#                 processed_count += 1

#                 logger.info(f"✓ [{idx + 1}/{len(df)}] Processed successfully: {filename}")

#                 # Clean up local file
#                 try:
#                     os.remove(local_file_path)
#                 except Exception as cleanup_e:
#                     logger.warning(f"Cleanup failed for {local_file_path}: {cleanup_e}")

#             except Exception as e:
#                 logger.error(f"✗ [{idx + 1}/{len(df)}] Failed processing row: {e}", exc_info=True)
#                 df.at[idx, 'status'] = f"FAILED: {str(e)}"
#                 failed_count += 1
#                 continue

#         # --- Step 3: Save Updated Output File ---
#         if not output_file_path:
#             output_file_path = input_file_path

#         try:
#             if output_file_path.lower().endswith('.csv'):
#                 df.to_csv(output_file_path, index=False)
#             else:
#                 df.to_excel(output_file_path, index=False, engine='openpyxl')
#             logger.info(f"Updated file saved to: {output_file_path}")
#         except Exception as e:
#             logger.error(f"Failed to save updated file: {e}")
#             return False

#         # --- Step 4: Upload Processed Output File ---
#         try:
#             if os.path.exists(output_file_path):
#                 upload_url = self.cloud_helper.upload_output_file(output_file_path)
#                 logger.info(f"✅ Output file uploaded successfully to: {upload_url}")
#         except Exception as e:
#             logger.error(f"Failed to upload output file: {e}", exc_info=True)

#         # --- Final Summary ---
#         elapsed = time.time() - start_time
#         logger.info("=" * 80)
#         logger.info("Expense Exporter Summary:")
#         logger.info(f"  Total rows: {len(df)}")
#         logger.info(f"  Processed: {processed_count}")
#         logger.info(f"  Successful: {success_count}")
#         logger.info(f"  Failed: {failed_count}")
#         logger.info(f"  Success rate: {(success_count / len(df) * 100):.1f}%" if len(df) > 0 else "0%")
#         logger.info(f"  Completed in {elapsed:.2f}s")
#         logger.info("=" * 80)
#         return True

#     def _download_pdf(self, url, row_num):
#         """Download PDF from URL with retry logic."""
#         try:
#             logger.debug(f"Downloading PDF from: {url}")
#             os.makedirs(DOWNLOAD_DIR, exist_ok=True)
#             parsed_url = urlparse(url)
#             filename = os.path.basename(parsed_url.path) or f"invoice_{row_num}.pdf"
#             local_path = os.path.join(DOWNLOAD_DIR, filename)

#             # ✅ Increased retries from 2 → 3 (from old code)
#             max_retries = 3
#             for attempt in range(max_retries):
#                 try:
#                     response = requests.get(url, timeout=30, stream=True)
#                     response.raise_for_status()
#                     with open(local_path, 'wb') as f:
#                         for chunk in response.iter_content(chunk_size=8192):
#                             f.write(chunk)

#                     if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
#                         logger.info(f"Downloaded: {url} → {local_path}")
#                         return local_path
#                     else:
#                         logger.warning(f"Downloaded file is empty: {local_path}")
#                         return None
#                 except requests.exceptions.RequestException as e:
#                     if attempt < max_retries - 1:
#                         logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying...")
#                         time.sleep(2 ** attempt)
#                     else:
#                         logger.error(f"All download attempts failed for {url}: {e}")
#                         return None
#         except Exception as e:
#             logger.error(f"Failed to download PDF from {url}: {e}", exc_info=True)
#             return None

import os
import sys
import time
import hashlib
import pandas as pd
# import requests  # Not used in test mode
from urllib.parse import urlparse
from .postgres_process import PostgresProcess
from .mongodb_process import MongoDBProcess
from .logger import logger 
from .cloud_helper import CloudHelperFactory
from .config import S3_UPLOAD_BUCKET, S3_UPLOAD_PREFIX, CLIENT, SOURCE, DOWNLOAD_DIR

def calculate_md5(file_path, chunk_size=4096):
    """Calculate the MD5 hash of a file."""
    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            md5.update(chunk)
    return md5.hexdigest()

# Toggle this to False to restore real download/upload behavior
TEST_BYPASS_DOWNLOAD_AND_UPLOAD = True

class FileProcessor:
    """Processor for CSV/Excel files containing hotel expense data."""

    def __init__(self):
        """Initialize the processor with Cloud Helper (AWS/Azure, etc.)"""
        # In test mode we still create cloud helper (some tests may assert its presence)
        self.cloud_helper = CloudHelperFactory.create() if not TEST_BYPASS_DOWNLOAD_AND_UPLOAD else None

    def process_file(self, input_file_path, output_file_path=None):
        """Process CSV or Excel file containing invoice data."""
        logger.info("==> Starting Expense Exporter Processor (TEST MODE bypass={}) <==".format(TEST_BYPASS_DOWNLOAD_AND_UPLOAD))
        start_time = time.time()

        # --- Input File Validation ---
        if not input_file_path:
            logger.error("Input file path is not configured. Please provide a valid file.")
            return False

        if not os.path.exists(input_file_path):
            logger.error(f"Input file not found: {input_file_path}")
            return False

        # --- Read File ---
        if input_file_path.lower().endswith('.csv'):
            df = pd.read_csv(input_file_path)
        elif input_file_path.lower().endswith(('.xlsx', '.xls')):
            df = pd.read_excel(input_file_path, engine='openpyxl')
        else:
            logger.error(f"Unsupported file format: {input_file_path}")
            return False

        logger.info(f"Loaded {len(df)} rows from {input_file_path}")

        # --- Expand rows for multiple links in HOTEL_INVOICE_PATH ---
        expanded_rows = []
        for idx, row in df.iterrows():
            if 'HOTEL_INVOICE_PATH' not in row or pd.isna(row['HOTEL_INVOICE_PATH']):
                expanded_rows.append(row)
                continue

            links_str = str(row['HOTEL_INVOICE_PATH']).strip()
            links = [link.strip() for link in links_str.replace(';', ',').replace('|', ',').split(',') if link.strip()]

            if len(links) <= 1:
                expanded_rows.append(row)
            else:
                logger.info(f"Row {idx + 1}: Found {len(links)} links, duplicating row")
                for link in links:
                    new_row = row.copy()
                    new_row['HOTEL_INVOICE_PATH'] = link
                    expanded_rows.append(new_row)

        df = pd.DataFrame(expanded_rows)
        logger.info(f"After link expansion: {len(df)} rows")

        # --- Ensure columns exist ---
        for col in ['s3_link', 'status', 'file_hash']:
            if col not in df.columns:
                df[col] = None

        processed_count = 0
        success_count = 0
        failed_count = 0

        # --- Process each row ---
        for idx, row in df.iterrows():
            try:
                logger.debug(f"Processing row {idx + 1}/{len(df)}")

                # Validate HOTEL_INVOICE_PATH
                if 'HOTEL_INVOICE_PATH' not in row or pd.isna(row['HOTEL_INVOICE_PATH']):
                    df.at[idx, 'status'] = "FAILED: Missing HOTEL_INVOICE_PATH"
                    failed_count += 1
                    continue

                invoice_path_value = str(row['HOTEL_INVOICE_PATH']).strip()
                invoice_url = f"https://files.finkraft.ai/{invoice_path_value}"

                if TEST_BYPASS_DOWNLOAD_AND_UPLOAD:
                    # ------------------------------
                    # TEST MODE: BYPASS download + upload
                    # ------------------------------
                    # Use a dummy local filename and hash - do NOT attempt network operations.
                    # Commented-out lines show the original operations to restore later.
                    
                    # Original download:
                    # local_file_path = self._download_pdf(invoice_url, idx + 1)
                    # if not local_file_path:
                    #     df.at[idx, 'status'] = "FAILED: PDF download failed"
                    #     failed_count += 1
                    #     continue

                    # Create dummy local file name (no actual file created)
                    local_file_path = f"dummy_file_{idx + 1}.pdf"
                    # Create deterministic dummy hash (so DB sees something unique/stable)
                    file_hash = f"dummy_hash_{idx + 1}"
                    df.at[idx, 'file_hash'] = file_hash

                    # Original upload:
                    # filename = os.path.basename(local_file_path)
                    # s3_key = f"tmc-portal/{CLIENT}/{filename}"
                    # upload_success = self.cloud_helper.upload_blob(local_file_path, s3_key)
                    # if not upload_success:
                    #     df.at[idx, 'status'] = "FAILED: Upload failed"
                    #     failed_count += 1
                    #     continue

                    # Dummy S3 link for testing
                    s3_link = f"https://dummy-s3-link.test/{local_file_path}"
                    df.at[idx, 's3_link'] = s3_link
                    df.at[idx, 'status'] = "SUCCESS (TEST)"
                else:
                    # ------------------------------
                    # REAL MODE: perform download + upload
                    # ------------------------------
                    local_file_path = self._download_pdf(invoice_url, idx + 1)
                    if not local_file_path:
                        df.at[idx, 'status'] = "FAILED: PDF download failed"
                        failed_count += 1
                        continue

                    file_hash = calculate_md5(local_file_path)
                    df.at[idx, 'file_hash'] = file_hash

                    filename = os.path.basename(local_file_path)
                    s3_key = f"tmc-portal/{CLIENT}/{filename}"

                    upload_success = self.cloud_helper.upload_blob(local_file_path, s3_key)
                    if not upload_success:
                        df.at[idx, 'status'] = "FAILED: Upload failed"
                        failed_count += 1
                        continue

                    s3_link = self.cloud_helper.get_file_url(s3_key)
                    df.at[idx, 's3_link'] = s3_link
                    df.at[idx, 'status'] = "SUCCESS"

                # --- Prepare MongoDB document and insert ---
                mongo_data = row.to_dict()
                mongo_data.update({
                    'corp_name': CLIENT,
                    'hotel_invoice_path': invoice_url,
                    's3_link': df.at[idx, 's3_link'],
                    'file_hash': df.at[idx, 'file_hash'],
                    'status': df.at[idx, 'status'],
                    'processed_at': pd.Timestamp.now(),
                    'source': SOURCE,
                    'client_name': CLIENT
                })

                with MongoDBProcess() as mongo_helper:
                    mongo_id = mongo_helper.insert_invoice_data(mongo_data)
                    if not mongo_id:
                        df.at[idx, 'status'] = "FAILED: MongoDB insert failed"
                        failed_count += 1
                        continue

                # --- Prepare PostgreSQL metadata and insert ---
                pg_data = {
                    'source': 'tmc-portal',
                    'source_id': str(mongo_id),
                    'client_name': CLIENT,
                    'file_url': df.at[idx, 's3_link'],
                    'file_hash': df.at[idx, 'file_hash'],
                    'status': 'PENDING',
                    'match_status': None,
                    '2b_id': None,
                    'booking_id': None,
                    'client_gstin': None,
                    'hotel_gstin': None,
                    'invoice_number': None,
                    'invoice_date': None,
                    'gst_amount': None,
                    'remarks': f"Processed from {CLIENT}",
                    'followup_tracking_id': None,
                    'updated_on': pd.Timestamp.now(),
                }

                column_mapping = {
                    'CLIENT_GST_NO': 'client_gstin',
                    'HOTEL_GST_NUMBER': 'hotel_gstin',
                    'Q2T_INVOICE_NO': 'invoice_number',
                    'HOTEL_INVOICE_DATE': 'invoice_date',
                    'TOTAL INVOICE AMOUNT': 'gst_amount',
                    'BOOKING_ID': 'booking_id'
                }

                for excel_col, pg_field in column_mapping.items():
                    if excel_col in row and not pd.isna(row[excel_col]):
                        if pg_field == 'gst_amount':
                            pg_data[pg_field] = float(row[excel_col])
                        elif pg_field == 'invoice_date':
                            pg_data[pg_field] = pd.to_datetime(row[excel_col])
                        else:
                            pg_data[pg_field] = str(row[excel_col])

                pg_result = PostgresProcess.insert_full_invoice_data(pg_data)
                if not pg_result:
                    df.at[idx, 'status'] = "FAILED: PostgreSQL insert failed"
                    failed_count += 1
                    continue

                # --- Update counters (important for summary) ---
                processed_count += 1
                success_count += 1

                logger.info(f"✓ [{idx + 1}/{len(df)}] Processed (TEST_BYPASS={TEST_BYPASS_DOWNLOAD_AND_UPLOAD}): {df.at[idx, 's3_link']} (Hash: {str(df.at[idx, 'file_hash'])[:12]}...)")

                # Clean up local file only in real mode
                if not TEST_BYPASS_DOWNLOAD_AND_UPLOAD:
                    try:
                        os.remove(local_file_path)
                        logger.debug(f"Cleaned up local file: {local_file_path}")
                    except Exception as cleanup_e:
                        logger.warning(f"Failed to clean up local file {local_file_path}: {cleanup_e}")

            except Exception as e:
                logger.error(f"✗ [{idx + 1}/{len(df)}] Failed processing row: {e}", exc_info=True)
                df.at[idx, 'status'] = f"FAILED: {str(e)}"
                failed_count += 1
                continue

        # --- Save the updated file ---
        if not output_file_path:
            output_file_path = input_file_path

        try:
            if output_file_path.lower().endswith('.csv'):
                df.to_csv(output_file_path, index=False)
            else:
                df.to_excel(output_file_path, index=False, engine='openpyxl')
            logger.info(f"Updated file saved to: {output_file_path}")
        except Exception as e:
            logger.error(f"Failed to save updated file: {e}", exc_info=True)
            return False

        # --- Optionally upload processed output file (skipped in TEST mode) ---
        if not TEST_BYPASS_DOWNLOAD_AND_UPLOAD and self.cloud_helper and os.path.exists(output_file_path):
            try:
                upload_url = self.cloud_helper.upload_output_file(output_file_path)
                logger.info(f"✅ Output file uploaded successfully to: {upload_url}")
            except Exception as e:
                logger.error(f"Failed to upload output file: {e}", exc_info=True)

        # --- Summary ---
        elapsed = time.time() - start_time
        logger.info("=" * 80)
        logger.info("Expense Exporter Summary:")
        logger.info(f"  Total rows: {len(df)}")
        logger.info(f"  Processed: {processed_count}")
        logger.info(f"  Successful: {success_count}")
        logger.info(f"  Failed: {failed_count}")
        logger.info(f"  Success rate: {(success_count / len(df) * 100):.1f}%" if len(df) > 0 else "0%")
        logger.info(f"  Completed in {elapsed:.2f}s")
        logger.info("=" * 80)
        return True

    def _download_pdf(self, url, row_num):
        """Download PDF from URL with retry logic (kept for completeness)."""
        try:
            logger.debug(f"Downloading PDF from: {url}")
            os.makedirs(DOWNLOAD_DIR, exist_ok=True)
            parsed_url = urlparse(url)
            filename = os.path.basename(parsed_url.path) or f"invoice_{row_num}.pdf"
            local_path = os.path.join(DOWNLOAD_DIR, filename)

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    import requests  # local import so module doesn't require requests in test mode
                    response = requests.get(url, timeout=30, stream=True)
                    response.raise_for_status()
                    with open(local_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)

                    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                        logger.info(f"Downloaded: {url} → {local_path}")
                        return local_path
                    else:
                        logger.warning(f"Downloaded file is empty: {local_path}")
                        return None
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                        time.sleep(2 ** attempt)
                    else:
                        logger.error(f"All download attempts failed for {url}: {e}")
                        return None
        except Exception as e:
            logger.error(f"Failed to download PDF from {url}: {e}", exc_info=True)
            return None
