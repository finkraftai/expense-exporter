import os
import sys
import time
import hashlib
import pandas as pd
import requests
from urllib.parse import urlparse
from .postgres_process import PostgresProcess
from .mongodb_process import MongoDBProcess
from .logger import logger
from .cloud_helper import CloudHelperFactory
from .config import CLIENT, SOURCE, DOWNLOAD_DIR

def calculate_md5(file_path, chunk_size=4096):
    """Calculate the MD5 hash of a file."""
    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            md5.update(chunk)
    return md5.hexdigest()
class FileProcessor:
    """Processor for CSV/Excel files containing hotel expense data."""

    def __init__(self):
        """Initialize the processor with cloud helper."""
        self.cloud_helper = CloudHelperFactory.create()

    def process_file(self, input_file_path, output_file_path):
        """Process CSV or Excel file containing invoice data."""
        logger.info("==> Starting Expense Exporter Processor <==")
        start_time = time.time()

        # Input file validation
        if not input_file_path:
            logger.error("Input file path is not configured. Please set INPUT_FILE_PATH in your .env file.")
            return False

        if not os.path.exists(input_file_path):
            logger.error(f"Input file not found at the configured path: {input_file_path}")
            return False

        # Determine file type and read data
        if input_file_path.lower().endswith('.csv'):
            df = pd.read_csv(input_file_path)
        elif input_file_path.lower().endswith(('.xlsx', '.xls')):
            df = pd.read_excel(input_file_path)
        else:
            logger.error(f"Unsupported file format: {input_file_path}")
            return False

        logger.info(f"Loaded {len(df)} rows from {input_file_path}")

        # Add external_file_link column with dummy URLs
        if 'external_file_link' not in df.columns:
            df['external_file_link'] = [f"https://files.finkraft.ai/invoice_{i+1}.pdf" for i in range(len(df))]
            # logger.info("Added 'external_file_link' column with dummy URLs")

        # Step 1: Handle multiple links per row (using external_file_link)
        expanded_rows = []
        for idx, row in df.iterrows():
            if 'external_file_link' not in row or pd.isna(row['external_file_link']):
                expanded_rows.append(row)
                continue

            links_str = str(row['external_file_link']).strip()
            links = [link.strip() for link in links_str.replace(';', ',').replace('|', ',').split(',') if link.strip()]

            if len(links) <= 1:
                expanded_rows.append(row)
            else:
                logger.info(f"Row {idx + 1}: Found {len(links)} links, duplicating row")
                for link in links:
                    new_row = row.copy()
                    new_row['external_file_link'] = link
                    expanded_rows.append(new_row)

        df = pd.DataFrame(expanded_rows)
        logger.info(f"After link expansion: {len(df)} rows")

        # Add new columns if they don't exist
        for col in ['s3_link', 'status', 'file_hash']:
            if col not in df.columns:
                df[col] = None

        processed_count = 0
        success_count = 0
        failed_count = 0

        # Step 2: Process each row
        for idx, row in df.iterrows():
            try:
                logger.debug(f"Processing row {idx + 1}/{len(df)}")

                if 'external_file_link' not in row or pd.isna(row['external_file_link']):
                    logger.warning(f"Row {idx + 1}: Missing external_file_link")
                    df.at[idx, 'status'] = "FAILED: Missing external_file_link"
                    failed_count += 1
                    continue

                file_url = str(row['external_file_link']).strip()

                # Step 2a: Download the PDF
                local_file_path = self._download_pdf(file_url, idx + 1)
                if not local_file_path:
                    logger.warning(f"Row {idx + 1}: PDF download failed for {file_url}")
                    df.at[idx, 'status'] = "FAILED: PDF download failed"
                    failed_count += 1
                    continue

                # Step 2b: Calculate file hash
                file_hash = calculate_md5(local_file_path)
                df.at[idx, 'file_hash'] = file_hash

                # Step 2c: Upload to S3 with specified path structure
                filename = os.path.basename(local_file_path)
                s3_key = f"fink-hotel-invoice-scraped/tmc_portal/{CLIENT}/file_{idx+1}.pdf"

                upload_success = self.cloud_helper.upload_blob(local_file_path, s3_key)
                if not upload_success:
                    logger.warning(f"Row {idx + 1}: S3 upload failed for {local_file_path}")
                    df.at[idx, 'status'] = "FAILED: S3 upload failed"
                    failed_count += 1
                    continue

                # Generate S3 link
                s3_link = self.cloud_helper.get_file_url(s3_key)
                df.at[idx, 's3_link'] = s3_link
                df.at[idx, 'status'] = "SUCCESS"
                logger.info(f"Row {idx + 1}: Generated S3 link: {s3_link}")

                # Step 2d: Prepare MongoDB data (all Excel columns + new fields)
                mongo_data = row.to_dict()
                mongo_data.update({
                    'external_file_link': file_url,
                    's3_link': s3_link,
                    'status': 'SUCCESS',
                    'file_hash': file_hash,
                    'corp_name': CLIENT,
                    'processed_at': pd.Timestamp.now(),
                    'source': SOURCE,
                    'client_name': CLIENT
                })

                # Insert into MongoDB
                with MongoDBProcess() as mongo_helper:
                    mongo_id = mongo_helper.insert_invoice_data(mongo_data)
                    if mongo_id:
                        logger.info(f"✓ [{idx + 1}/{len(df)}] MongoDB insert successful (ID: {mongo_id})")
                        source_id = str(mongo_id)
                    else:
                        logger.warning(f"Row {idx + 1}: MongoDB insert failed")
                        df.at[idx, 'status'] = "FAILED: MongoDB insert failed"
                        failed_count += 1
                        continue

                # Step 2e: Insert metadata to PostgreSQL invoice_uploads table
                pg_data = {
                    'source': 'tmc-portal',
                    'source_id': source_id,
                    'client_name': CLIENT,
                    'file_url': s3_link,
                    'file_hash': file_hash,
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
                    'updated_on': pd.Timestamp.now()
                }

                # Mapping from Excel column names to PostgreSQL field names
                column_mapping = {
                    'CLIENT_GST_NO': 'client_gstin',
                    'HOTEL_GST_NUMBER': 'hotel_gstin',
                    'Q2T_INVOICE_NO': 'invoice_number',
                    'HOTEL_INVOICE_DATE': 'invoice_date',
                    'TOTAL INVOICE AMOUNT': 'gst_amount',
                    'BOOKING_ID': 'booking_id'
                }

                # Extract fields from the row using the mapping
                for excel_col, pg_field in column_mapping.items():
                    if excel_col in row and not pd.isna(row[excel_col]):
                        if pg_field == 'gst_amount':
                            pg_data[pg_field] = float(row[excel_col])
                        elif pg_field == 'invoice_date':
                            pg_data[pg_field] = pd.to_datetime(row[excel_col])
                        else:
                            pg_data[pg_field] = str(row[excel_col])

                pg_result = PostgresProcess.insert_full_invoice_data(pg_data)
                if pg_result:
                    logger.info(f"✓ [{idx + 1}/{len(df)}] PostgreSQL insert successful (ID: {pg_result.get('id', 'N/A')})")
                else:
                    logger.warning(f"Row {idx + 1}: PostgreSQL insert failed")
                    df.at[idx, 'status'] = "FAILED: PostgreSQL insert failed"
                    failed_count += 1
                    continue

                logger.info(f"✓ [{idx + 1}/{len(df)}] Successfully processed: {CLIENT} (Hash: {file_hash[:8]}...)")

                # Clean up local file
                try:
                    os.remove(local_file_path)
                    logger.debug(f"Cleaned up local file: {local_file_path}")
                except Exception as cleanup_e:
                    logger.warning(f"Failed to clean up local file {local_file_path}: {cleanup_e}")

                processed_count += 1
                success_count += 1

            except Exception as e:
                logger.error(f"✗ [{idx + 1}/{len(df)}] Failed processing row: {e}", exc_info=True)
                df.at[idx, 'status'] = f"FAILED: {str(e)}"
                failed_count += 1
                continue

        # Step 3: Save updated file
        if output_file_path:
            try:
                if output_file_path.lower().endswith('.csv'):
                    df.to_csv(output_file_path, index=False)
                elif output_file_path.lower().endswith(('.xlsx', '.xls')):
                    df.to_excel(output_file_path, index=False)
                logger.info(f"Updated file saved to: {output_file_path}")
            except Exception as e:
                logger.error(f"Failed to save updated file: {e}")
                return False

        # Upload output file to cloud storage
        if self.cloud_helper and os.path.exists(output_file_path):
            try:
                upload_url = self.cloud_helper.upload_output_file(output_file_path)
                logger.info(f"✅ Output file uploaded successfully to: {upload_url}")
            except Exception as e:
                logger.error(f"Failed to upload output file: {e}", exc_info=True)

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
        """Download PDF from URL."""
        try:
            logger.debug(f"Downloading PDF from: {url}")
            os.makedirs(DOWNLOAD_DIR, exist_ok=True)
            parsed_url = urlparse(url)
            filename = os.path.basename(parsed_url.path)
            if not filename or '.' not in filename:
                filename = f"invoice_{row_num}.pdf"
            local_path = os.path.join(DOWNLOAD_DIR, filename)

            # Download file with retry logic
            max_retries = 2
            for attempt in range(max_retries):
                try:
                    response = requests.get(url, timeout=30, stream=True)
                    response.raise_for_status()

                    with open(local_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)

                    # Verify file was downloaded
                    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                        logger.info(f"Downloaded PDF: {url} → {local_path} ({os.path.getsize(local_path)} bytes)")
                        return local_path
                    else:
                        logger.warning(f"Downloaded file is empty or missing: {local_path}")
                        return None

                except requests.exceptions.RequestException as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Download attempt {attempt + 1} failed for {url}: {e}. Retrying...")
                        time.sleep(2 ** attempt)
                    else:
                        logger.error(f"All download attempts failed for {url}: {e}")
                        return None

        except Exception as e:
            logger.error(f"Failed to download PDF from {url}: {e}", exc_info=True)
            return None
