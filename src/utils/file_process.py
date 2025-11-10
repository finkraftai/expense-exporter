import os
import sys
import time
import hashlib
import pandas as pd
import requests
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright
from .postgres_process import PostgresProcess
from .mongodb_process import MongoDBProcess
from .logger import logger
from .cloud_helper import CloudHelperFactory
from .config import CLIENT, SOURCE, DOWNLOAD_DIR,S3_UPLOAD_PREFIX

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

        # Assume 'External File Links' column exists in the input file with codes or URLs
        if 'External File Links' not in df.columns:
            logger.error("Required column 'External File Links' not found in input file. Please ensure the input file contains this column with PDF download codes or URLs.")
            return False
        # logger.info("Using 'External File Links' column from input file for PDF downloads")

# Step 1: Handle multiple links per row (using External File Links)
        expanded_rows = []
        for idx, row in df.iterrows():
            if 'External File Links' not in row or pd.isna(row['External File Links']):
                expanded_rows.append(row)
                continue

            links_str = str(row['External File Links']).strip()
            links = [link.strip() for link in links_str.replace(';', ',').replace('|', ',').split(',') if link.strip()]

            if len(links) <= 1:
                expanded_rows.append(row)
            else:
                logger.info(f"Row {idx + 1}: Found {len(links)} links, duplicating row")
                for link in links:
                    new_row = row.copy()
                    new_row['External File Links'] = link
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
                logger.info(f"Processing row {idx + 1}/{len(df)}")

                if 'External File Links' not in row or pd.isna(row['External File Links']):
                    logger.warning(f"Row {idx + 1}: Missing External File Links")
                    df.at[idx, 'status'] = "FAILED: Missing External File Links"
                    failed_count += 1
                    continue

                link = str(row['External File Links']).strip()
                if link.startswith('https://wormhole.app/'):
                    file_url = link
                else:
                    file_url = 'https://wormhole.app/' + link

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
                s3_key = f"{S3_UPLOAD_PREFIX}/file_{idx+1}.pdf"

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

                # Step 2d: Check for duplicates in MongoDB before inserting
                with MongoDBProcess() as mongo_helper:
                    if mongo_helper.check_duplicate_by_hash(file_hash):
                        logger.info(f"✓ [{idx + 1}/{len(df)}] Duplicate file detected in MongoDB (Hash: {file_hash[:8]}...), skipping insert")
                        df.at[idx, 'status'] = "DUPLICATE: File already processed"
                        failed_count += 1
                        continue

                    # Prepare MongoDB data (all Excel columns + new fields)
                    mongo_data = row.to_dict()
                    mongo_data.update({
                        'file_link': file_url,
                        's3_link': s3_link,
                        'status': 'SUCCESS',
                        'file_hash': file_hash,
                        'corp_name': CLIENT,
                        'processed_at': pd.Timestamp.now(),
                        'source': SOURCE,
                        'client_name': CLIENT
                    })

                    # Insert into MongoDB
                    mongo_id = mongo_helper.insert_invoice_data(mongo_data)
                    if mongo_id:
                        logger.info(f"✓ [{idx + 1}/{len(df)}] MongoDB insert successful (ID: {mongo_id})")
                        source_id = str(mongo_id)
                    else:
                        logger.warning(f"Row {idx + 1}: MongoDB insert failed")
                        df.at[idx, 'status'] = "FAILED: MongoDB insert failed"
                        failed_count += 1
                        continue

                # Step 2e: Insert metadata to PostgreSQL hotel_invoice table
                pg_data = {
                    'source': 'tmc-portal',
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
                    'Hotel GST NO': 'hotel_gstin'
                    # 'CLIENT_GST_NO': 'client_gstin',
                    # 'Q2T_INVOICE_NO': 'invoice_number',
                    # 'HOTEL_INVOICE_DATE': 'invoice_date',
                    # 'TOTAL INVOICE AMOUNT': 'gst_amount',
                    # 'BOOKING_ID': 'booking_id'
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
        """Download PDF from URL using Selenium for sites requiring button clicks."""
        try:
            # logger.debug(f"Downloading PDF from: {url}")
            os.makedirs(DOWNLOAD_DIR, exist_ok=True)
            parsed_url = urlparse(url)
            filename = os.path.basename(parsed_url.path)
            if not filename or '.' not in filename:
                filename = f"invoice_{row_num}.pdf"
            local_path = os.path.join(DOWNLOAD_DIR, filename)

            # Check if URL is from wormhole.app or similar sites requiring interaction
            if 'wormhole.app' in url.lower():
                # logger.info(f"Detected wormhole.app URL, using Playwright for download: {url}")
                return self._download_with_playwright(url, local_path, row_num)
            else:
                # Use requests for direct downloads
                return self._download_with_requests(url, local_path, row_num)

        except Exception as e:
            logger.error(f"Failed to download PDF from {url}: {e}", exc_info=True)
            return None

    def _download_with_requests(self, url, local_path, row_num):
        """Download PDF using requests for direct URLs."""
        max_retries = 1
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

    def _download_with_playwright(self, url, local_path, row_num):
        """Download PDF using Playwright for sites requiring button clicks."""
        try:
            # logger.debug(f"Setting up Playwright for URL: {url}")
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(accept_downloads=True)
                page = context.new_page()
                page.goto(url, timeout=60000)
                # logger.debug(f"Navigated to {url}")

                # Wait for page to load and try multiple selectors for download button
                page.wait_for_load_state('networkidle', timeout=30000)

                # Try multiple possible selectors for download button
                selectors = [
                    "button[type='button'].css-bbqjr1",
                    "button:has-text('Download')",
                    "a:has-text('Download')",
                    "button[class*='download']",
                    "a[class*='download']",
                    "[data-testid*='download']",
                    "button",
                    "a"
                ]

                download_button = None
                for selector in selectors:
                    try:
                        download_button = page.wait_for_selector(selector, timeout=5000)
                        if download_button:
                            # logger.debug(f"Found download element with selector: {selector}")
                            break
                    except:
                        continue

                if not download_button:
                    logger.error(f"No download button found on page {url}. Page title: {page.title()}")
                    # Log some page content for debugging
                    # body_text = page.locator('body').text_content()
                    # logger.debug(f"Page body text (first 500 chars): {body_text[:500]}")
                    return None

                # logger.debug("Found download button, clicking...")
                with page.expect_download(timeout=60000) as download_info:  # Increased timeout
                    download_button.click()
                download = download_info.value
                download.save_as(local_path)
                logger.info(f"Downloaded PDF via Playwright: {url} → {local_path}")
                browser.close()
                return local_path
        except Exception as e:
            logger.error(f"Playwright download failed for {url}: {e}", exc_info=True)
            return None
