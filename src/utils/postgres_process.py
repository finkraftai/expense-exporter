import psycopg2
from .config import DB_CONFIG
from .logger import logger

class PostgresProcess:
    """Handles PostgreSQL database operations for invoice data."""

    @staticmethod
    def get_db_connection():
        """Establish a connection to the PostgreSQL database."""
        logger.debug("Establishing PostgreSQL database connection")
        return psycopg2.connect(**DB_CONFIG)

    @staticmethod
    def insert_file_metadata(file_url, source, client_name, file_hash):
        """Insert basic file metadata and update duplicates."""
        try:
            with PostgresProcess.get_db_connection() as conn:
                with conn.cursor() as cur:
                    logger.debug(f"Checking if hash exists: {file_hash}")
                    cur.execute("SELECT id FROM hotel_uploads WHERE file_hash = %s", (file_hash,))
                    existing = cur.fetchone()

                    if existing:
                        record_id = existing[0]
                        cur.execute("""
                            UPDATE hotel_uploads
                            SET updated_on = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """, (record_id,))
                        conn.commit()
                        logger.info(f"Duplicate file detected. Refreshed updated_on for record {record_id}")
                        return {"id": str(record_id), "is_duplicate": True}

                    cur.execute("""
                        INSERT INTO hotel_uploads (file_url, source, client_name, file_hash, status, updated_on)
                        VALUES (%s, %s, %s, %s, 'PENDING', CURRENT_TIMESTAMP)
                        RETURNING id
                    """, (file_url, source, client_name, file_hash))
                    record_id = cur.fetchone()[0]
                    conn.commit()
                    logger.info(f"Inserted new record {record_id} for {file_url}")
                    return {"id": str(record_id), "is_duplicate": False}
        except Exception as e:
            logger.error(f"PostgreSQL insert failed for {file_url}: {e}", exc_info=True)
            return None

    @staticmethod
    def insert_full_invoice_data(invoice_data):
        """Insert or update a full invoice record into the hotel_uploads table."""
        try:
            with PostgresProcess.get_db_connection() as conn:
                with conn.cursor() as cur:
                    file_hash = invoice_data.get('file_hash')
                    logger.debug(f"Checking if hash exists: {file_hash}")
                    cur.execute("SELECT id FROM hotel_uploads WHERE file_hash = %s", (file_hash,))
                    existing = cur.fetchone()

                    if existing:
                        record_id = existing[0]
                        # Explicit update to maintain compatibility across DBs
                        cur.execute("""
                            UPDATE hotel_uploads
                            SET updated_on = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """, (record_id,))
                        conn.commit()
                        logger.info(f"Duplicate file detected. Refreshed updated_on for record {record_id}")
                        return {"id": str(record_id), "is_duplicate": True}

                    # Build insert dynamically
                    fields, values, placeholders = [], [], []
                    for field in [
                        'source_id', 'source', 'client_name', 'file_url', 'file_hash', 'status',
                        'match_status', '2b_id', 'booking_id', 'client_gstin', 'hotel_gstin',
                        'invoice_number', 'invoice_date', 'gst_amount', 'remarks', 'followup_tracking_id', 'updated_on'
                    ]:
                        if field in invoice_data and invoice_data[field] is not None:
                            fields.append(f'"{field}"')
                            values.append(invoice_data[field])
                            placeholders.append('%s')

                    query = f"""
                        INSERT INTO hotel_uploads ({', '.join(fields)})
                        VALUES ({', '.join(placeholders)})
                        RETURNING id
                    """

                    cur.execute(query, values)
                    record_id = cur.fetchone()[0]
                    conn.commit()
                    logger.info(f"Inserted new invoice record {record_id}")
                    return {"id": str(record_id), "is_duplicate": False}
        except Exception as e:
            logger.error(f"PostgreSQL full insert failed: {e}", exc_info=True)
            return None































# import psycopg2
# from .config import DB_CONFIG
# from .logger import logger

# class PostgresProcess:
#     """Class for handling PostgreSQL database operations."""
    
#     @staticmethod
#     def get_db_connection():
#         """
#         Establish a connection to the PostgreSQL database.
#         """
#         logger.debug("Establishing PostgreSQL database connection")
#         return psycopg2.connect(**DB_CONFIG)
    
#     @staticmethod
#     def insert_file_metadata(file_url, source, client_name, file_hash):
#         """
#         Insert file metadata into the hotel_invoice table.
#         Checks for duplicates based on file_hash and updates updated_on if duplicate.

#         Returns:
#             dict or None: {"id": str, "is_duplicate": bool} on success, None on failure.
#         """
#         try:
#             with PostgresProcess.get_db_connection() as conn:
#                 with conn.cursor() as cur:
#                     logger.debug(f"Checking if hash exists: {file_hash}")
#                     cur.execute("SELECT id FROM hotel_invoice WHERE file_hash = %s", (file_hash,))
#                     existing = cur.fetchone()

#                     if existing:
#                         record_id = existing[0]
#                         # Update updated_on for duplicate file
#                         cur.execute("""
#                             UPDATE hotel_invoice
#                             SET updated_on = CURRENT_TIMESTAMP
#                             WHERE id = %s
#                         """, (record_id,))
#                         conn.commit()
#                         logger.info(f"Duplicate file detected. Refreshed updated_on for record {record_id}")
#                         return {"id": str(record_id), "is_duplicate": True}

#                     logger.debug(f"Inserting new record for {file_url}")
#                     cur.execute("""
#                         INSERT INTO hotel_invoice (file_url, source, client_name, file_hash, status, updated_on)
#                         VALUES (%s, %s, %s, %s, 'PENDING', CURRENT_TIMESTAMP)
#                         RETURNING id
#                     """, (file_url, source, client_name, file_hash))
#                     conn.commit()
#                     record_id = cur.fetchone()[0]
#                     logger.info(f"Inserted new record {record_id} for {file_url}")
#                     return {"id": str(record_id), "is_duplicate": False}
#         except Exception as e:
#             logger.error(f"PostgreSQL insert failed for {file_url}: {e}", exc_info=True)
#             return None

#     @staticmethod
#     def insert_full_invoice_data(invoice_data):
#         """
#         Insert full invoice data into the hotel_invoice table.
#         Checks for duplicates based on file_hash and updates updated_on if duplicate.

#         Args:
#             invoice_data (dict): Invoice data to insert.

#         Returns:
#             dict or None: {"id": str, "is_duplicate": bool} on success, None on failure.
#         """
#         try:
#             with PostgresProcess.get_db_connection() as conn:
#                 with conn.cursor() as cur:
#                     file_hash = invoice_data.get('file_hash')
#                     logger.debug(f"Checking if hash exists: {file_hash}")
#                     cur.execute("SELECT id FROM hotel_invoice WHERE file_hash = %s", (file_hash,))
#                     existing = cur.fetchone()

#                     if existing:
#                         record_id = existing[0]
#                         # The 'updated_on' field is now updated automatically by a database trigger.
#                         # We can simply log and return. If we needed to update other fields, we would do it here.
#                         cur.execute("""
#                             -- This query is just to ensure the trigger fires if we need to log an update.
#                             -- For now, we can just log that a duplicate was found.
#                             SELECT id FROM hotel_invoice WHERE id = %s
#                         """, (record_id,))
#                         conn.commit()
#                         logger.info(f"Duplicate file detected. Refreshed updated_on for record {record_id}")
#                         return {"id": str(record_id), "is_duplicate": True}

#                     logger.debug(f"Inserting new invoice record")

#                     # Build the INSERT query dynamically based on available fields
#                     fields = []
#                     values = []
#                     placeholders = []

#                     for field in ['source_id', 'source', 'client_name', 'file_url', 'file_hash', 'status',
#                                 'match_status', '2b_id', 'booking_id', 'client_gstin', 'hotel_gstin',
#                                 'invoice_number', 'invoice_date', 'gst_amount', 'remarks', 'followup_tracking_id']:
#                         if field in invoice_data and invoice_data[field] is not None:
#                             fields.append(f'"{field}"')  # Quote field names to handle special characters/names
#                             values.append(invoice_data[field])
#                             placeholders.append('%s')

#                     query = f"""
#                         INSERT INTO hotel_invoice ({', '.join(fields)}, "updated_on")
#                         VALUES ({', '.join(placeholders)}, CURRENT_TIMESTAMP)
#                         RETURNING id
#                     """

#                     cur.execute(query, values)
#                     conn.commit()
#                     record_id = cur.fetchone()[0]
#                     logger.info(f"Inserted new invoice record {record_id}")
#                     return {"id": str(record_id), "is_duplicate": False}
#         except Exception as e:
#             logger.error(f"PostgreSQL full insert failed: {e}", exc_info=True)
#             return None
