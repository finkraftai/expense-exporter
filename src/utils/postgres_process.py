import psycopg2
from .config import DB_CONFIG
from .logger import logger

class PostgresProcess:
    """Class for handling PostgreSQL database operations."""
    
    @staticmethod
    def get_db_connection():
        """
        Establish a connection to the PostgreSQL database.
        """
        logger.debug("Establishing PostgreSQL database connection")
        return psycopg2.connect(**DB_CONFIG)
    
    @staticmethod
    def insert_file_metadata(file_url, source, client_name, file_hash):
        """
        Insert file metadata into the hotel_upload table.
        Checks for duplicates based on file_hash and updates updated_on if duplicate.

        Returns:
            dict or None: {"id": str, "is_duplicate": bool} on success, None on failure.
        """
        try:
            with PostgresProcess.get_db_connection() as conn:
                with conn.cursor() as cur:
                    logger.debug(f"Checking if hash exists: {file_hash}")
                    cur.execute("SELECT id FROM hotel_invoice WHERE file_hash = %s", (file_hash,))
                    existing = cur.fetchone()

                    if existing:
                        record_id = existing[0]
                        # Update updated_on for duplicate file
                        cur.execute("""
                            UPDATE hotel_invoice
                            SET updated_on = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """, (record_id,))
                        conn.commit()
                        logger.info(f"Duplicate file detected. Refreshed updated_on for record {record_id}")
                        return {"id": str(record_id), "is_duplicate": True}

                    logger.debug(f"Inserting new record for {file_url}")
                    cur.execute("""
                        INSERT INTO hotel_invoice (file_url, source, client_name, file_hash, status, updated_on)
                        VALUES (%s, %s, %s, %s, 'PENDING', CURRENT_TIMESTAMP)
                        RETURNING id
                    """, (file_url, source, client_name, file_hash))
                    conn.commit()
                    record_id = cur.fetchone()[0]
                    logger.info(f"Inserted new record {record_id} for {file_url}")
                    return {"id": str(record_id), "is_duplicate": False}
        except Exception as e:
            logger.error(f"PostgreSQL insert failed for {file_url}: {e}", exc_info=True)
            return None         
        finally:
            conn.close()