""" MongoDB Process Module
-Handles all MongoDB database operations for the hotel expense exporter.
-Provides functions for connecting to MongoDB, inserting invoice data, and closing connections safely.
"""

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from .config import MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION_NAME
from .logger import logger

class MongoDBProcess:
    """Class for handling MongoDB database operations."""

    def __init__(self):
        """Initialize MongoDB client and database connection."""
        try:
            self.client = MongoClient(MONGO_URI) 
            self.db = self.client[MONGO_DB_NAME]
            self.collection = self.db[MONGO_COLLECTION_NAME]
            # ✅ Avoid logging URI (security best practice)
            logger.info(f"Connected to MongoDB: {MONGO_DB_NAME}.{MONGO_COLLECTION_NAME}")
        except PyMongoError as e:
            logger.error(f"Failed to connect to MongoDB: {e}", exc_info=True)
            # ✅ Raise the error to prevent silent failures
            raise

    def insert_invoice_data(self, invoice_data):
        """ Insert processed invoice data into MongoDB."""
        try:
            result = self.collection.insert_one(invoice_data)
            logger.info(f"Inserted invoice data into MongoDB with ID: {result.inserted_id}")
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Failed to insert invoice data into MongoDB: {e}", exc_info=True)
            return None

    def check_duplicate_by_hash(self, file_hash):
        """ Check if a document with the given file_hash already exists in MongoDB."""
        try:
            existing = self.collection.find_one({"file_hash": file_hash})
            return existing is not None
        except PyMongoError as e:
            logger.error(f"Failed to check for duplicate in MongoDB: {e}", exc_info=True)
            return False

    def close_connection(self):
        """Close the MongoDB client connection."""
        if hasattr(self, "client") and self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

    def __enter__(self):
        """Support use with 'with' context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure connection closure when leaving 'with' context."""
        self.close_connection()





















# from pymongo import MongoClient
# from pymongo.errors import PyMongoError
# from .logger import logger
# from .config import MONGO_COLLECTION_NAME,MONGO_DB_NAME,MONGO_URI

# class MongoDBProcess:
#     """Class for handling MongoDB database operations"""
#     def __init__(self):
#         """
#         Initialize MongoDB client and database connection.
#         """
#         try:
#             self.client = MongoClient(MONGO_URI)
#             self.db = self.client[MONGO_DB_NAME]
#             self.collection = self.db[MONGO_COLLECTION_NAME]
#             logger.info(f"Connected to MongoDB: {MONGO_DB_NAME}-{MONGO_COLLECTION_NAME}-{MONGO_URI}")
#         except PyMongoError as e:
#             logger.error(f"Failed to connect to MongoDB: {e}", exc_info=True)
            
#     def insert_invoice_data(self, invoice_data):
#         """
#         Insert processed invoice data into MongoDB.
#         """
#         try:
#             result = self.collection.insert_one(invoice_data)
#             logger.info(f"Inserted invoice data into MongoDB with ID: {result.inserted_id}")
#             return str(result.inserted_id)
#         except PyMongoError as e:
#             logger.error(f"Failed to insert invoice data into MongoDB: {e}", exc_info=True)
#             return None
        
#     def close_connection(self):
#         """
#         Close the MongoDB client connection.
#         """
#         if self.client:
#             self.client.close()
#             logger.info("MongoDB connection closed")
    
            
#     def __enter__(self):
#         return self

#     def __exit__(self):
#         self.close_connection()
