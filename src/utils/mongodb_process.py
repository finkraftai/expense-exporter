from pymongo import MongoClient
from pymongo.errors import PyMongoError
from .logger import logger
from .config import MONGO_COLLECTION_NAME,MONGO_DB_NAME,MONGO_URI

class MongoDBProcess:
    """Class for handling MongoDB database operations"""
    def __init__(self):
        """
        Initialize MongoDB client and database connection.
        """
        try:
            self.client = MongoClient(MONGO_URI)
            self.db = self.client[MONGO_DB_NAME]
            self.collection = self.db[MONGO_COLLECTION_NAME]
            logger.info(f"Connected to MongoDB: {MONGO_DB_NAME}-{MONGO_COLLECTION_NAME}-{MONGO_URI}")
        except PyMongoError as e:
            logger.error(f"Failed to connect to MongoDB: {e}", exc_info=True)
            
    def insert_invoice_data(self, invoice_data):
        """
        Insert processed invoice data into MongoDB.
        """
        try:
            result = self.collection.insert_one(invoice_data)
            logger.info(f"Inserted invoice data into MongoDB with ID: {result.inserted_id}")
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Failed to insert invoice data into MongoDB: {e}", exc_info=True)
            return None
        
    def close_connection(self):
        """
        Close the MongoDB client connection.
        """
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
    
            
    def __enter__(self):
        return self

    def __exit__(self):
        self.close_connection()
