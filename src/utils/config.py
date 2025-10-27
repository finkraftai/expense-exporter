import os
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(BASE_DIR, "..", ".env")
load_dotenv(dotenv_path)

# Database Config
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

# Cloud Provider Config
CLOUD_PROVIDER = os.getenv("CLOUD_PROVIDER").lower()

# Azure Config
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
AZURE_PDF_PATH = os.getenv("AZURE_PDF_PATH")

# AWS Config
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_PREFIX = os.getenv("AWS_PREFIX")

# S3 Upload Config
S3_UPLOAD_BUCKET = os.getenv("S3_UPLOAD_BUCKET")
CLIENT_FOLDER = os.getenv("CLIENT")
S3_UPLOAD_PREFIX = f"tmc-portal/{CLIENT_FOLDER}"

# CSV/Excel Processing Config
INPUT_FILE_PATH = os.getenv("INPUT_FILE_PATH")
OUTPUT_FILE_PATH = os.getenv("OUTPUT_FILE_PATH")

# MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")

#Downloads
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "downloads")
LOG_DIR = os.getenv("LOG_DIR", "logs")

#Client and Source
CLIENT = os.getenv("CLIENT")
SOURCE = os.getenv("SOURCE")