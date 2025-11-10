import os
from dotenv import load_dotenv

# === Project Root ===
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
dotenv_path = os.path.join(PROJECT_ROOT, ".env")
load_dotenv(dotenv_path)

# === Database Config ===
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}
if not DB_CONFIG["dbname"]:
    raise ValueError("DB_NAME is missing from environment variables.")

# === Cloud Provider Config ===
CLOUD_PROVIDER = os.getenv("CLOUD_PROVIDER", "aws").lower()
if not CLOUD_PROVIDER:
    raise ValueError("CLOUD_PROVIDER is not set in .env")

# === Azure Config ===
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
AZURE_PDF_PATH = os.getenv("AZURE_PDF_PATH")

# === AWS Config ===
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_PREFIX = os.getenv("AWS_PREFIX")

# === S3 Upload Config ===
S3_UPLOAD_BUCKET = os.getenv("S3_UPLOAD_BUCKET", AWS_BUCKET_NAME)
CLIENT_FOLDER = os.getenv("CLIENT")
S3_UPLOAD_PREFIX = f"tmc-portal/{CLIENT_FOLDER}"

# === CSV/Excel File Paths ===
input_file = os.getenv("INPUT_FILE_PATH")
output_file = os.getenv("OUTPUT_FILE_PATH")

INPUT_FILE_PATH = os.path.join(PROJECT_ROOT, input_file) if input_file else None
OUTPUT_FILE_PATH = os.path.join(PROJECT_ROOT, output_file) if output_file else None

# === MongoDB Config ===
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")

# === Directories ===
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "downloads")
LOG_DIR = os.getenv("LOG_DIR", "logs")

# === Client / Source ===
CLIENT = os.getenv("CLIENT")
SOURCE = os.getenv("SOURCE")





























# import os
# from dotenv import load_dotenv

# # Define the project root directory (which is two levels up from this config file's directory)
# # src/utils/config.py -> src/utils -> src -> project_root
# PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# dotenv_path = os.path.join(PROJECT_ROOT, ".env")
# load_dotenv(dotenv_path)

# # Database Config
# DB_CONFIG = {
#     "dbname": os.getenv("DB_NAME"),
#     "user": os.getenv("DB_USER"),
#     "password": os.getenv("DB_PASSWORD"),
#     "host": os.getenv("DB_HOST"),
#     "port": os.getenv("DB_PORT"),
# }

# # Cloud Provider Config
# # Provide a default value (e.g., 'aws') and handle the case where the variable might be missing.
# cloud_provider_env = os.getenv("CLOUD_PROVIDER", "aws")
# if not cloud_provider_env:
#     raise ValueError("CLOUD_PROVIDER environment variable is not set. Please check your .env file.")
# CLOUD_PROVIDER = cloud_provider_env.lower()

# # Azure Config
# AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
# AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
# AZURE_PDF_PATH = os.getenv("AZURE_PDF_PATH")

# # AWS Config
# AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
# AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
# AWS_REGION = os.getenv("AWS_REGION")
# AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
# AWS_PREFIX = os.getenv("AWS_PREFIX")

# # S3 Upload Config
# CLIENT_FOLDER = os.getenv("CLIENT")
# S3_UPLOAD_PREFIX = f"tmc-portal/{CLIENT_FOLDER}"

# # CSV/Excel Processing Config
# input_file = os.getenv("INPUT_FILE_PATH")
# output_file = os.getenv("OUTPUT_FILE_PATH")

# # Construct absolute paths relative to the project root
# INPUT_FILE_PATH = os.path.join(PROJECT_ROOT, input_file) if input_file else None
# OUTPUT_FILE_PATH = os.path.join(PROJECT_ROOT, output_file) if output_file else None

# # MongoDB
# MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
# MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
# MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")

# #Downloads
# DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "downloads")
# LOG_DIR = os.getenv("LOG_DIR", "logs")

# #Client and Source
# CLIENT = os.getenv("CLIENT")
# SOURCE = os.getenv("SOURCE")
