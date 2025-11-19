import os
import re
import boto3
from abc import ABC, abstractmethod
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError
from botocore.exceptions import BotoCoreError, ClientError
from .config import CLOUD_PROVIDER
from .config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_BUCKET_NAME,S3_UPLOAD_PREFIX
from .config import AZURE_CONNECTION_STRING, AZURE_CONTAINER_NAME, AZURE_PDF_PATH
from .logger import logger

class CloudHelper(ABC):
    """Abstract base class for cloud storage helpers-Defines the interface for uploading blobs and generating file URLs."""

    @abstractmethod
    def upload_blob(self, local_path, blob_name):
        """Upload a file to cloud storage."""
        pass

    @abstractmethod
    def upload_output_file(self, output_file_path):
        """Upload a processed output file and return its public URL."""
        pass

    @abstractmethod
    def get_file_url(self, blob_name):
        """Generate the public URL for a blob/object."""
        pass

class CloudHelperFactory:
    """Factory class to create the appropriate CloudHelper instance based on CLOUD_PROVIDER."""

    @staticmethod
    def create():
        """
        Create a CloudHelper instance.
        Returns:
            CloudHelper: Instance of AzureHelper or AwsHelper.
        """
        if CLOUD_PROVIDER == "azure":
            return AzureHelper()
        elif CLOUD_PROVIDER == "aws":
            return AwsHelper()
        else:
            raise ValueError(f"Unsupported CLOUD_PROVIDER: {CLOUD_PROVIDER}")
        
class AwsHelper(CloudHelper):
    """Helper class for AWS S3 operations."""
    def __init__(self):
        """Initialize the S3 client with credentials and bucket info."""
        s3_config = {
            'aws_access_key_id': AWS_ACCESS_KEY_ID,
            'aws_secret_access_key': AWS_SECRET_ACCESS_KEY,
            'region_name': AWS_REGION
        }

        # self.s3_client = boto3.client('s3', **s3_config)
        # Use regional endpoint for S3
        endpoint_url = f"https://s3.{AWS_REGION}.amazonaws.com"
        self.s3_client = boto3.client('s3', endpoint_url=endpoint_url, **s3_config)
        self.bucket_name = AWS_BUCKET_NAME

    def upload_blob(self, local_path, s3_key):
        """Upload a file to AWS S3 and verify upload success."""
        try:
            logger.debug(f"Uploading to S3: {local_path} → s3://{self.bucket_name}/{s3_key}")

            # Verify file existence before upload
            if not os.path.exists(local_path):
                logger.error(f"Local file does not exist: {local_path}")
                return False

            # Perform upload
            self.s3_client.upload_file(local_path, self.bucket_name, s3_key)

            # Verify upload
            try:
                self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
                logger.info(f"Uploaded successfully: s3://{self.bucket_name}/{s3_key}")
                return True
            except Exception as verify_err:
                logger.error(f"Upload verification failed for s3://{self.bucket_name}/{s3_key}: {verify_err}")
                return False

        except (BotoCoreError, ClientError, Exception) as e:
            logger.error(f"Failed to upload {local_path} to S3: {e}", exc_info=True)
            return False

    def generate_presigned_url(self, s3_key, expiry_seconds=518400):
        """
        Generate a pre-signed S3 URL (valid for default 6 days).
        Returns:
            str: Pre-signed URL or fallback URL.
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': s3_key},
                ExpiresIn=expiry_seconds
            )
            logger.debug(f"Generated pre-signed URL for {s3_key}")
            return url
        except Exception as e:
            logger.warning(f"Failed to generate pre-signed URL for {s3_key}: {e}")
            fallback_url = f"https://{self.bucket_name}.s3.amazonaws.com/{s3_key}"
            logger.warning(f"Using fallback URL: {fallback_url}")
            return fallback_url

    def get_file_url(self, blob_name):
        """Generate a pre-signed S3 URL for the object."""
        return self.generate_presigned_url(blob_name)

    def upload_output_file(self, output_file_path):
        """Upload processed output file (Excel/CSV) to S3, verify upload, and return URL."""
        try:
            if not os.path.exists(output_file_path):
                logger.warning(f"Output file does not exist: {output_file_path}")
                return None

            file_name = os.path.basename(output_file_path)
            s3_key = f"{S3_UPLOAD_PREFIX}/processed/{file_name}"

            logger.debug(f"Uploading output file to S3: {output_file_path} → s3://{self.bucket_name}/{s3_key}")

            self.s3_client.upload_file(output_file_path, self.bucket_name, s3_key)

            # Verify upload
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            public_url = f"https://{self.bucket_name}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
            logger.info(f"✅ Uploaded processed file to AWS S3: {public_url}")
            return public_url

        except (BotoCoreError, ClientError, Exception) as e:
            logger.error(f"❌ Failed to upload output file {output_file_path}: {e}", exc_info=True)
            return None

class AzureHelper(CloudHelper):
    """ Helper class for Azure Blob Storage operations. """

    def __init__(self):
        """Initialize the BlobServiceClient and container info."""
        self.blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        self.container_name = AZURE_CONTAINER_NAME
        self.pdf_path = AZURE_PDF_PATH

    def _get_account_name(self):
        """Extract account name from connection string."""
        match = re.search(r"AccountName=([^;]+)", AZURE_CONNECTION_STRING)
        return match.group(1) if match else None
    
    def upload_blob(self, local_path, blob_name):
        """Upload a file to Azure Blob Storage."""
        try:
            logger.debug(f"Uploading file: {local_path} → {blob_name}")
            blob_client = self.blob_service.get_blob_client(self.container_name, blob_name)
            with open(local_path, "rb") as f:
                blob_client.upload_blob(f, overwrite=True)
            logger.info(f"Uploaded file: {local_path} → {blob_name}")
            return True
        except AzureError as e:
            logger.error(f"Error uploading file {local_path} to {blob_name}: {e}", exc_info=True)
            return False

    def get_file_url(self, blob_name):
        """Generate Azure Blob URL for the blob."""
        account_name = self._get_account_name()
        return f"https://{account_name}.blob.core.windows.net/{self.container_name}/{blob_name}"

    def upload_output_file(self, output_file_path):
        """Upload processed output file (Excel/CSV) to Azure, verify upload, and return URL."""
        try:
            if not os.path.exists(output_file_path):
                logger.warning(f"Output file does not exist: {output_file_path}")
                return None

            file_name = os.path.basename(output_file_path)
            upload_subpath = f"processed/{file_name}"
            blob_name = f"{self.pdf_path}/{upload_subpath}".strip("/")

            logger.debug(f"Uploading output file to Azure: {output_file_path} → {blob_name}")

            blob_client = self.blob_service.get_blob_client(self.container_name, blob_name)
            with open(output_file_path, "rb") as data:
                blob_client.upload_blob(data=data, overwrite=True)

            public_url = blob_client.url
            logger.info(f"✅ Uploaded processed file to Azure Blob: {public_url}")
            return public_url

        except (AzureError, Exception) as e:
            logger.error(f"❌ Failed to upload output file {output_file_path}: {e}", exc_info=True)
            return None




















    # def upload_output_file(self, output_file_path):
    #     """ Upload processed output file (Excel/CSV) to S3 and verify upload. """
    #     try:
    #         if not os.path.exists(output_file_path):
    #             logger.warning(f"Output file does not exist: {output_file_path}")
    #             return False

    #         file_name = os.path.basename(output_file_path)
    #         s3_key = f"{S3_UPLOAD_PREFIX}/processed/{file_name}"

    #         logger.debug(f"Uploading output file to S3: {output_file_path} → s3://{self.bucket_name}/{s3_key}")

    #         # Upload
    #         self.s3_client.upload_file(output_file_path, self.bucket_name, s3_key)

    #         # Verify
    #         try:
    #             self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
    #             logger.info(f"Uploaded processed file: s3://{self.bucket_name}/{s3_key}")
    #             return True
    #         except Exception as verify_err:
    #             logger.error(f"Verification failed for output file upload: {verify_err}")
    #             return False

    #     except Exception as e:
    #         logger.error(f"Failed to upload output file {output_file_path} to S3: {e}", exc_info=True)
    #         return False


        
# class AwsHelper(CloudHelper):
#     """Helper class for AWS S3 operations."""

#     def __init__(self):
#         """ Initialize the S3 client with credentials and bucket info. """
#         s3_config = {
#             'aws_access_key_id': AWS_ACCESS_KEY_ID,
#             'aws_secret_access_key': AWS_SECRET_ACCESS_KEY,
#             'region_name': AWS_REGION
#         }
        
#         self.s3_client = boto3.client('s3', **s3_config)
#         self.bucket_name = AWS_BUCKET_NAME

#     def upload_blob(self, local_path, key):
#         """ Upload a file to AWS S3. """
#         try:
#             logger.debug(f"Uploading file: {local_path} → s3://{self.bucket_name}/{key}")
#             self.s3_client.upload_file(local_path, self.bucket_name, key)
#             logger.info(f"Uploaded file: {local_path} → s3://{self.bucket_name}/{key}")
#             return True
#         except (BotoCoreError, ClientError) as e:
#             logger.error(f"Error uploading file {local_path} to S3: {e}", exc_info=True)
#             return False

#     def get_file_url(self, key):
#         """ Generate S3 URL for the object. """
#         return f"s3://{self.bucket_name}/{key}"
