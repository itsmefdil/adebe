import os
import shutil
from abc import ABC, abstractmethod
from typing import BinaryIO
from ftplib import FTP
import boto3
from botocore.exceptions import NoCredentialsError
from fastapi import UploadFile

class StorageBackend(ABC):
    @abstractmethod
    def upload(self, file_path: str, filename: str) -> str:
        """Upload a file and return its identifier/path."""
        pass

    @abstractmethod
    def download(self, filename: str, destination: str):
        """Download a file to the destination path."""
        pass
    
    @abstractmethod
    def list_backups(self) -> list:
        """List available backups."""
        pass
    
    @abstractmethod
    def delete(self, filename: str):
        """Delete a file."""
        pass

class LocalStorageBackend(StorageBackend):
    def __init__(self, base_dir: str = "app/data/backups"):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

    def upload(self, file_path: str, filename: str) -> str:
        destination = os.path.join(self.base_dir, filename)
        shutil.copy2(file_path, destination)
        return filename

    def download(self, filename: str, destination: str):
        source = os.path.join(self.base_dir, filename)
        if not os.path.exists(source):
            raise FileNotFoundError(f"Backup {filename} not found.")
        shutil.copy2(source, destination)

    def list_backups(self) -> list:
        if not os.path.exists(self.base_dir):
            return []
        return sorted(os.listdir(self.base_dir), reverse=True)

    def delete(self, filename: str):
        path = os.path.join(self.base_dir, filename)
        if os.path.exists(path):
            os.remove(path)

class S3StorageBackend(StorageBackend):
    def __init__(self):
        self.bucket = os.getenv("S3_BUCKET")
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
            endpoint_url=os.getenv("S3_ENDPOINT_URL") # For Minio
        )

    def upload(self, file_path: str, filename: str) -> str:
        try:
            self.s3.upload_file(file_path, self.bucket, filename)
            return filename
        except NoCredentialsError:
            raise Exception("S3 Credentials not available")

    def download(self, filename: str, destination: str):
        self.s3.download_file(self.bucket, filename, destination)

    def list_backups(self) -> list:
        response = self.s3.list_objects_v2(Bucket=self.bucket)
        if 'Contents' in response:
            return sorted([obj['Key'] for obj in response['Contents']], reverse=True)
        return []

    def delete(self, filename: str):
        self.s3.delete_object(Bucket=self.bucket, Key=filename)

class FTPStorageBackend(StorageBackend):
    def __init__(self):
        self.host = os.getenv("FTP_HOST")
        self.user = os.getenv("FTP_USER")
        self.password = os.getenv("FTP_PASSWORD")
        self.dir = os.getenv("FTP_DIR", "/")

    def _get_connection(self):
        ftp = FTP(self.host)
        ftp.login(self.user, self.password)
        ftp.cwd(self.dir)
        return ftp

    def upload(self, file_path: str, filename: str) -> str:
        ftp = self._get_connection()
        with open(file_path, 'rb') as f:
            ftp.storbinary(f"STOR {filename}", f)
        ftp.quit()
        return filename

    def download(self, filename: str, destination: str):
        ftp = self._get_connection()
        with open(destination, 'wb') as f:
            ftp.retrbinary(f"RETR {filename}", f.write)
        ftp.quit()

    def list_backups(self) -> list:
        ftp = self._get_connection()
        files = ftp.nlst()
        ftp.quit()
        return sorted(files, reverse=True)

    def delete(self, filename: str):
        ftp = self._get_connection()
        ftp.delete(filename)
        ftp.quit()

def get_storage_backend() -> StorageBackend:
    storage_type = os.getenv("STORAGE_TYPE", "LOCAL").upper()
    if storage_type == "S3":
        return S3StorageBackend()
    elif storage_type == "FTP":
        return FTPStorageBackend()
    else:
        return LocalStorageBackend()
