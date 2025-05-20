"""MinIO helper."""
import io
from pathlib import Path

import requests
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()


BUCKET_SENSOR_COMMUNITY = 'sensor-community'


def get_minio_client() -> Minio:
    return Minio(
        os.getenv('MINIO_ENDPOINT'),
        access_key=os.getenv('MINIO_ACCESS_KEY'),
        secret_key=os.getenv('MINIO_SECRET_KEY'),
        secure=False  # Set to False if not using HTTPS
    )


def load_file_to_bucket(file_url: str, file_path: Path, bucket_name: str) -> None:
    # Download the file
    response = requests.get(file_url)
    response.raise_for_status()  # Check if the request was successful

    # Create an in-memory buffer
    file_buffer = io.BytesIO(response.content)

    # MinIO client configuration
    minio_client = get_minio_client()

    # Upload the file to MinIO
    try:
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=file_path.as_posix(),
            data=file_buffer,
            length=len(response.content),
            content_type='application/csv'
        )
        # print(f"'{file_path}' is successfully uploaded to bucket 'your-bucket-name'.")
    except S3Error as e:
        print(f"Error occurred: {e}")

def check_file_exist(bucket_name: str, file_path: Path) -> bool:
    minio_client = get_minio_client()
    try:
        minio_client.stat_object(bucket_name, file_path.as_posix())
        return True
    except S3Error:
        return False

def get_file_per_bucket(bucket_name: str, prefix: str) -> list[str]:
    minio_client = get_minio_client()
    try:
        objects = minio_client.list_objects(bucket_name, prefix=prefix)
        return [obj.object_name for obj in objects]
    except S3Error as e:
        print(f"Error occurred: {e}")

def get_files_per_date(bucket_name: str) -> dict[str, list[str]]:
    dates = get_file_per_bucket(bucket_name, '')
    files_per_date = {}
    for date in dates:
        files = get_file_per_bucket(bucket_name, date)
        files_per_date[date] = files
    return files_per_date

if __name__ == '__main__':
    ...