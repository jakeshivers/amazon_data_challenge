import os
import zipfile
from pathlib import Path
from io import BytesIO
import httpx
import pandas as pd
from orm.amazon_products import session_scope
from sqlalchemy import Table


def download_file(url: str, download_path: Path) -> None:
    """
    Args:
        url (str): URL to download the file from
        download_path (Path): Path to save the downloaded file to

    Something to download data from
        https://www.kaggle.com/api/v1/datasets/download/karkavelrajaj/amazon-sales-dataset

    If you have trouble with Kaggle, you might not have used it and have a key in ~/.kaggle/kaggle.json,
        if this is the case, just fill in how you might download the file and download it manually through
        the browser

    Raises:
        Exception: If the download fails (fine to be generic here)
    """
    try:
        with httpx.Client(follow_redirects=True) as client:
            response = client.get(url)
            response.raise_for_status()  # Raise an error for bad responses (4xx, 5xx)

            # Write the content to the file
            with open(download_path, "wb") as file:
                file.write(response.content)

        print(f"File downloaded to: {download_path}")
    except Exception as e:
        raise Exception(f"Failed to download file from {url}: {e}")

def read_file_from_zip(zip_path: Path, file_name: str) -> BytesIO:
    """
    Reads a specific file from a ZIP archive into a BytesIO object

    Args:
        zip_path (Path): Path to the ZIP file (e.g., '~/Downloads/amazon-sales-dataset.zip')
        file_name (str): Name of the file inside the ZIP to read (e.g., 'amazon_sales.csv')

    Returns:
        BytesIO: An in-memory binary stream containing the file's contents

    Raises:
        FileNotFoundError: If the ZIP file doesn't exist
        KeyError: If the specified file_name isn't found in the ZIP archive
    """
    zip_path = Path(zip_path).expanduser()  # Handle user home directory (~)

    if not zip_path.exists():
        raise FileNotFoundError(f"ZIP file not found: {zip_path}")

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        if file_name not in zip_ref.namelist():
            raise KeyError(f"File '{file_name}' not found in ZIP archive")
        
        # Read file into memory
        with zip_ref.open(file_name) as file:
            return BytesIO(file.read())  # Return as in-memory binary stream

def bulk_insert_df(df: pd.DataFrame, table: Table) -> None:
    with session_scope() as session:
        records = df_to_dict_list(df)  # Convert DataFrame to list of dicts
        session.bulk_insert_mappings(table, records)  # Bulk insert
        
        

def df_to_dict_list(df: pd.DataFrame):
    """
    Convert DataFrame to a list of dictionaries for bulk insertion.
    Handles NaN values and ensures datetime conversion.
    """
    df = df.replace({pd.NA: None})  # Replace Pandas NA values with Python None

    datetime_cols = df.select_dtypes(include=['datetime64[ns]']).columns

    records = df.to_dict(orient='records')  # Convert DataFrame rows to list of dicts

    for record in records:
        for col in datetime_cols:
            if col in record:
                record[col] = record[col].to_pydatetime() if pd.notna(record[col]) else None

    return records