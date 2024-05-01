import requests

from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from zipfile import ZipFile
from pathlib import Path

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def main():
    print('=== Starting download ===')
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_url, url, "./downloads") for url in download_uris]
        for future in as_completed(futures):
            print(future.result())
    print('=== Download completed ===')
        

def process_url(url, destination_folder):
    Path(destination_folder).mkdir(parents=True, exist_ok=True)
    try:
        print(f'{url} -- Starting')
        zip_content = download_file(url)
        extract_zip(zip_content, destination_folder)
        print(f'{url} -- Succeed')
        return url, True
    except Exception as e:
        print(f'{url} -- Failed. Cause {e}')
        return url, False


def download_file(url):
    response = requests.get(url)
    if not response.ok:
        raise IOError(f'HTTP Status code: {response.status_code} - {response.reason}')
    print(f'Download from {url} succeed')
    return response.content     

def extract_zip(zip_bytes, path):
    zip_file = ZipFile(BytesIO(zip_bytes))
    zip_file.extractall(path)


if __name__ == "__main__":
    main()
