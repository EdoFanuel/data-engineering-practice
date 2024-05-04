from urllib.error import HTTPError
from pathlib import Path

import requests
import gzip
import shutil


CC_DATA_SERVER = 'https://data.commoncrawl.org'
INDEX_NAME = 'CC-MAIN-2022-05'
TEMP_FOLDER = './temp'

def main():
    print('==== STARTING ====')
    print(f'Downloading WET file for {INDEX_NAME} index')
    main_file = download_url(f'{CC_DATA_SERVER}/crawl-data/{INDEX_NAME}/wet.paths.gz')
    main_file_decompressed = gzip.decompress(main_file)
    first_segment = main_file_decompressed.splitlines()[0].decode()
    print(f'Downloading first segment at {CC_DATA_SERVER}/{first_segment}')
    segment_file = download_url_as_file(f'{CC_DATA_SERVER}/{first_segment}', TEMP_FOLDER, first_segment.split('/')[-1])
    with gzip.open(segment_file, 'rb') as data:
        i = 0
        for line in data:
            print(f'#{i} | {line.decode()}', end='')
            # Only printing first 500 lines so it doesn't take forever
            i = i + 1 
            if i > 500:
                break
    shutil.rmtree(TEMP_FOLDER)
    print('==== COMPLETED ====')

# Not using boto3 anymore because unsigned access via S3 API is disabled now 
# Refer to (https://commoncrawl.org/blog/introducing-cloudfront-access-to-common-crawl-data)
def download_url(url):
    response = requests.get(url)
    if not response.ok:
        raise HTTPError(f'HTTP Status code: {response.status_code} - {response.reason}')
    print(f'Download from {url} succeed')
    return response.content     

def download_url_as_file(url, dir, filename):
    Path(dir).mkdir(parents=True, exist_ok=True)
    filepath = f'{dir}/{filename}'
    response = requests.get(url, stream=True)
    with open(filepath, 'wb') as file:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                file.write(chunk)
    print(f'Downwload from {url} to {filepath} succeed')
    return filepath


if __name__ == "__main__":
    main()
