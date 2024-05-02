import requests
import pandas as pd
import math

from bs4 import BeautifulSoup
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

main_url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
target_url_column = 'Last modified'
target_last_modified = '2024-01-19 10:45'
target_csv_column = 'HourlyDryBulbTemperature'

def main():
    html_content = download_file(main_url)
    parsed_html = parse_html_content(html_content)
    target_row = parsed_html[parsed_html[target_url_column] == target_last_modified]
    dataframes = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(lambda url: parse_csv(download_file(url)), f'{main_url}{filename}') for filename in target_row['Name']]
        dataframes.extend([future.result() for future in as_completed(futures)])
    combined_frames = pd.concat(dataframes, axis=0, ignore_index=True)
    combined_frames = cleanup_data(combined_frames)
    ids = combined_frames[target_csv_column].idxmax()
    print(combined_frames.loc[ids])

def download_file(url):
    response = requests.get(url)
    if not response.ok:
        raise IOError(f'HTTP Status code: {response.status_code} - {response.reason}')
    print(f'Download from {url} succeed')
    return response.content     

def parse_html_content(content):
    parsed_content = BeautifulSoup(content, 'html.parser')
    headers = [header.get_text() for header in parsed_content.find_all("th")][0:4]
    data = [data.get_text().strip() for data in parsed_content.find_all("td")]
    print(f'Headers = {headers}')
    record = {header: [] for header in headers}
    for i in range(len(data)):
        record[headers[i % 4]].append(data[i])
    return pd.DataFrame(record, columns=headers)

def parse_csv(content):
    return pd.read_csv(BytesIO(content), converters={target_csv_column: to_float})

def cleanup_data(df):
    return df[pd.to_numeric(df[target_csv_column], errors='coerce').notnull()]

def to_float(str):
    try:
        return float(str)
    except ValueError:
        return math.nan


if __name__ == "__main__":
    main()
