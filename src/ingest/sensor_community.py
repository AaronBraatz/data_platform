"""Load data from sensor.community."""
import re
from asyncio import as_completed
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from time import sleep

import requests
from lxml import html
from tqdm import tqdm

from src.minio_helper import load_file_to_bucket, BUCKET_SENSOR_COMMUNITY, check_file_exist, get_files_per_date

BASE_URL = 'https://archive.sensor.community/'
DATE_PATTERN = r'\d{4}-\d\d-\d\d'
FILE_PATTERN = r'\d{4}-\d\d-\d\d.+\.csv'
RETRY_MAX_ATTEMPTS = 10
RETRY_WAIT_SECONDS = 5
MAX_WORKER = 14

def get_sub_urls(base_url: str, pattern: str) -> list[str]:
    for _ in range(RETRY_MAX_ATTEMPTS):
        try:
            response = requests.get(base_url)
            tree = html.fromstring(response.content)
            urls = tree.xpath('//table//tr/td/a/@href')
            return [url for url in urls if re.match(pattern, url)]
        except ConnectionError:
            sleep(RETRY_WAIT_SECONDS)


def main():
    loaded_files = get_files_per_date(BUCKET_SENSOR_COMMUNITY)

    for day_url in get_sub_urls(BASE_URL, DATE_PATTERN):
        print(f'Loading for this date: {day_url}')
        file_urls = get_sub_urls(BASE_URL + day_url, FILE_PATTERN)
        needed_files = [day_url + file_url for file_url in file_urls]
        loaded_files_per_date = loaded_files.get(day_url)

        if loaded_files_per_date is None:
            missing_files = needed_files
        else:
            missing_files = list(
                set(needed_files)
                - set(loaded_files_per_date)
            )

        with ProcessPoolExecutor(max_workers=MAX_WORKER) as executor:
            futures = []
            with tqdm(total=len(missing_files)) as pbar:
                for file_url in missing_files:
                    file_path = Path(file_url)
                    if check_file_exist(BUCKET_SENSOR_COMMUNITY, file_path):
                        pbar.update(1)
                        continue
                    future = executor.submit(
                        load_file_to_bucket,
                        file_url=BASE_URL + file_url,
                        file_path=file_path,
                        bucket_name=BUCKET_SENSOR_COMMUNITY
                    )
                    futures.append(future)
                    future.add_done_callback(lambda p: pbar.update(1))

            for future in as_completed(futures):
                future.result()


if __name__ == '__main__':
    main()