import argparse
import datetime
import logging
import os
from pathlib import Path
from shutil import copy2

import numpy as np
import pandas as pd
import requests

ERROR_STRING = -12513911  # random value to get rid of NaNs
STARTING_YEAR = 2020


def different_dataframe(data_year: pd.DataFrame, filepath: str) -> bool:
    data_year = data_year.replace(np.nan, ERROR_STRING)
    last_data = pd.read_excel(filepath, keep_default_na=False)
    last_data = last_data.drop(['Unnamed: 0'], axis=1)
    last_data = last_data.replace(r'^\s*$', ERROR_STRING, regex=True)
    dtypes = data_year.dtypes
    last_data = last_data.astype(dtypes)
    last_data = last_data.set_index([pd.Index(range(1, len(data_year) + 1))])
    return (last_data != data_year).any().any()


def scrape_wta(current_dir: str):
    last_run_path = os.path.join(current_dir, "data\\last_run.txt")
    f = open(last_run_path, "r")
    last_run_date = f.read()
    f.close()
    last_run_date_path = Path(os.path.join(current_dir, f"data\\{last_run_date}"))

    today = datetime.datetime.now().date()
    f = open(last_run_path, "w")
    f.write(str(today))
    f.close()

    year = STARTING_YEAR  # get it automatically, but it does not work on January 1st
    while True:
        data_year = pd.DataFrame()
        page = 0
        while True:
            url = f"https://api.wtatennis.com/tennis/stats/{year}/Current_Rank?page={page}&pageSize=100&sort=asc"
            r = requests.get(url=url)

            # extracting data in json format
            data = r.json()
            if len(data) == 0:
                print(year, page)
                break
            data_year = data_year.append(pd.DataFrame(data))
            page = page + 1

        if not data_year.empty:
            data_year = data_year.set_index([pd.Index(range(1, len(data_year) + 1))])
            data_year.tourn_year = data_year.tourn_year.astype(int)
            data_year.PlayerNbr = data_year.PlayerNbr.astype(int)
            filepath = os.path.join(current_dir, f"data\\wta_{year}.xlsx")
            if not os.path.exists(filepath):
                data_year.to_excel(filepath)
            elif different_dataframe(data_year, filepath):
                print(f"Changes in year {year}")
                last_run_date_path.mkdir(parents=True, exist_ok=True)
                copy2(filepath, last_run_date_path)
                data_year.to_excel(filepath)
        else:
            break
        year = year - 1


if __name__ == "__main__":
    start_time = datetime.datetime.now()

    parser = argparse.ArgumentParser(description="Script to download stats from WTA official webpage.")
    parser.add_argument("--working_directory", help="The working directory of the scraper", required=False)
    args = parser.parse_args()

    working_directory = args.working_directory
    if working_directory is None:
        working_directory = os.getcwd()

    scrape_wta(working_directory)

    end_time = datetime.datetime.now()
    logging.info(f"\nDuration: {(end_time - start_time)}")
