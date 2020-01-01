import pandas as pd
import requests


def scrape_wta():
    year = 2019
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
            data_year.to_excel(f"data/wta_{year}.xlsx")
        else:
            break
        year = year - 1


if __name__ == "__main__":
    scrape_wta()
