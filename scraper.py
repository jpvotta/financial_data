from flytekit import task, workflow, ImageSpec
import os
import datetime
import time
import pandas as pd

STOCK_LIST_URL = 'https://www3.hkexnews.hk/sdw/search/ccass_stock_list.htm?sortby=stockcode&shareholdingdate='

scraper_image_spec = ImageSpec(
    builder="fast-builder",
    name="financial-data-scraper",
    requirements="requirements.txt",
    registry=os.environ.get("DOCKER_REGISTRY", None),
).with_commands([
    "apt-get update",
    "apt-get install -y wget",
    "wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb",
    "apt-get install -y ./google-chrome-stable_current_amd64.deb",
])

@task(container_image=scraper_image_spec)
def scrape_list_of_securities(d: datetime.date) -> pd.DataFrame:

    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.service import Service as ChromeService
    from webdriver_manager.chrome import ChromeDriverManager

    options = Options()
    options.add_argument("--headless=new")
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

    url = STOCK_LIST_URL + d.strftime('%Y%m%d')

    driver.get(url)
    time.sleep(2)

    table = driver.find_element(By.TAG_NAME, "table")
    body = table.find_element(By.TAG_NAME, "tbody")
    rows = body.find_elements(By.TAG_NAME, "tr")

    out_dict = {
        'stock_code': [],
        'ccass_holder': []
    }

    for row in rows:
        elements = row.find_elements(By.TAG_NAME, "td")
        out_dict['stock_code'].append(elements[0].text)
        out_dict['ccass_holder'].append(elements[1].text)

    driver.quit()

    out_df = pd.DataFrame(out_dict)
    print("successfully scraped {} securities".format(out_df.shape[0]))

    return out_df

    # return pd.DataFrame({"a": [1], "b": [2]})



@workflow
def scraper() -> pd.DataFrame:
    securities_df = scrape_list_of_securities(d=datetime.date(2023, 7, 14))
    return securities_df


# if __name__ == '__main__':
#
#     securities_df = scrape_list_of_securities(datetime.date(2023, 7, 14))
#     securities_df.to_csv('securities_df.csv')
#     # securities_df.to_pickle('securities_df.pkl')

