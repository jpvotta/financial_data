import datetime
import time
import pandas as pd
import selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

STOCK_LIST_URL = 'https://www3.hkexnews.hk/sdw/search/ccass_stock_list.htm?sortby=stockcode&shareholdingdate='

def scrape_list_of_securities(d: datetime.date) -> pd.DataFrame:
        
    url = STOCK_LIST_URL + d.strftime('%Y%m%d')
    
    options = Options()
    options.add_argument("--headless=new")
    driver = webdriver.Chrome(options=options)

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

    # need to save the dataframe somewhere (s3)

    return out_df



if __name__ == '__main__':

    securities_df = scrape_list_of_securities(datetime.date(2023, 7, 14))

    securities_df.to_csv('securities_df.csv')
    
    # securities_df.to_pickle('securities_df.pkl')

