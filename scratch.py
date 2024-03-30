#!/usr/bin/env python3

# Setup.
import sys
import certifi
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import datetime
import sqlalchemy
import concurrent.futures
import time
import random
import pytz


NEW_CERT_PATH = 'SectigoRSAOrganizationValidationSecureServerCA.pem'

# database-related variables
USER = 'bowenanalytics'
PW = 'iwtbaMb#0Bs'
HOST = 'bowenanalytics-db.c6ax3bdwxsjr.us-east-1.rds.amazonaws.com'
PORT = 3306
DBNAME = 'ba_db'

# scraping-related variables
HOLDER_INFO_URL = "https://www.hkexnews.hk/sdw/search/searchsdw.aspx"
STOCK_LIST_URL = 'https://www.hkexnews.hk/sdw/search/stocklist.aspx?sortby=stockcode&shareholdingdate='
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'


def scrape_list_of_securities(date):
    """Queries the CCASS website for the list of all securities as of a
    particular date. Returns a Pandas dataframe containing this information.
    """
    # The date format is like "20190514".
    date_str = date.strftime('%Y%m%d')
    url = STOCK_LIST_URL + date_str
    data = None

    try:
        print('Attempting to scrape HKEX list of securities for {0}...'
              .format(date.strftime('%m/%d/%Y')))
        with requests.Session() as s:
            s.headers['user-agent'] = USER_AGENT
            r = s.get(url)
            data = r.text
        print('Successfully scraped HTML table')
    except requests.exceptions.SSLError as err:
        print('SSL Error. Adding custom certs to Certifi store...')
        cafile = certifi.where()
        with open(NEW_CERT_PATH, 'rb') as infile:
            customca = infile.read()
        with open(cafile, 'ab') as outfile:
            outfile.write(customca)
        print('Custom certs added to Certifi store. Exiting execution.')
        sys.exit()

    out_df = parse_list_of_securities(date, data)
    return out_df


def requests_retry_session(
        retries=3,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
        session=None,
):
    """Framework to retry the HTML get or post request if rejected."""
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def scrape_ccass(date, code):
    """This method takes as arguments a datetime object (the date we want to
    scrape) and a string (the code we want to scrape), and returns a string
    containing the HTML of the website query result.
    """
    url = HOLDER_INFO_URL

    """Sleep a random amount of time to avoid whole batch hitting server at 
    the same time.
    """
    time.sleep(random.uniform(0, 2))

    """The string txtShareholdingDate needs to be of the form YYYY/MM/DD, 
    (example: '2019/05/14'). We only need txtShareholdingDate and txtStockCode 
    params to query the database.
    """
    txtShareholdingDate = date.strftime('%Y/%m/%d')
    txtStockCode = code

    data = ''

    """Use session object we paramaterize in requests_retry_session(), which 
    has functionality to retry failed commands. Drastically lowers fail rate.
    """
    with requests_retry_session() as s:
        # with requests.Session() as s:
        s.headers['user-agent'] = USER_AGENT
        r = s.get(url)
        data = r.text
        bs = BeautifulSoup(data, features='html.parser')

        """VIEWSTATE and VIEWSTATEGENERATOR are returned by the website and need 
        to be attached to the post request we make later.
        """
        VIEWSTATE = bs.find('input', {'id': '__VIEWSTATE'}).attrs['value']
        VIEWSTATEGENERATOR = bs.find('input', {'id': '__VIEWSTATEGENERATOR'}).attrs['value']

        params = {
            '__EVENTTARGET': 'btnSearch',
            '__VIEWSTATE': VIEWSTATE,
            '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
            'today': '',
            'sortBy': '',
            'sortDirection': '',
            'alertMsg': '',
            'txtShareholdingDate': txtShareholdingDate,
            'txtStockCode': txtStockCode,
            'txtStockName': '',
            'txtParticipantID': '',
            'txtParticipantName': '',
            'txtSelPartID': '',
        }

        r = s.post(url, params)
        data = r.text

    return data


def parse_summary(date, code, data):
    """Parses the contents of the summary table from our website query. Takes
    a date, stock code, and string of HTML which is the query output, and returns
    a Pandas dataframe containing the parsed data.
    """
    out_dict = {
        'id': np.nan,
        'code': code,
        'date': date,
        'mi_holding': 0,
        'mi_count': 0,
        'mi_percent': np.nan,

        'cip_holding': 0,
        'cip_count': 0,
        'cip_percent': np.nan,

        'ncip_holding': 0,
        'ncip_count': 0,
        'ncip_percent': np.nan,

        'total_holding': 0,
        'total_count': 0,
        'total_percent': np.nan,
        'scraped_date': np.datetime64('now')
    }

    lookup_dict = {
        'Market Intermediaries': 'mi',
        'Consenting Investor Participants': 'cip',
        'Non-consenting Investor Participants': 'ncip',
        'Total': 'total'
    }

    """Searching for ccass-search-datarow returns 2-4 elements:
    option 1: "Market Intermediaries"
    option 2: "Consenting Investor Participants"
    option 3: "Non-consenting Investor Participants"
    option 4: "Total"
    """
    bs = BeautifulSoup(data, 'html.parser')
    summary_table_data = bs.find_all('div', class_='ccass-search-datarow')

    # Loop through the BeautifulSoup object elements, checking for matches.
    for t in summary_table_data:
        this_category = t.find('div', class_='summary-category').text
        # Check each element against 4 options described above.
        for lookup in lookup_dict:
            if (this_category == lookup):

                # This row's holdings.
                holding_outer = t.find('div', class_='shareholding')
                holding_inner = holding_outer.find('div', class_='value').text
                holding = int(holding_inner.replace(',', ''))
                out_key = lookup_dict[lookup] + '_' + 'holding'
                out_dict[out_key] = holding

                # This row's number of participants.
                count_outer = t.find('div', class_='number-of-participants')
                count_inner = count_outer.find('div', class_='value').text
                count = int(count_inner.replace(',', ''))
                out_key = lookup_dict[lookup] + '_' + 'count'
                out_dict[out_key] = count

                # This row's participants' percentage of total listed shares.
                percent_outer = t.find('div', class_='percent-of-participants')
                percent_inner = percent_outer.find('div', class_='value').text
                percent = np.nan
                if (len(percent_inner) > 0):
                    percent = float(percent_inner.replace('%', ''))
                out_key = lookup_dict[lookup] + '_' + 'percent'
                out_dict[out_key] = percent

                """ccass-search-remarks contains "Total number of Issued 
                Shares/Warrants/Units (last updated figure)".
                """
                remarks_data = bs.find_all('div', class_='ccass-search-remarks')
                total_issued_inner = remarks_data[0].find('div', class_='summary-value').text
                total_issued = int(total_issued_inner.replace(',', ''))
                out_dict['shares_outstanding'] = total_issued

    # Export as Pandas DataFrame
    list_of_dicts = []
    list_of_dicts.append(out_dict)
    summary_df = pd.DataFrame(list_of_dicts, columns=out_dict.keys())
    return summary_df


def parse_holdings(date, code, data):
    """Parses the contents of the holdings table from our website query. Takes
    a date, stock code, and string of HTML which is the query output, and returns
    a Pandas dataframe containing the parsed data.
    """

    bs = BeautifulSoup(data, 'html.parser')

    list_of_dicts = []

    holdings_table_data = bs.find_all('table', class_='table')
    holdings_table_body = holdings_table_data[0].find('tbody')

    for row in holdings_table_body.find_all('tr'):
        # print("this is one:\n\n", row, "\n")
        out_dict = {
            'id': np.nan,
            'code': code,
            'date': date
        }

        """columns of table contain the following information:
        [0] Participant ID;
        [1] Name of CCASS Participant (* for Consenting Investor Participants);
        [2] Address;
        [3] Shareholding;
        [4] % of the total number of Issued Shares/ Warrants/ Units.
        """

        cells = row.find_all('td')
        out_dict['participant_id'] = cells[0].find('div', class_='mobile-list-body').text.strip()
        out_dict['participant_name'] = cells[1].find('div', class_='mobile-list-body').text.strip()
        out_dict['participant_address'] = cells[2].find('div', class_='mobile-list-body').text.strip()
        out_dict['holding'] = int(cells[3].find('div', class_='mobile-list-body').text.replace(',', ''))

        out_dict['holding_percent'] = np.nan
        if (len(cells) > 4):
            percent_text = cells[4].find('div', class_='mobile-list-body').text
            if (len(percent_text) > 0):
                out_dict['holding_percent'] = percent_text.replace('%', '')

        out_dict['scraped_date'] = np.datetime64('now')

        # print(out_dict)
        list_of_dicts.append(out_dict)

    # print(list_of_dicts)

    """We denote the variable "cip" to be True if holder is a Consenting 
    Investor Participant, False otherwise.
    """
    holdings_df = pd.DataFrame(list_of_dicts, columns=out_dict.keys())
    holdings_df['is_cip'] = np.where(holdings_df['participant_name'].str.contains('\*'), True, False)

    # # test to see if there are any consenting investor participants in this stock
    # subset_df = holdings_df[holdings_df['is_cip'] == True]
    # subset_df

    return holdings_df


def parse_list_of_securities(date, data):
    """Parses the HTML string from the website query and returns a Pandas
    dataframe containing this information.
    """
    bs = BeautifulSoup(data, 'html.parser')
    table_data = bs.find_all('table', class_='table')
    table_body = table_data[0].find('tbody')

    list_of_dicts = []

    for row in table_body.find_all('tr'):
        # print("this is one:\n\n", row, "\n")
        out_dict = {
            # 'id': np.nan,
            'date': date
        }

        cells = row.find_all('td')
        # for cell in cells:
        # print("this is one:\n\n", cell.text.strip(), "\n")

        out_dict['code'] = cells[0].text.strip()
        out_dict['security_name'] = cells[1].text.strip()
        out_dict['scraped_date'] = np.datetime64('now')
        # print(out_dict)
        list_of_dicts.append(out_dict)

    securities_df = pd.DataFrame(list_of_dicts, columns=out_dict.keys())

    """Extract A-share code from end of name string, if applicable. Typical syntax 
    is ZBOM HOME COLLECTION CO.,LTD (A #603801). Save 603801 in a new column called
    "ashare".
    """
    securities_df['ashare'] = securities_df['security_name'].str.extract('(A\s?#\s?[0-9]{6})')
    securities_df['ashare'] = securities_df['security_name'].str.extract('([0-9]{6}|^$)')
    securities_df = securities_df.replace(np.nan, '', regex=True)

    return securities_df


def make_stock_info_table():
    """Creates a table within the mysql database to store stock
    information scraped from CCASS.
    """
    user = USER
    pw = PW
    host = HOST
    port = PORT
    database = DBNAME
    engine = sqlalchemy.create_engine('mysql+pymysql://' +
                                      user + ':' + pw + '@' + host + ':' + str(port) + '/' + database, echo=False)

    engine.execute("""
  CREATE TABLE ccass_stock_info (
    id BIGINT(20) NOT NULL AUTO_INCREMENT,
    date DATETIME NOT NULL,
    code VARCHAR(256) NOT NULL,
    security_name TEXT,
    ashare TEXT,
    scraped_date DATETIME NOT NULL,
    PRIMARY KEY (id)
  );
  """)

    engine.execute("""
  CREATE INDEX idx_ccass_stock_info_date_code ON ccass_stock_info (code, date) 
  COMMENT '' ALGORITHM DEFAULT LOCK DEFAULT
  """)

    engine.execute("""
  CREATE INDEX idx_ccass_stock_info_date ON ccass_stock_info (date) 
    COMMENT '' ALGORITHM DEFAULT LOCK DEFAULT
  """)

    engine.dispose()


def make_holdings_info_table():
    """Creates a table within the mysql database to store holdings
    information scraped from CCASS.
    """
    user = USER
    pw = PW
    host = HOST
    port = PORT
    database = DBNAME
    engine = sqlalchemy.create_engine('mysql+pymysql://' +
                                      user + ':' + pw + '@' + host + ':' + str(port) + '/' + database, echo=False)

    engine.execute("""
  CREATE TABLE ccass_holdings_info (
    id BIGINT(20) NOT NULL AUTO_INCREMENT,
    code VARCHAR(256) NOT NULL,
    date DATETIME NOT NULL,
    participant_id TEXT,   
    participant_name TEXT,
    participant_address TEXT,
    holding BIGINT(20) NOT NULL,
    holding_percent DOUBLE,
    is_cip TINYINT(1) NOT NULL,
    scraped_date DATETIME NOT NULL,
    PRIMARY KEY (id)
  );
  """)

    engine.execute("""
  CREATE INDEX idx_ccass_holdings_info_code_date ON ccass_holdings_info (code, date) 
  COMMENT '' ALGORITHM DEFAULT LOCK DEFAULT
  """)

    engine.execute("""
  CREATE INDEX idx_ccass_holdings_info_date_code ON ccass_holdings_info (date, code) 
    COMMENT '' ALGORITHM DEFAULT LOCK DEFAULT
  """)

    engine.dispose()


def make_summary_table():
    """Creates a table within the mysql database to store summary
    information scraped from CCASS.
    """
    user = USER
    pw = PW
    host = HOST
    port = PORT
    database = DBNAME
    engine = sqlalchemy.create_engine('mysql+pymysql://' +
                                      user + ':' + pw + '@' + host + ':' + str(port) + '/' + database, echo=False)

    engine.execute("""
  CREATE TABLE ccass_summary_info (
    id BIGINT(20) NOT NULL AUTO_INCREMENT,
    code VARCHAR(256) NOT NULL,
    date DATETIME NOT NULL,
    mi_holding BIGINT(20),
    mi_count BIGINT(20),
    mi_percent DOUBLE,
    cip_holding BIGINT(20),
    cip_count BIGINT(20),
    cip_percent DOUBLE,
    ncip_holding BIGINT(20),
    ncip_count BIGINT(20),
    ncip_percent DOUBLE,
    total_holding BIGINT(20),
    total_count BIGINT(20),
    total_percent DOUBLE,
    shares_outstanding BIGINT(20),
    scraped_date DATETIME NOT NULL,
    PRIMARY KEY (id)
  );
  """)

    engine.execute("""
  CREATE INDEX idx_ccass_summary_info_code_date ON ccass_summary_info (code, date) 
  COMMENT '' ALGORITHM DEFAULT LOCK DEFAULT
  """)

    engine.execute("""
  CREATE INDEX idx_ccass_summary_info_date_code ON ccass_summary_info (date, code) 
  COMMENT '' ALGORITHM DEFAULT LOCK DEFAULT
  """)

    engine.dispose()


def make_tables():
    """Creates new, empty tables in CCASS database (fails if already exists)."""
    make_stock_info_table()
    make_holdings_info_table()
    make_summary_table()


def dump_df_to_sql(data_df, table_name):
    """Append the contents of data_df to the MySQL table table_name"""
    user = USER
    pw = PW
    host = HOST
    port = PORT
    database = DBNAME
    engine = sqlalchemy.create_engine('mysql+pymysql://' +
                                      user + ':' + pw + '@' + host + ':' + str(port) + '/' + database, echo=False)
    data_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    engine.dispose()


def query_codes_on_date(date, table_name):
    """Queries only unique codes for the specified date and database."""
    statement = """
                SELECT code 
                FROM {}
                WHERE date BETWEEN '{}' AND '{}'
                GROUP BY code
                """.format(table_name, date, date)

    return execute_sql(statement)


def query_all_on_date(date, table_name):
    """Performs a full SELECT for the specified date and database. Can be slow."""
    statement = """
                SELECT * 
                FROM {}
                WHERE date BETWEEN '{}' AND '{}'
                """.format(table_name, date, date)

    return execute_sql(statement)

def query_all_on_date_code(date, code, table_name):
    """Performs a full SELECT for the specified date and database. Can be slow."""
    statement = """
                SELECT * 
                FROM {}
                WHERE (date BETWEEN '{}' AND '{}') AND (code = {})
                """.format(table_name, date, date, code)

    return execute_sql(statement)

def query_code_on_date_range(start_date, end_date, code, table_name):
    """Performs a full SELECT for the specified date and database. Can be slow."""
    statement = """
                SELECT * 
                FROM {}
                WHERE (date BETWEEN '{}' AND '{}') AND (code = {})
                """.format(table_name, start_date, end_date, code)

    return execute_sql(statement)


def safe_scrape(date, bucket_size):
    """Checks what we have already in the db and scrapes only when we have nothing
    in the db.
    """
    print('Checking existing records for {}'.format(date))
    stock_info_df = query_codes_on_date(date, 'ccass_stock_info')
    holdings_info_df = query_codes_on_date(date, 'ccass_holdings_info')
    summary_info_df = query_codes_on_date(date, 'ccass_summary_info')
    print('Found {} records in ccass_stock_info for {}'.format(len(stock_info_df), date))
    print('Found {} records in ccass_holdings_info for {}'.format(len(holdings_info_df), date))
    print('Found {} records in ccass_summary_info for {}'.format(len(summary_info_df), date))

    if len(stock_info_df.index) == 0:
        print('Scraping list of securities and dumping into ccass_stock_info')
        stock_info_df = scrape_list_of_securities(date)
        dump_df_to_sql(stock_info_df, 'ccass_stock_info')
    else:
        stock_info_df = query_all_on_date(date, 'ccass_stock_info')

    codes_from_hkex = stock_info_df['code'].tolist()
    codes_from_our_db = summary_info_df['code'].tolist() if len(summary_info_df.index) > 0 else []
    unscraped_codes = [x for x in codes_from_hkex if x not in codes_from_our_db]
    if len(unscraped_codes) > 0:
        print('Scraping summaries and holdings information for {} codes on {}.'.format(len(unscraped_codes), date))
        scrape_by_bucket(unscraped_codes, date, bucket_size)
    else:
        print('This date {} has already been scraped.'.format(date))


def scrape_by_bucket(all_codes, date, bucket_size):
    """Breaks the query into subqueries according to the bucket_size parameter.
    Scrapes using multithreading.
    """
    # Break the query into buckets according to bucket_size
    buckets = []
    current = []
    code_counter = 0
    for c in all_codes:
        code_counter += 1
        current.append(c)
        if code_counter % bucket_size == 0 or code_counter == len(all_codes):
            buckets.append(current)
            current = []

    summaries = []
    holdings = []
    for i in range(len(buckets)):
        print('Scraping bucket {} of {}...'.format(i + 1, len(buckets)))

        # Use a with statement to ensure threads are cleaned up promptly.
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            # Make a dictionary of [Future, code]
            futures = {}
            for code in buckets[i]:
                futures[executor.submit(scrape_ccass, date, code)] = code

            for future in concurrent.futures.as_completed(futures):
                code = futures[future]
                try:
                    data = future.result()
                    summary_df = parse_summary(date, code, data)

                    summaries.append(summary_df)
                    holdings_df = parse_holdings(date, code, data)
                    holdings.append(holdings_df)

                except Exception as exc:
                    print('Code {} generated an exception: {}'.format(code, exc))
                else:
                    print('Code {}\'s page size is {} bytes'.format(code, len(data)))
        print('Sleeping briefly...')
        time.sleep(5)

    big_summaries_df = pd.concat(summaries, sort=False)
    print('Dumping summaries into ccass_summary_info db: {} rows...'.format(len(big_summaries_df)))
    dump_df_to_sql(big_summaries_df, 'ccass_summary_info')

    big_holdings_df = pd.concat(holdings, sort=False)
    print('Dumping holdings into ccass_holdings_info db: {} rows...'.format(len(big_holdings_df)))
    dump_df_to_sql(big_holdings_df, 'ccass_holdings_info')


def execute_sql(statement):
    """Executes a general sql query. Should really have a try/catch block."""
    user = USER
    pw = PW
    host = HOST
    port = PORT
    database = DBNAME
    engine = sqlalchemy.create_engine('mysql+pymysql://' +
                                      user + ':' + pw + '@' + host + ':' + str(port) + '/' + database, echo=False)
    out_result = engine.execute(statement)
    out_list = out_result.fetchall()
    out_df = pd.DataFrame(out_list)
    if len(out_df.index) > 0:
        out_df.columns = out_result.keys()

    engine.dispose()
    return out_df


def scrape_date_range(start_date, end_date, bucket_size):
    """Runs scraping framework on a series of dates between start_date and end_date,
    including endpoints
    """
    dates = pd.date_range(start=str(start_date), end=str(end_date)).tolist()
    for d in dates:
        safe_scrape(d, bucket_size)

def clean_df_column_names(df):
    """Replaces '%' in column names with 'pct'"""
    old_column_names = list(df.columns.values)
    new_column_names = [x.replace('%', 'pct') for x in old_column_names]
    column_dict = dict(zip(old_column_names, new_column_names))
    df = df.rename(columns=column_dict)
    return df

def convert_code_for_yf(code, ashare):
    """Converts the HKEX code into a yf-compatible code, according to the rules:
    1) if A-share code is blank and and HKEX code <= 9999, 4-digit HKEX code + '.HK';
    2) if A-share code is blank and HKEX code > 9999, the full HKEX code + '.HK';
    3) if A-share code begins with '60', the 6-digit A-share code + '.SS';
    4) if A-share code begins with '00' or '300', the 6-digit A-share code + '.SZ';
    """
    out_code = ''
    if (ashare == '') & (int(code) <= 9999):
        out_code = code[1:] + '.HK'
    elif (ashare == '') & (int(code) > 9999):
        out_code = code + '.HK'
    elif (ashare != '') & (ashare[0:2] == '60'):
        out_code = ashare + '.SS'
    elif (ashare != '') & ((ashare[0:2] == '00') | (ashare[0:3] == '300')):
        out_code = ashare + '.SZ'
    else:
        raise ValueError('Something is messed up with code: {}.'.format(code))

    return out_code


def get_day_hkt():
    """Returns the current day (date) in Hong Kong time as of the moment the function is called."""

    # utc_moment_naive = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
    utc_moment_naive = datetime.datetime.utcnow()
    utc_moment = utc_moment_naive.replace(tzinfo=pytz.utc)

    # timezones = ['Asia/Hong_Kong', 'UTC', 'EST']
    # localFormat = "%Y-%m-%d %H:%M:%S"
    # for tz in timezones:
    #     local_date = utc_moment.astimezone(pytz.timezone(tz))
    #     print('with times:', tz, local_date.strftime(localFormat))
    # for tz in timezones:
    #     local_date = utc_moment.astimezone(pytz.timezone(tz)).date()
    #     print('without times:', tz, local_date.strftime(localFormat))

    local_date = utc_moment.astimezone(pytz.timezone('Asia/Hong_Kong')).date()

    return local_date


if __name__ == '__main__':

    # call scrape_list_of_securities: contains logic to update certifications.
    scrape_list_of_securities(get_day_hkt())

    # make_tables()
    # date_to_scrape = datetime(2019, 7, 16)
    # safe_scrape(date_to_scrape, 100)

    # # To catch up.
    # start_date = datetime.datetime(2020, 1, 2)
    # end_date = datetime.datetime(2020, 1, 18)
    # print('Scraping data from {} to {}...'.format(start_date, end_date))
    # scrape_date_range(start_date, end_date, 250)

    # For regular scraping. Incorporate a lag.
    start_date = get_day_hkt() - datetime.timedelta(days=100)
    end_date = get_day_hkt() - datetime.timedelta(days=1)
    print('Scraping data from {} to {}...'.format(start_date, end_date))
    scrape_date_range(start_date, end_date, 250)



