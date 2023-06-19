import requests,json
import pandas as pd
import datetime
import logging
endpoint="https://openlibrary.org/dev/docs/api/subjects.json"
# ep="http://openlibrary.org/api/books"

# logger.info(f"state date is : {start_ts}")
# logger.info(f"end date is : {end_ts}")
# logger.info(f"Clicks Reporting Start date: {pd.to_datetime(start_ts, unit='s')}")
# logger.info(f"Clicks Reporting End date: {pd.to_datetime(end_ts, unit='s')}")
# headers = { 'Authorization': "Bearer " + access_token }
response=requests.get(endpoint)
# response=requests.get(ep)

def subject_api():
    """This function uses requests library to connect and with "GET' method
    to pull information and also flatten json to specifically look for Books and Authors"""
    try:
        while page is not None :
            page=''
            data_text = json.loads(requests.get(endpoint + f"&limit=1000&start={page}").text)
            print(" the next page is " + page, "\n we're calling endpoint " + endpoint + f"&limit=1000&start={page}")

            subject_df=pd.json_normalize(
            data=data_text,
            record_path=['works'],
            max_level=1,
            errors='ignore')

            selector_d = {'name': 'subject', 'works.title': 'Book', 'authors.name': 'Author'}

            subject_df.rename(columns=selector_d)[[*selector_d.values()]]
            subject_df.to_csv('subject.csv' , index=False)

    except (requests.exceptions.HTTPError, requests.exceptions.RequestException):
        raise
    finally:
        print('call complete ' )


def lambda_handler(event, context):
    """
    This is scheduled to run daily with AWS EventBridge and Cloudwatch 
    to post Authors and Book list into csv
    """
    for i in range(1,2):
        to = date.today() - timedelta(days=i-1)
        fromm = date.today() - timedelta(days=i)
        start_date = fromm.strftime('%Y-%m-%d %H:%M:%S')
        end_date = to.strftime('%Y-%m-%d %H:%M:%S')
        ef_load = subject_api(start_date, end_date)
        logging.info(f' the start_date is {start_date}')
        logging.info(f' the end_date is {end_date}')
