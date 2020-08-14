"""
Main script to auto analizing data
Usage: >>> [DEBUG=1] [CHANNEL=1] python3 main.py
"""
import os

import pandas as pd
import dotenv
from google.oauth2 import service_account

import lib_main

dotenv.load_dotenv(dotenv.find_dotenv('.env'))

CREDENTIALS = service_account.Credentials.from_service_account_info({
    "type": os.getenv("TYPE"),
    "project_id": os.getenv("PROJECT_ID"),
    "private_key_id": os.getenv("PRIVATE_KEY_ID"),
    "private_key": os.getenv("PRIVATE_KEY"),
    "client_email": os.getenv("CLIENT_EMAIL"),
    "client_id": os.getenv("CLIENT_ID"),
    "auth_uri": os.getenv("AUTH_URI"),
    "token_uri": os.getenv("TOKEN_URI"),
    "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL"),
    "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL"),
})

# choose fields
if os.getenv("CHANNEL"):
    file_name = 'local_db_channel.csv'
    fields = ['assignee_id', 'status', 'channel']
else:
    file_name = 'local_db.csv'
    fields = ['assignee_id', 'status']


# load dataframe
if os.getenv("DEBUG"):
    if os.path.isfile(file_name):
        DataFrame = pd.read_csv(file_name)
    else:
        DataFrame = lib_main.getFreshData(CREDENTIALS, 'findcsystem', fields)
        DataFrame.to_csv(file_name)
else:
    DataFrame = lib_main.getFreshData(CREDENTIALS, 'findcsystem', fields)

dataframe = DataFrame[:].reset_index(drop=True)


# scoring
print("Start scoring...")
if os.getenv("CHANNEL"):
    result, result_total = lib_main.workloadScoringByStatusesChannelsFast(dataframe, 63, 7)

    print("!INFO: As we can see, there is not a uniform distribution of scores between channels, can be significant")
    mean_score = result.groupby('channel')['score_value'].mean().rename('mean_score')
    count_sups = dataframe.groupby('channel')['assignee_id'].count().rename('count')
    print(pd.DataFrame([mean_score, count_sups]).transpose())
else:
    result, result_total = lib_main.workloadScoringByStatuses(dataframe, 63, 7)

# insert into bq
table_total = 'score_result_total'
if os.getenv("CHANNEL"):
    table_result = 'score_result_status_channel'
else:
    table_result = 'score_result_status'

lib_main.insertScoreResultData(result, 'findcsystem', 'xsolla_summer_school', table_result)
lib_main.insertScoreResultData(result_total, 'findcsystem', 'xsolla_summer_school', table_total)

