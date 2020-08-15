"""
Main script to auto analizing data
Usage: >>> [DEBUG=1] [RESEARCH=1] [CHANNEL=1] python3 main.py
DEBUG       - use local database storage after loading from BigQuery
            - dont write results to BigQuery
RESEARCH    - comparing central values
            - dont write results to BigQuery
CHANNEL     - use channel field when scoring
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

# set scoring function
if os.getenv("CHANNEL"):
    Score = lib_main.workloadScoringByStatusesChannels
else:
    Score = lib_main.workloadScoringByStatuses

# scoring research if specified
if os.getenv("RESEARCH"):
    result, result_total = Score(dataframe, 63, 7)
    
    result_robust, result_total_robust = Score(dataframe, 63, 7, UseRobast=True)
    
    result_Student, result_total_Student = Score(dataframe, 63, 7, ConfidenceInterval=0.05)
    
    # count_sups = dataframe.groupby('assignee_id').count()
    # print(pd.DataFrame([result_t('assignee_id'),
                        # result_total_robust.set_index('assignee_id')]))
    print(result_total.merge(result_total_robust, on='assignee_id', suffixes=['', '_robust'])\
                      .merge(result_total_Student, on='assignee_id', suffixes=['', '_Student'])\
                      .set_index('assignee_id'))
    # print(pd.DataFrame([result_total, result_total_robust, result_total_Student]))
    exit()

# scoring
print("Start scoring...")
result, result_total = Score(dataframe, 63, 7)

# insert into bq
table_total = 'score_result_total'
if os.getenv("CHANNEL"):
    table_result = 'score_result_status_channel'
else:
    table_result = 'score_result_status'

if not os.getenv("DEBUG"):
    lib_main.insertScoreResultData(result, 'findcsystem', 'xsolla_summer_school', table_result)
    lib_main.insertScoreResultData(result_total, 'findcsystem', 'xsolla_summer_school', table_total)
