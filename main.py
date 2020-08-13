import os

import pandas as pd
import dotenv
from google.oauth2 import service_account

from lib_main import getFreshData, workloadScoringByStatuses, insertScoreResultData

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

if os.getenv("DEBUG") and os.path.isfile('local_db.csv'):
    DataFrame = pd.read_csv('local_db.csv')
else:
    DataFrame = getFreshData(CREDENTIALS, 'findcsystem')
    if os.getenv("DEBUG"):
      DataFrame.to_csv('local_db.csv')
      
test_user = DataFrame[:].reset_index(drop=True)

test_result, test_result_total = workloadScoringByStatuses(test_user, 63, 7)

insertScoreResultData(test_result, 'findcsystem',
                      'xsolla_summer_school', 'score_result_status')
insertScoreResultData(test_result_total, 'findcsystem',
                      'xsolla_summer_school', 'score_result_total')
