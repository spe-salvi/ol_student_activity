import os
from dotenv import load_dotenv
load_dotenv()

BASE_URL = "https://franciscan.instructure.com"
BETA_URL = "https://franciscan.beta.instructure.com"
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
ACCESS_TOKEN_2 = os.getenv('ACCESS_TOKEN_2')
ACCESS_TOKEN_EL = os.getenv('ACCESS_TOKEN_EL')
ACCESS_TOKEN_EL_2 = os.getenv('ACCESS_TOKEN_EL_2')

 
API_URL = f'{BASE_URL}/api/v1'
BETA_API_URL = f'{BETA_URL}/api/v1'
FUS_ACCOUNT = '/accounts/1'
HEADERS = {
    "Authorization": "Bearer " + ACCESS_TOKEN_EL
    }

MAX_WORKERS = 15

TERM = 116
SEARCH_TERM = '-OL-'