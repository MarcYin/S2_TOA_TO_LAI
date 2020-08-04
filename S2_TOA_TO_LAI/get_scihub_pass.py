import os
import getpass
import requests
import numpy as np
from os.path import expanduser
from .create_logger import create_logger

file_path = os.path.dirname(os.path.realpath(__file__))
logger = create_logger()

test_url = 'https://scihub.copernicus.eu/dhus/search?q=*&rows=25'

def loginScihub():
    if os.path.exists(file_path + '/.scihub_auth'):
        try:
            username, password = np.loadtxt(file_path + '/.scihub_auth', dtype=str)
            auth = tuple([username, password])
            r = requests.get(test_url, auth = auth)
            while r.status_code == 401:
                logger.error('Wrong username and password stored, please enter again')
                username = input('Username for scihub: ')
                password = getpass.getpass('Password for scihub: ')
                auth = tuple([username, password])
                r = requests.get(test_url, auth = auth)
            os.remove(file_path + '/.scihub_auth')
            with open(file_path + '/.scihub_auth', 'wb') as f:     
                for i in auth:                                     
                    f.write((i + '\n').encode())
            auth = tuple([username, password])
        except:
            logger.error('Please provide Copernicus Open Access Hub (https://scihub.copernicus.eu/) username and password for downloading Sentinel 2 data.')
            username = input('Username for scihub: ')
            password = getpass.getpass('Password for scihub: ')
            auth = username, password                              
            r = requests.get(test_url, auth = auth)
            while r.status_code == 401:
                logger.error('Wrong username and password typed, please enter again')
                username = input('Username for scihub: ')
                password = getpass.getpass('Password for scihub: ')
                auth = tuple([username, password])
                r = requests.get(test_url, auth = auth)

            os.remove(file_path + '/.scihub_auth')
            with open(file_path + '/.scihub_auth', 'wb') as f:     
                for i in auth:                                     
                    f.write((i + '\n').encode())
            auth = tuple([username, password])
    else:
        username = input('Username for scihub: ')
        password = getpass.getpass('Password for scihub: ')
        auth = username, password

        r = requests.get(test_url, auth = auth)
        while r.status_code == 401:
            logger.error('Wrong username and password typed, please enter again')
            username = input('Username for scihub: ')
            password = getpass.getpass('Password for scihub: ')
            auth = tuple([username, password])
            r = requests.get(test_url, auth = auth)

        with open(file_path + '/.scihub_auth', 'wb') as f:
            for i in auth: 
                f.write((i + '\n').encode())
if __name__ == '__main__':
    loginScihub()