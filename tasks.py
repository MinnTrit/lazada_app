from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from celery import Celery
from laz_scrapper import Scrapper
import pandas as pd
import io
import os
import json

app = Flask(__name__)
USERNAME = os.getenv('username')
PASSWORD = os.getenv('password')
HOST = os.getenv('instance_host')
DATABASE_NAME = os.getenv('database_name') 
PORT = os.getenv('port')
connection_url = f'mysql://{USERNAME}:{PASSWORD}@{HOST}/{DATABASE_NAME}'
app.config['SQLALCHEMY_DATABASE_URI'] = connection_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['CELERY_RESULT_BACKEND'] = os.getenv('broker_url')
app.config['CELERY_BROKER_URL'] = os.getenv('broker_url')
db = SQLAlchemy(app)
broker_url = os.getenv('broker_url')

def make_celery(app):
    celery = Celery(
        app.import_name,
        backend=app.config['CELERY_RESULT_BACKEND'],
        broker=app.config['CELERY_BROKER_URL']
    )
    celery.conf.update(app.config)
    return celery

celery = make_celery(app)

@celery.task(queue='lazada_scrapping')
def scrape_lazada(search_word:str,
                  scrapping_pages:int):
    lazada_scrapper = Scrapper(
        search_word, 
        scrapping_pages
    )
    lazada_scrapper.get_product()

@celery.task(queue='lazada_scrapping')
def renew_cookies(raw_input_cookies:str):
    save_file_path = "/home/ubuntu/cookies.json"
    result_dict = {}
    cookies_df = pd.read_csv(io.StringIO(raw_input_cookies),sep=";")
    cookies_list = cookies_df.columns.tolist()
    clean_list = [cookie.strip() for cookie in cookies_list]
    for node in clean_list:
        key = node.split("=")[0]
        value = node.split("=")[1]
        result_dict[key] = value
    with open(save_file_path, 'w') as file:
        json.dump(result_dict, file)