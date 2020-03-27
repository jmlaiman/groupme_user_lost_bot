from http.server import HTTPServer
import re
import os.path
import json
import urllib.request as request
import http.client as httplib
import logging
import schedule
import time
#from .messagerouter import MessageRouter
from .messages_collector import MessagesCollector
import threading
import sqlite3


player_data_file = os.path.join('.', 'data', 'player_data.json')
config_file = os.path.join('.', 'data', 'config.json')

class CheckBot():
    def __init__(self, bot_id):
        self.logger = logging.getLogger('checklog')

        with open(config_file) as data_file:
            config = json.load(data_file)


        self.bot_id = config['bot_id']
        self.refresh_days = config['refresh_days']

        self.messages_collection = MessagesCollector()

        self.logger.info("Bot initialized; bot_id=%s", bot_id)
        self.messages_collection.get_messages(self.refresh_days)

    #related to logging
    def _attach_debug_handler(self):
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

    #update the database from recent groupme messages
    def refresh_data_files(self):
        self.messages_collection.get_messages(self.refresh_days)
        self.messages_collection.update_names()
        self.messages_collection.get_likes()
        
    #check the last time a user has sent a message or liked a message
    def check_interact(self):
        self.messages_collection.update_interact()

def initialize(bot_id=0, service_credentials=None):
    global bot
    bot = CheckBot(
        bot_id=bot_id)
    return bot

def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()

def schduled_tasks():
    #bot.birthday_time()
     
    #schedule.every(5).minutes.do(run_threaded,bot.check_interact)
    #schedule.every().minute.do(run_threaded,bot.check_interact)
    schedule.every().hour.do(run_threaded,bot.refresh_data_files)

    while 1:
            schedule.run_pending()
            time.sleep(5)


