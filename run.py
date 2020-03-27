import sys
import os
import json
from threading import Thread

import checkbot.checkbot as checkbot

if len(sys.argv) is 2: #config file is specified
    config_file = os.path.normpath(sys.argv[1])
else:
    config_file = os.path.join('.', 'data', 'config.json')

with open(config_file) as data_file:
    config = json.load(data_file)

checkbot.initialize(
    bot_id=config['bot_id']
    )

refreshthread = Thread(target=checkbot.schduled_tasks)
refreshthread.start()
