import requests
import sys
import json
import time
import os
import logging
import csv
from random import randint
import sqlite3
import ast
import datetime

#establishes config file paths
config_path = os.path.join('.','data','config.json')

class MessagesCollector():

    def __init__(self):
        self.logger = logging.getLogger('checklog')
        self.dbpath = './data/checkdatabase.sqlite3'

        #grabs values from config file
        with open(config_path) as config_file:
            config = json.load(config_file)
        self.like_threshold = config['like_threshold']
        self.year_like_threshold = config['year_like_threshold']
        self.limit_image = config['limit_image']       
        self.api_key = config['api_key']
        self.group_id = config['group_id']
        self.image_disabled = config['disable_image'] 

    #creates/updates messages in the local database, pass in how many days back to scan, 0 to scan every message (can take a long time!)
    def get_messages(self,days_back=0):
        apikey = self.api_key
        groupid = self.group_id
        db = sqlite3.connect(self.dbpath)
        
        cursor = db.cursor()

        #creates users table if it doesn't exist
        cursor.execute('''
                CREATE TABLE IF NOT EXISTS users(
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    image_url TEXT,
                    other_id TEXT unique,
                    likes INTEGER,
                    rank INTEGER,
                    date_added INTEGER,
                    self_likes INTEGER,
                    times_kicked INTEGER,
                    times_kicker INTEGER,
                    can_randimage INTEGER
                    )
            ''')
        db.commit()
        #create messages table if it doesn't exist
        cursor.execute('''
                CREATE TABLE IF NOT EXISTS messages(
                    id TEXT PRIMARY KEY,
                    created_at INTEGER,
                    text TEXT,
                    favorites INTEGER,
                    favorited_by TEXT,
                    is_bot INTEGER,
                    sender_id TEXT,
                    system TEXT,
                    has_image INTEGER,
                    has_loci INTEGER,
                    has_tag INTEGER,
                    attachments TEXT,
                    event TEXT,
                    CONSTRAINT fk_users
                        FOREIGN KEY (sender_id)
                        REFERENCES users(id)
                )
            ''')
        db.commit()
        #create interact table if it doesn't exist
        cursor.execute('''
                CREATE TABLE IF NOT EXISTS interact(
                    time TEXT PRIMARY KEY,
                    message_count INTEGER,
                    unique_posters INTEGER,
                    unique_interacts INTEGER
                    )
            ''')
        db.commit()
        
        #get columns from interact table
        tb = cursor.execute('SELECT * from interact;')
        cols = next(zip(*tb.description))

        #iterate through each user in the users table and add to interact table if they don't exist
        for u in cursor.execute("SELECT id FROM users;"):
            currentuser = u[0]
            user_interact = u[0] + '_interaction'
            #user_interact_time = u[0] + '_time'
            #if a user that is not in the table, add to interact table
            if not user_interact in cols:
                cursor.execute('''ALTER TABLE interact ADD COLUMN {} TEXT'''.format(user_interact))
        db.commit()
        
        tb = cursor.execute('SELECT * from interact;')
        cols = next(zip(*tb.description))
        print(cols)
        cursor.execute('''INSERT OR IGNORE INTO interact(?,?,?,?)
                          VALUES(?,?,?,?);''', (cols[0],cols[1],cols[2],cols[3],'time',0,0,0)
        )
        db.commit()

        #gets group data, puts group members in users table
        response = requests.get('https://api.groupme.com/v3/groups/' + groupid + '?token='+ apikey)
        currentgroup = response.json()['response']
        members = currentgroup['members']
        for m in members:
            cursor.execute('''INSERT OR IGNORE INTO users(id,name,image_url,other_id,likes,self_likes,times_kicked,times_kicker,can_randimage)
                              VALUES(?,?,?,?,?,?,?,?,?)''', (m['user_id'],m['nickname'],m['image_url'],m['id'],0,0,0,0,1)
            
            )
            db.commit()

        message_id = 0

        #finds stopping point for get messages if a set amount of days
        #finds the messages thats closest to the desired unix time, sets it as the stopping point
        if days_back > 0:
            timegap = time.time() - (86400 * days_back)
            cursor.execute('''SELECT
                                id,
                                created_at
                              FROM
                                messages
                              WHERE
                                created_at < ?
                              LIMIT 1;
                            ''' , (timegap,)
            )
            continue_to = cursor.fetchone()
            #print(continue_to)
            print("uncomment this when done testing, line 139 ish")
            #ending_message = continue_to[0]
            ending_message = 1584719536 
        else:
            cursor.execute('''SELECT
                                id,
                                created_at
                              FROM
                                messages
                              ORDER BY
                                created_at ASC
                              LIMIT 1
                            '''
            )
            continue_to = cursor.fetchone()
            if(continue_to != None):
                ending_message = continue_to[0]
            else:
                ending_message = 0

        #while the current message is less than the desired message, request 100 messages at a time to update in the database
        while 1:
            params = {
                    # Get maximum number of messages at a time
                    'limit': 100,
                }
            if message_id != 0:
                params['before_id'] = message_id
            response = requests.get('https://api.groupme.com/v3/groups/%s/messages?token=%s' % (groupid, apikey), params=params)

            messages = response.json()['response']['messages']
            for m in messages:
                sysval = 0
                is_bot = 0
                
                #if a user that is no longer in the group, add to users table
                cursor.execute('SELECT EXISTS(SELECT 1 FROM users WHERE id="'+ m['sender_id'] +'" LIMIT 1);')
                if cursor.fetchone()[0] == 0:
                    cursor.execute('''INSERT OR IGNORE INTO users(id,name,likes,self_likes,times_kicked,times_kicker,can_randimage)
                              VALUES(?,?,?,?,?,?,?)''', (m['sender_id'],m['name'],0,0,0,0,1))
                    db.commit()

                if m['system']:
                    sysval = 1
                if m['sender_type'] == "bot":
                    is_bot = 1
                
                hasimage = 0
                hasloci = 0
                hastag = 0
                for a in m['attachments']:
                    if a['type'] == 'image':
                        hasimage = 1
                    if a['type'] == 'video':
                        hasimage = 1
                    if a['type'] == 'mentions':
                        hastag = 1
                #case for if the message is a system message, has a few different/missing  attributes
                if sysval == 0:
                    cursor.execute('''INSERT OR REPLACE INTO messages(id,created_at,favorites,favorited_by,sender_id,system,has_image,has_loci,has_tag,attachments,text,is_bot)
                                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''', (m['id'],int(m['created_at']),len(m['favorited_by']),str(m['favorited_by']), str(m['sender_id']),sysval,hasimage,hasloci,hastag,str(m['attachments']),m['text'],is_bot)
                    )
                else:
                    if 'event' in m:
                        cursor.execute('''INSERT OR REPLACE INTO messages(id,created_at,favorites,favorited_by,sender_id,system,has_image,has_loci,has_tag,attachments,text,event,is_bot)
                                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''', (m['id'],int(m['created_at']),len(m['favorited_by']),str(m['favorited_by']), str(m['sender_id']),sysval,hasimage,hasloci,hastag,str(m['attachments']),m['text'],str(m['event']),0)
                        )
                if(m['id'] == ending_message):
                    db.commit()
                    db.close
                    self.logger.info('Refresh completed successfully.')
                    return
            db.commit()
            message_id = messages[-1]['id']
            if(len(messages) < 100):
                break

    #returns group info like total messages, date created, and other non-sensitive config file info
    def get_group_info_message(self):
        response = requests.get('https://api.groupme.com/v3/groups/' + self.group_id + '?token='+ self.api_key)
        currentgroup = response.json()['response']
        return_string = "[Group Information]\n"
        return_string += "Total messages: " + str(currentgroup['messages']['count']) + "\n"
        return_string += "Date created: " + str(datetime.datetime.utcfromtimestamp(currentgroup['created_at']).strftime('%m-%d-%Y | %H:%M:%S UTC')) + "\n"
        try:
            return_string += "Created by: " + str(currentgroup['creator_user_id']) + "\n"
        except:
            return_string += "Created by: UNAVAILABLE ON INITIAL BUILD" + "\n"
        return return_string

    def get_total_messages(self):
        response = requests.get('https://api.groupme.com/v3/groups/' + self.group_id + '?token='+ self.api_key)
        currentgroup = response.json()['response']
        return int(currentgroup['messages']['count'])
        
    #processes the database to calculate likes for each user
    def get_likes(self):
        db = sqlite3.connect(self.dbpath)
        cursor = db.cursor()
        cursor2 = db.cursor()
        cursor_writer = db.cursor()

        #iterate through each user in the users table, then iterate through all their messages and total their likes
        for u in cursor.execute("SELECT id FROM users;"):
            currentuser = u[0]
            currentlikes = 0
            currentSlikes = 0
            for m in cursor2.execute("SELECT favorites,favorited_by FROM messages WHERE sender_id = ?;",(currentuser,)):
                currentlikes += int(m[0])
                favby = ast.literal_eval(m[1])
                if currentuser in favby:
                    currentSlikes += 1
            cursor_writer.execute("UPDATE users SET likes=?,self_likes=? WHERE id=?;", (currentlikes,currentSlikes,currentuser))
        db.commit()
        
        #sort the users table by likes, then give each user their ranking in order
        rank = 1
        for u in cursor.execute("SELECT id,likes FROM users ORDER BY likes DESC;"):
            cursor_writer.execute("UPDATE users SET rank=? WHERE id = ?;", (rank,u[0],))
            rank += 1
        db.commit()
        db.close()
        self.logger.info('Like data updated..')

    #update nicknames for all users currently in the group
    def update_names(self):
        db = sqlite3.connect(self.dbpath)
        apikey = self.api_key
        groupid = self.group_id
        cursor = db.cursor()

        response = requests.get('https://api.groupme.com/v3/groups/' + groupid + '?token='+ apikey)
        #self.logger.info('Server Response: ' + response)
        currentgroup = response.json()['response']
        members = currentgroup['members']

        #for each member in the group, update their nickname in the database
        for m in members:
            cursor.execute("UPDATE users SET name=?,image_url=? WHERE id = ?;",(m['nickname'],m['image_url'],m['user_id']))
        
        db.commit()
        db.close()
        self.logger.info('Nicknames updated..')
    
    def add_interact_table(self):
        db = sqlite3.connect(self.dbpath)
        cursor = db.cursor()
        
        #create base of table
        cursor.execute('''
                CREATE TABLE IF NOT EXISTS interact(
                    time TEXT PRIMARY KEY,
                    message_count INTEGER,
                    unique_posters INTEGER,
                    unique_interacts INTEGER
                    )
            ''')
        db.commit()
        
        #get columns from interact table
        tb = cursor.execute('SELECT * from interact')
        cols = next(zip(*tb.description))

        #iterate through each user in the users table and add to interact table
        for u in cursor.execute("SELECT id FROM users;"):
            currentuser = u[0]
            user_interact = u[0] + '_interaction'
            #user_interact_time = u[0] + '_time'
            #if a user that is not in the table, add to interact table
            if not user_interact in cols:
                cursor.execute('''ALTER TABLE ADD COLUMN {} TEXT'''.format(user_interact))

        db.commit()
        db.close()

   
    def update_interact(self):
        db = sqlite3.connect(self.dbpath)
        cursor = db.cursor()
        
        #iterate through each user in the users table
        for u in cursor.execute("SELECT id FROM users;"):
            currentuser = u[0]
            #while 

        db.close()
        self.logger.info('Interations updated..')

