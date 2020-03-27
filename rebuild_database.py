from extbot.messages_collector import MessagesCollector
import os
import datetime
#recreates (or initially creates) the database

m_c = MessagesCollector()

curr_db = os.path.join('data','exdatabase.sqlite3')
os.remove(curr_db)

print("Working with Group:")
print(m_c.get_group_info_message())
print("This may take some time!")
total = m_c.get_total_messages()
print("Your group has %s messages, this may take at least %s minutes." % (total, total/6000))
start_time = datetime.datetime.now()
print("Start time: " + repr(start_time))
print("Requesting messages...")
m_c.get_messages()
m_c.update_names()
m_c.get_likes()

print("Complete!")
end_time = datetime.datetime.now()
print("End time: " + repr(end_time))
