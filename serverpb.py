import sqlite3
import pandas as pd
from sodapy import Socrata
import paho.mqtt.client as paho

broker="localhost"
port=1883
def on_publish(client,userdata,result):
    print("Device 1 : Data published.")
    pass

sqlite_file = 'melbourne.sqlite'    # name of the sqlite database file
conn = sqlite3.connect(sqlite_file)
c = conn.cursor()

client = Socrata("data.melbourne.vic.gov.au", None)
results = client.get("277b-wacc", limit=2000)
results_df = pd.DataFrame.from_records(results)
results_df.to_sql("data", conn, if_exists="replace")

c.execute("SELECT * FROM data")
row = c.fetchall()
# print(c.fetchall())

client= paho.Client("admin")
client.on_publish = on_publish
client.connect(broker,port)
print(row[0])
#publishing timestamp 0
ret= client.publish("/timestamp", ' | '.join(str(e) for e in row[0]))
print("Stopped...")

#publishing temp_avg 6
ret= client.publish("/tempavg", ' | '.join(str(e) for e in row[6]))
print("Stopped...")


#publishing boardid index 3
ret= client.publish("/boardid", ' | '.join(str(e) for e in row[3]))
print("Stopped...")


conn.commit()
conn.close()
