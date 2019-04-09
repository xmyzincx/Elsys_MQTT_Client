#!/usr/bin/python

from __future__ import division
import paho.mqtt.client as mqtt
import json
import time
import datetime
import Queue
import threading
import ConfigParser
import MySQLdb 
from decimal import Decimal
import sys
import logging
import logging.config
import warnings
import os
import signal


# Some settings to catch MySQL warnings
warnings.filterwarnings('error', category=MySQLdb.Warning)

# Loading configuration file
config_file_name = 'server.conf'
data_queue = Queue.Queue()
config = ConfigParser.ConfigParser()
config.read(config_file_name)

# Setting up logger
file_name, extension = os.path.splitext(os.path.basename(sys.argv[0]))
logger = logging.getLogger(file_name)
handler = logging.handlers.RotatingFileHandler(('/var/log/mqttClientsLog/' + file_name + '.log'), maxBytes=10485670, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Number of threads for handling MQTT messages
worker_threads = 1

# Topic for publish and subscription
subs_topic = "cwc/elsys/downlinkMessage"

# AppEUI for filtering messages from Elsys sensors only
app_eui = "43-57-43-5f-44-45-4d-4f"

# Boolean for main thread alive
main_thread_alive = True

# Handler for TERM signal
def terminate_signal_handler(signal, frame):
    logger.critical("Got termination signal from system")
    global main_thread_alive
    main_thread_alive = False
    mqtt_client.loop_stop()
    logger.info("MQTT client loop stopped")
    mqtt_client.disconnect()
    logger.info("MQTT client disconnected")
    logging.shutdown()


# This is asynchronous thread for inserting data to MySQL database
class insert_thread(threading.Thread):
    insert_query = ("INSERT INTO virpac.elsys_data (timestamp_created, ACK, ADR, AppEUI, CHAN, CLS, CODR, DeviceID, FREQ, LSNR, MHDR, MODU, OPTS, `PORT`, RFCH, RSSI, SEQN, Size, timestamp_node, Payload, MsgID, Temperature, Humidity, CO2, Light, PIR, Battery) VALUES (%(timestamp_created)s, %(ACK)s, %(ADR)s, %(AppEUI)s, %(CHAN)s, %(CLS)s, %(CODR)s, %(DeviceID)s, %(FREQ)s, %(LSNR)s, %(MHDR)s, %(MODU)s, %(OPTS)s, %(PORT)s, %(RFCH)s, %(RSSI)s, %(SEQN)s, %(Size)s, %(timestamp_node)s, %(Payload)s, %(MsgID)s, %(Temperature)s, %(Humidity)s, %(CO2)s, %(Light)s, %(PIR)s, %(Battery)s)")


    def __init__(self, queue, db_con):
        threading.Thread.__init__(self)
        self.queue = queue
        self.db = db_con
        self.cursor = self.db.cursor()


    def run(self):
        while True:
            global main_thread_alive
            if main_thread_alive:
                #print (self.queue.get())
                db_mesg = self.parse_message(self.queue.get())
                if db_mesg:
                    #print(db_mesg)
                    if db_mesg['AppEUI'] == app_eui:
                        #print(self.insert_query % db_mesg)
                        try:
                            self.cursor.execute(self.insert_query, db_mesg)
                            self.db.commit()
                        except (MySQLdb.Error, MySQLdb.Warning) as e:
                            logger.error("Error occured while executing MySQL query.")
                            try:
                                logger.error("MySQL error %d: %s: " % (e.args[0], e.args[1]))
                            except IndexError:
                                logger.error("MySQL error: %s" % str(e))
                            logger.info("Not panicking, carrying on anyway!!")
                            return None
                    self.queue.task_done()
                else:
                    logger.info("Main thread stopped")
                    self.db.close()
                    logger.info("Database connection closed")
                    


    # This messages parser is specifically for Elsys sensors only.
    # For other sensors, you can write your own parser.
    def parse_message(self, raw_mesg):
        db_mesg_json = {}
        payload = []
        try:
            # raw_mesg_json is the message obtained from broker
            raw_mesg_json = json.loads(raw_mesg)
            #print(raw_mesg_json)
            # db_mesg_json is JSON object with respect to database.
            # This is kind of mapping raw_mesg_json to db_mesg_json
            db_mesg_json["ACK"] = raw_mesg_json["ack"]
            db_mesg_json["ADR"] = raw_mesg_json["adr"]
            db_mesg_json["AppEUI"] = raw_mesg_json["appeui"]
            db_mesg_json["CHAN"] = raw_mesg_json["chan"]
            db_mesg_json["CLS"] = raw_mesg_json["cls"]
            db_mesg_json["CODR"] = raw_mesg_json["codr"]
            db_mesg_json["DeviceID"] = raw_mesg_json["deveui"]
            db_mesg_json["FREQ"] = Decimal(raw_mesg_json["freq"])
            db_mesg_json["LSNR"] = Decimal(raw_mesg_json["lsnr"])
            db_mesg_json["MHDR"] = raw_mesg_json["mhdr"]
            db_mesg_json["MODU"] = raw_mesg_json["modu"]
            if not raw_mesg_json["opts"]:
                db_mesg_json["OPTS"] = None
            else:
                db_mesg_json["OPTS"] = raw_mesg_json["opts"]
            db_mesg_json["PORT"] = raw_mesg_json["port"]
            db_mesg_json["RFCH"] = raw_mesg_json["rfch"]
            db_mesg_json["RSSI"] = raw_mesg_json["rssi"]
            db_mesg_json["SEQN"] = raw_mesg_json["seqn"]
            mesg_size = raw_mesg_json["size"]
            db_mesg_json["Size"] = mesg_size
            timestamp_str  = raw_mesg_json["timestamp"]
            # TODO Have to correct this. This does not produce correct result.
            db_mesg_json["timestamp_node"] = time.mktime(datetime.datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%fZ").timetuple())
            payload = raw_mesg_json["payload"]
            db_mesg_json["Payload"] = str(payload)
            db_mesg_json["MsgID"] = raw_mesg_json["_msgid"]
            db_mesg_json["Temperature"] = ((payload[1]*256) + payload[2])/10
            db_mesg_json["Humidity"] = payload[4]
            db_mesg_json["Light"] = (payload[6]*256) + payload[7]
            if mesg_size == 16:
                db_mesg_json["PIR"] = 0
                db_mesg_json["CO2"] = 0
                db_mesg_json["Battery"] = ((payload[9]*256) + payload[10])/1000
            if mesg_size == 20:
                db_mesg_json["PIR"] = payload[9]
                db_mesg_json["CO2"] = 0
                db_mesg_json["Battery"] = ((payload[11]*256) + payload[12])/1000
            if mesg_size == 24:
                db_mesg_json["PIR"] = payload[9]
                db_mesg_json["CO2"] = (payload[11]*256) + payload[12]
                db_mesg_json["Battery"] = ((payload[14]*256) + payload[15])/1000
            db_mesg_json["timestamp_created"] = int(round(time.time() * 1000))
            return db_mesg_json

        except ValueError as e:
            logger.warning("Incomming message is not a valid JSON.")
            logger.warning("Error: " + e.message)
            logger.warning(raw_mesg)
            return False

        except Exception as e:
            logger.warning("Something went wrong. Ignoring message.")
            return False


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to the broker with code: " + str(rc))
        client.connected_flag = True

        # Topic for CWC CloudMQTT broker
        client.subscribe(subs_topic)

        # Topic for CWC Panoulu broker
        #client.subscrinbe("test")
    else:
        logger.critical("Error occured while connecting to the broker. Error code: " + str(rc))


def on_message(client, userdata, mesg):
    data_queue.put(mesg.payload)


def on_disconnect(client, userdata, rc):
    logging.CRITICAL("Client disconnected. Trying to reconnect.")


def init_client_object():

    client = mqtt.Client("mqttClient")

    broker_user = config.get('MqttBroker', 'user')
    broker_pass = config.get('MqttBroker', 'passwd')

    # Credentials for CloudMQTT broker
    client.username_pw_set(broker_user, password=broker_pass)
    client.connected_flag = False
    client.on_connect = on_connect
    client.on_message = on_message
    #client.on_disconnect = on_disconnect

    return client


def connect_client(mqtt_client):

    # Server and port for CloudMQTT broker
    rc = mqtt_client.connect("m21.cloudmqtt.com", port=14551, keepalive=60)

    # Server and port for Panoulu broker (localhost only)
    #client.connect("127.0.0.1", port=1883, keepalive=60)
    if rc == 0:
        mqtt_client.loop_forever()
    else:
        logger.critical("Connection to the broker failed. Return code: " + str(rc))


if __name__ == '__main__':

    # Setting handler for terminate signal
    signal.signal(signal.SIGTERM, terminate_signal_handler)

    # Initializing database
    db_host = config.get('Database', 'host')
    db_port = config.get('Database', 'port')
    db_user = config.get('Database', 'user')
    db_pass = config.get('Database', 'passwd')
    db_name = config.get('Database', 'database')
    db_table = config.get('Database', 'table')
    
    try:
        db = MySQLdb.Connect(host = db_host, port = int(db_port), user = db_user, passwd = db_pass, db = db_name)
        logger.info("Connected to database.")

        for i in range(worker_threads):
            t = insert_thread(data_queue, db)
            t.setDaemon(True)
            t.start()
            logger.info("%s has started", t.getName())

        # Initializing MQTT client and connecting it to localhost broker
        mqtt_client = init_client_object()
        connect_client(mqtt_client)

    except MySQLdb.Error as e:
        db.rollback()
        logger.critical("Error occured in connecting to the database. Error: " + e.message)

    except KeyboardInterrupt:
        db.close()
        logger.critical("Database connection closed.")
        logger.critical("System interrupted, exiting system!")
        sys.exit(1)
