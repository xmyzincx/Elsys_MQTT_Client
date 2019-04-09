# MQTT Client

This is a simple Python v2.7 script that acts as a MQTT client. It can do following tasks,
* It can be connected to local or remote MQTT broker and can only subscribe (at the moment) to a topic.
* Parses the messages from Elsys sensors.
* It stores the data into MySQL database.
* Filters messages from Elsys sensors only which have specific App EUI.
* Manages the logs in /var/logs/mqttClientLogs.log file.
* This Mqtt Client performs database transaction using threads and number of threads can be configured in the code.

## Configuration
For this client to work properly, you have to define 'server.conf' file which should look something like this:  


	```  
	[Database]  
	host: localhost  
	user: abc  
	passwd: xyz  
	port: 3306  
	database: virpac  
	table: elsys_data  
	```
	```	  
	[MqttBroker]  
	user: cba  
	passwd: zyx  
	```

## Running the script
You can run the script by 'sudo python mqtt_client_v2.py'

