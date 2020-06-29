# RaumueberwachungSS20-Asseco-AzureFunction-Writetotable

Azure function “writetotable1” 

This function is run on a trigger.  The function is triggered whenever the Service Bus forwards a message to this function. 

 

On each run, function firstly converts the payload into json object.  

Payload example: 

{'PayLoadTimeStamp': '2020-05-27T12:31:01Z', 'ReverseTimeStamp': '8586110242237421568.000000', 'Payload': {'aq_temperature': 29.73, 'baro_altitude': 505.283, 'aq_iaq_accuracy': 1, 'motion_detector': 0, 'baro_temperature': 28.47, 'aq_air_pressure': 967.47, 'aq_iaq_index': 95, 'al_illuminance': 3.19, 'aq_humidity': 26.69, 'baro_airpressure': 969.637}​} 

 

 

From original payload, we separate each key-value pair representing a sensor and its value into separate json objects. Namely, one sensor-value pair is represented by 2 json objects. 

 

One object will be stored as a row in “Raw” table and another object will be stored in “Last” table. 

 

The reason for this is because “Raw” and “Last” tables do not have the same attributes so each json object is adjusted for a specific table. 

 

So, as we have 10 different sensor-value pairs we create 20 new json objects. 

 

Currently, “Device_Id” is hardcoded as “device01” as there is only one device. 

 

After all json object have been created, the fuction makes an HTTP request of type POST to Azure Storage - “Raw2” table, containing in the body appropriate json object. This process is repeated in loop for 10 times until all 10 json objects are written into the table. 
 
After that, the same procedure happens when making requests to the “Last2” table.
