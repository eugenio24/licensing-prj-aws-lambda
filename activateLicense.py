import json
import boto3
import pymongo
import sys
import random
import string
import os

def lambda_handler(event, context):
    n_to_remove = len('/request')
    response_topic = event['topic'][:-n_to_remove] + '/response'
    mqttclient = boto3.client('iot-data', region_name='us-east-1')
    
    client = pymongo.MongoClient(os.environ['DB_URL']) 
    try:    
        client.admin.command('ping')
    except ConnectionFailure:
        response = mqttclient.publish(
            topic = response_topic,
            qos=1,
            payload=json.dumps({"success": False, "message": "Server Error"})
        )
        return response

    db = client.licensingdb
    col = db.licensing
    
    hardware_id = event.get('hardware_id')
    app_type = event.get('app_type')
    
    if hardware_id is None or app_type is None:
        response = mqttclient.publish(
            topic = response_topic,
            qos=1,
            payload= json.dumps({"success": False, "message": "Invalid request"})
        )
        return response
    
    print(f'Recived activate license request with args: hardware_id: {hardware_id} app_type: {app_type}')
    
    count = col.count_documents({"hardware_id": hardware_id, "app_type": app_type})
    if count > 0:
        response = mqttclient.publish(
            topic = response_topic,
            qos=1,
            payload= json.dumps({"success": True, "validLicense": False, "message": "License for this hardware_id and app_type already exist"})
        )
        return response

    license_key = ''.join(random.choice(string.ascii_lowercase) for i in range(15))
    
    print(f'Generated license_key: {license_key}')
    
    col.insert_one({'hardware_id': hardware_id, 'app_type': app_type, 'license_key': license_key })

    client.close()

    response = mqttclient.publish(
        topic = response_topic,
        qos=1,
        payload=json.dumps({"success": True, "validLicense": True, "license_key": license_key})
    )
    
    return response