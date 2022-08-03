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
        print("Cannot connect to MongoDB")
        response = mqttclient.publish(
            topic = response_topic,
            qos=1,
            payload=json.dumps({"success": False, "message": "Server Error"})
        )
        return response

    db = client.licensingdb
    col = db.licensing
    
    license_key = event.get('license_key')
    hardware_id = event.get('hardware_id')
    
    if license_key is None or hardware_id is None:
        response = mqttclient.publish(
            topic = response_topic,
            qos=1,
            payload=json.dumps({"success": False, "message": "Invalid request"})
        )
        return response
    
    print(f'Recived check license request with args: license_key: {license_key} hardware_id: {hardware_id}')
    
    lic = col.find_one({'license_key': license_key})
    
    if lic is None:
        response = mqttclient.publish(
            topic = response_topic,
            qos=1,
            payload= json.dumps({"success": True, "validLicense": False, "message": "License not found"})
        )
        return response
    
    if lic.get("hardware_id") != hardware_id:
        response = mqttclient.publish(
            topic = response_topic,
            qos=1,
            payload=json.dumps({"success": True, "validLicense": False, "message": "Hardware id not match"})
        )
        return response
        
    client.close()

    response = mqttclient.publish(
        topic = response_topic,
        qos=1,
        payload=json.dumps({"success": True, "validLicense": True })
    )
    
    return response