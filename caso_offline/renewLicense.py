import json
import boto3
import pymongo
import sys
import random
import string
import os
import datetime

import base64
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA

key_pem = """-----BEGIN PRIVATE KEY-----
            <your private key>
-----END PRIVATE KEY-----
"""

def lambda_handler(event, context):
    n_to_remove = len('/request')
    response_topic = event['topic'][:-n_to_remove] + '/response'
    mqttclient = boto3.client('iot-data', verify=False)
    
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
    license_key = event.get('license_key')
    
    if license_key is None or hardware_id is None or app_type is None:
        response = mqttclient.publish(
            topic = response_topic,
            qos=1,
            payload= json.dumps({"success": False, "message": "Invalid request"})
        )
        return response
    
    print(f'Recived renew license request with args: license_key {license_key} hardware_id: {hardware_id} app_type: {app_type}')
    
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

    if lic.get("app_type") != app_type:
        response = mqttclient.publish(
            topic = response_topic,
            qos=1,
            payload=json.dumps({"success": True, "validLicense": False, "message": "App type not match"})
        )
        return response
    
    new_expiration = datetime.datetime.now() + datetime.timedelta(days = 30)
    print(f'New expiration for license_key: {license_key} expires on: {new_expiration}')
    
    col.find_one_and_update({'license_key': license_key}, { "$set": { "expiration": new_expiration } })
    
    client.close()
    
    license = f'{license_key};{hardware_id};{app_type};{new_expiration}'.encode()
    key = RSA.import_key(key_pem) 
    hash = SHA256.new(license)

    signer = pkcs1_15.new(key)
    signature = signer.sign(hash)
    
    response = mqttclient.publish(
        topic = response_topic,
        qos=1,
        payload=json.dumps({"success": True, "validLicense": True, "license": license.decode(), "signature": base64.b64encode(signature).decode('ascii')})
    )
    
    return response