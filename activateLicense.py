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
    function_checksum = event.get('function_checksum')
    app_type = event.get('app_type')
    
    if hardware_id is None or app_type is None or function_checksum is None:
        response = mqttclient.publish(
            topic = response_topic,
            qos=1,
            payload= json.dumps({"success": False, "message": "Invalid request"})
        )
        return response
    
    print(f'Recived activate license request with args: hardware_id: {hardware_id} function_checksum: {function_checksum} app_type: {app_type}')
    
    count = col.count_documents({"hardware_id": hardware_id, "app_type": app_type})
    if count > 0:
        response = mqttclient.publish(
            topic = response_topic,
            qos=1,
            payload= json.dumps({"success": True, "validLicense": False, "message": "License for this hardware_id and app_type already exist"})
        )
        return response

    license_key = ''.join(random.choice(string.ascii_lowercase) for i in range(15))
    expiration = datetime.datetime.now() + datetime.timedelta(days = 30)
    print(f'Generated license_key: {license_key} expires on: {expiration}')
    
    col.insert_one({'hardware_id': hardware_id, 'function_checksum': function_checksum, 'app_type': app_type, 'license_key': license_key, 'expiration': expiration })
    
    client.close()
    
    license = f'{license_key};{hardware_id};{function_checksum};{app_type};{expiration}'.encode()
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
