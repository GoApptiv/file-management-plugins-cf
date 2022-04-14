import urllib.request
import os
import json
from google.cloud import storage, vision
import google.oauth2.credentials
from google.cloud import pubsub_v1
import constants
import base64
import io
import time
import logging

BASE_TEMP_DIR = "/tmp/"

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = constants.google_vision_api_key_path

def store_output_to_bucket(event, context):

    try:
        pubsub_data_string = event['data']
        pubsub_message = base64.b64decode(pubsub_data_string).decode('utf-8')
        data = json.loads(pubsub_message, strict=False)

        uuid = None
        variant_id = None
        project_id = None
        file_name = None
        source_path = None
        destination_path = None
        bucket_access_token = None
        bucket_name = None
        topic_name = None

        if data.get('metadata'):
            uuid = data.get('metadata').get('uuid')
            variant_id = data.get('metadata').get('variantId')
            project_id = data.get('metadata').get('projectId')

        if data.get('bucket'):
            if data.get('bucket').get('source'):
                file_name = data.get('bucket').get('source').get('file')
                source_path = data.get('bucket').get('source').get('path')
            if data.get('bucket').get('destination'):
                destination_path = data.get('bucket').get('destination').get('path')
                bucket_access_token = data.get('bucket').get('destination').get('accessToken')
                bucket_name = data.get('bucket').get('destination').get('bucketName')
    
        if data.get('response'):
            topic_name = data.get('response').get('topic')

        bucket_credentials = google.oauth2.credentials.Credentials(bucket_access_token)

        source_file_path = str(source_path) + '/' + str(file_name)

        print("Downloading file from bucket")
        file_details = download_from_bucket(bucket_name, file_name, project_id, bucket_credentials, source_file_path)

        print("Checking if image is downloaded completely or not")
        if file_details[1] == (os.stat(file_details[0]).st_size):
            print("File size match")

            print("Running vision API on image")
            with io.open(file_details[0], 'rb') as image:
                image_data = image.read()
            vision_api_json_response = run_vision_api_on_image(image_data)

            file_name_without_extension = os.path.splitext(file_name)[0]
            file_name_json_format = str(file_name_without_extension) + '.json'

            print("Uploading vision api response to bucket")
            upload_to_bucket(vision_api_json_response, file_name, source_path, bucket_name, project_id, bucket_credentials)
                    
            print("Creating custom pub-sub object")
            pubsub_object = create_pubsub_object(uuid, variant_id, destination_path, file_name_json_format, constants.success_response)

            print(pubsub_object)
            print("Publishing custom pub-sub object to topic")
            publish_to_pubsub_topic(constants.pubsub_project_id, topic_name, pubsub_object)

            print("Deleting files from temporary directory")
            delete_files_from_directory(BASE_TEMP_DIR)

            print('testing the response...')
            return (constants.success_response, constants.success_status_code)
    except Exception as e:
        print(str(e))
        pubsub_object = create_pubsub_object(uuid, variant_id, destination_path, file_name,
                                             constants.failure_response)
        publish_to_pubsub_topic(constants.pubsub_project_id, topic_name, pubsub_object)
        print('failure response')
        return (constants.failure_response, constants.success_status_code)
        

def create_pubsub_object(uuid, variant_id, file_path, file_name, status):
    file_name_without_extension = os.path.splitext(file_name)[0]
    file_name_json_format = str(file_name_without_extension) + '.json'
    response_object = {
        "uuid": uuid,
        "variantId": variant_id,
        "filePath" : file_path,
        "fileName" : file_name_json_format,
        "status": status
    }
    return response_object


def publish_to_pubsub_topic(project_id, topic_name, data):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = constants.pubsub_key
    publisher = pubsub_v1.PublisherClient()
    print(f'Publishing message to topic {topic_name}')

    topic_path = publisher.topic_path(project_id, topic_name)

    message_json = json.dumps({
        'message': data,
    })
    message_bytes = message_json.encode('utf-8')
    print('message encoded')

    attributes = {
        'projectId': str(project_id)
    }

    try:
        publish_future = publisher.publish(topic_path, data=message_bytes, **attributes)
        publish_future.result()
        print('Message published.')
    except Exception as e:
        logging.error(e)
        return (e, constants.internal_server_error_code)


def delete_files_from_directory(directory):
    for f in os.listdir(directory):
        os.remove(os.path.join(directory, f))


def download_from_bucket(bucket_name, file_name, project_id, bucket_credentials, source_file_path):

    storage_client = storage.Client(credentials=bucket_credentials)
    local_file_path = BASE_TEMP_DIR + file_name
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(source_file_path)
    file_size_from_bucket = blob.size
    blob.download_to_filename(local_file_path, raw_download=True)

    return (local_file_path, file_size_from_bucket)


def run_vision_api_on_image(image_data):

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = constants.google_vision_api_key_path
    client = vision.ImageAnnotatorClient()
    image = vision.Image(content=image_data)
    response = client.text_detection(image=image)
    response_in_json = vision.AnnotateImageResponse.to_json(response)
    return response_in_json


def upload_to_bucket(json_data, file_name, source_path, bucket_name, project_id, bucket_credentials):
    
    file_name_without_extension = os.path.splitext(file_name)[0]
    file_name_json_format = file_name_without_extension + '.json'
    remote_json_file_path = source_path + '/'  + file_name_json_format

    storage_client = storage.Client(credentials=bucket_credentials)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(remote_json_file_path)
    blob.upload_from_string(json_data, content_type='application/json')