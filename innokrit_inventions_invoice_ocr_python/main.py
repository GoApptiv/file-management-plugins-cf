import os
import json
import google.oauth2.credentials
import constants
import base64
import logging
import requests
from google.cloud import storage
from google.cloud import pubsub_v1


def store_output_to_bucket(event, context):

    try:
        pubsub_data_string = event['data']
        print(pubsub_data_string)
        pubsub_message = base64.b64decode(pubsub_data_string).decode('utf-8')
        data = json.loads(pubsub_message, strict=False)

        uuid = None
        variant_id = None
        project_id = None
        file_name = None
        source_path = None
        destination_path = None
        destination_bucket_access_token = None
        destination_bucket_name = None
        read_signed_url = None
        topic_name = None

        if data.get('metadata'):
            uuid = data.get('metadata').get('uuid')
            variant_id = data.get('metadata').get('variantId')
            project_id = data.get('metadata').get('projectId')

        if data.get('bucket'):
            if data.get('bucket').get('source'):
                read_signed_url = data.get('bucket').get(
                    'source').get('readSignedUrl')
                file_name = data.get('bucket').get('source').get('file')
                source_path = data.get('bucket').get('source').get('path')
            if data.get('bucket').get('destination'):
                destination_path = data.get('bucket').get(
                    'destination').get('path')
                destination_bucket_access_token = data.get('bucket').get(
                    'destination').get('accessToken')
                destination_bucket_name = data.get('bucket').get(
                    'destination').get('bucketName')

        if data.get('response'):
            topic_name = data.get('response').get('topic')

        bucket_credentials = google.oauth2.credentials.Credentials(
            destination_bucket_access_token)

        print("Running Innokrit Inventions OCR API on image")
        api_json_response = run_api_on_image(read_signed_url)

        file_name_without_extension = os.path.splitext(file_name)[0]
        file_name_json_format = str(file_name_without_extension) + '.json'

        print("Uploading api response to bucket")
        upload_to_bucket(json.dumps(api_json_response), file_name,
                         source_path, destination_bucket_name, bucket_credentials)

        file_destination_path = source_path + '/' + file_name_json_format

        print("Checking if the json uploaded")
        check_if_file_exists(file_destination_path,
                             destination_bucket_name, bucket_credentials)

        print("Creating custom pub-sub object")
        pubsub_object = create_pubsub_object(
            uuid, variant_id, destination_path, file_name_json_format, constants.success_response)

        print(pubsub_object)
        print("Publishing custom pub-sub object to topic")
        publish_to_pubsub_topic(
            constants.pubsub_project_id, project_id, topic_name, pubsub_object)

        print('success response...')
        return (constants.success_response, constants.success_status_code)

    except Exception as e:
        logging.error(str(e))
        pubsub_object = create_pubsub_object(uuid, variant_id, destination_path, file_name,
                                             constants.failure_response)
        publish_to_pubsub_topic(
            constants.pubsub_project_id, topic_name, pubsub_object)
        print('failure response')
        return (constants.failure_response, constants.success_status_code)


def create_pubsub_object(uuid, variant_id, file_path, file_name, status):
    file_name_without_extension = os.path.splitext(file_name)[0]
    file_name_json_format = str(file_name_without_extension) + '.json'
    response_object = {
        "uuid": uuid,
        "variantId": variant_id,
        "filePath": file_path,
        "fileName": file_name_json_format,
        "status": status
    }
    return response_object


def publish_to_pubsub_topic(project_id, project_id_from_request, topic_name, data):
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
        'projectId': str(project_id_from_request)
    }
    print('project_id  ', project_id_from_request)

    try:
        publish_future = publisher.publish(
            topic_path, data=message_bytes, **attributes)
        publish_future.result()
        print('Message published.')
    except Exception as e:
        print(e)
        return (e, constants.internal_server_error_code)


def run_api_on_image(image_data):

    payload = json.dumps({
        "images": [
            {
                "imageUrl": image_data
            }
        ],
        "skipPerspectiveError": True,
        "applyPerspectiveCorrection": False,
        "documentInfo": {
            "type": "inv_stm",
            "invStmCode": "goapptiv"
        }
    })

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    response = requests.request(
        "POST", constants.api_url, headers=headers, data=payload)
    response_in_json = response.json()
    return response_in_json


def upload_to_bucket(json_data, file_name, source_path, bucket_name, bucket_credentials):

    file_name_without_extension = os.path.splitext(file_name)[0]
    file_name_json_format = file_name_without_extension + '.json'
    remote_json_file_path = source_path + '/' + file_name_json_format

    storage_client = storage.Client(credentials=bucket_credentials)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(remote_json_file_path)
    blob.upload_from_string(json_data, content_type='application/json')


def check_if_file_exists(file_path, bucket_name, bucket_credentials):
    storage_client = storage.Client(credentials=bucket_credentials)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_path)
    if blob.exists():
        print(f'File {file_path} exists')
    else:
        print(f'File does not exist')
        raise Exception("File does not exist")
