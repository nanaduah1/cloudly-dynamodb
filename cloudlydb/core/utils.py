from boto3.dynamodb.transform import TypeDeserializer

deserializer = TypeDeserializer()


def normalize_dynamo_json(dynamo_json: dict):
    return {k: deserializer.deserialize(v) for k, v in dynamo_json.items()}
