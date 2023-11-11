import boto3
import pytest
import uuid

from cloudlydb.models import model


@pytest.fixture(scope="session")
def table_name():
    return f"test_table-{uuid.uuid4().hex}"


@pytest.fixture(scope="session")
def db_client():
    return boto3.client("dynamodb", endpoint_url="http://localhost:8000")


@pytest.fixture(scope="session")
def dynamo_table(db_client, table_name):
    yield db_client.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    db_client.delete_table(TableName=table_name)


@pytest.fixture(scope="session")
def db_table(dynamo_table, table_name):
    return boto3.resource("dynamodb", endpoint_url="http://localhost:8000").Table(
        table_name
    )


@pytest.fixture(scope="session")
def put_item(db_table):
    def _put_item(item):
        db_table.put_item(Item=item)

    return _put_item


@pytest.fixture(scope="session")
def get_items(db_table):
    def _get_items():
        return db_table.scan()["Items"]

    return _get_items


@pytest.fixture(scope="session")
def get_item(db_table):
    def _get_item(pk, sk):
        return db_table.get_item(Key=dict(pk=pk, sk=sk))

    return _get_item


@pytest.fixture(scope="session")
def model_get(db_table):
    def _get(model: model.DynamodbItem):
        pk = model._create_pk(**model.__dict__)
        sk = model._create_sk(**model.__dict__)
        return db_table.get_item(Key=dict(pk=pk, sk=sk))["Item"]

    return _get
