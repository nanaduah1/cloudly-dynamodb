from dataclasses import dataclass
import pytest

from cloudlydb.models.model import DynamodbItem, UnknownFieldException


class FakeCreateTable:
    def __init__(self):
        self.items = []

    def put_item(self, Item, **kwargs):
        self.items.append({**Item})


class FakeGetTable:
    def __init__(self, response: dict):
        self._response = response
        self._params = None

    def query(self, **kwargs):
        self._params = kwargs
        return dict(Items=(self._response,))


def test_id_auto_generates():
    table = FakeCreateTable()

    @dataclass
    class MyFakeModel(DynamodbItem):
        name: str
        age: int

        class Meta:
            dynamo_table = table

    model = MyFakeModel(name="test", age=10)
    model.save()
    assert model.id is not None
    assert len(table.items) == 1
    item = table.items[0]

    assert item["pk"] == model.__class__._create_pk()
    assert item["sk"] == model.__class__._create_sk(id=model.id)
    assert item["data"]["id"] == model.id
    assert item["data"]["name"] == "test"
    assert item["data"]["age"] == 10
    assert item["data"]["id"] is not None


def test_id_is_unique():
    table = FakeCreateTable()

    @dataclass
    class MyFakeModel(DynamodbItem):
        name: str
        age: int

        class Meta:
            dynamo_table = table

    model1 = MyFakeModel(name="test", age=10)
    model2 = MyFakeModel(name="test2", age=11)
    model1.save()
    model2.save()
    assert model1.id != model2.id


def test_create_record_from_manager():
    table = FakeCreateTable()

    @dataclass
    class MyFakeModel(DynamodbItem):
        name: str
        age: int

        class Meta:
            dynamo_table = table

    model = MyFakeModel.items.create(name="test", age=10)
    assert model.id is not None
    assert len(table.items) == 1
    item = table.items[0]

    assert item["data"]["id"] == model.id
    assert item["data"]["name"] == "test"
    assert item["data"]["age"] == 10
    assert item["data"]["id"] is not None


def test_raises_exception_when_field_is_not_on_model():
    table = FakeCreateTable()

    @dataclass
    class MyFakeModel(DynamodbItem):
        name: str
        age: int

        class Meta:
            dynamo_table = table

    with pytest.raises(UnknownFieldException):
        MyFakeModel.items.create(last_name="test", age=10)


def test_get_record_from_manager():
    table = FakeGetTable(
        {
            "pk": "test",
            "sk": "test",
            "data": {"id": "test", "name": "test", "age": 10},
        }
    )

    @dataclass
    class MyFakeModel(DynamodbItem):
        name: str
        age: int

        class Meta:
            dynamo_table = table

    model = MyFakeModel.items.get(id="test")
    assert model.id == "test"
    assert model.name == "test"
    assert model.age == 10


def test_override_pk():
    table = FakeCreateTable()

    @dataclass
    class MyFakeModel(DynamodbItem):
        name: str
        age: int
        _country: str = "USA"

        @classmethod
        def _create_pk(cls, **kwargs):
            return f"DATA#FAKA#{kwargs.get('_country')}"

        class Meta:
            dynamo_table = table

    model = MyFakeModel(name="test", age=10, _country="UK")
    model.save()
    assert model.id is not None
    assert len(table.items) == 1
    item = table.items[0]

    assert item["pk"] == model.__class__._create_pk(_country="UK")
    assert item["sk"] == model.__class__._create_sk(id=model.id)
    assert item["data"]["name"] == "test"
    assert item["data"]["age"] == 10
    assert item["data"]["id"] is not None


def test_override_sk():
    table = FakeCreateTable()

    @dataclass
    class MyFakeModel(DynamodbItem):
        name: str
        age: int
        _country: str = "USA"

        @classmethod
        def _create_sk(cls, **kwargs):
            return f"DATA#FAKA#{kwargs.get('_country')}"

        class Meta:
            dynamo_table = table

    model = MyFakeModel(name="test", age=10, _country="UK")
    model.save()
    assert model.id is not None
    assert len(table.items) == 1
    item = table.items[0]

    assert item["pk"] == model.__class__._create_pk()
    assert item["sk"] == model.__class__._create_sk(_country="UK")
    assert item["data"]["name"] == "test"
    assert item["data"]["age"] == 10
    assert item["data"]["id"] is not None
