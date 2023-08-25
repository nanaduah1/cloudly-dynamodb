from dataclasses import dataclass
from cloudlydb.models.model import DynamodbItem


class FakeCreateTable:
    def __init__(self):
        self.items = []

    def put_item(self, Item):
        self.items.append(Item)


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

    assert item["pk"] == model._pk
    assert item["sk"] == model._sk
    assert item["data"]["id"] == model.id
    assert item["data"]["name"] == "test"
    assert item["data"]["age"] == 10


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
