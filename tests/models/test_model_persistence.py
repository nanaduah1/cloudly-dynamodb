from dataclasses import dataclass
import pytest
from cloudlydb.models import model


@pytest.fixture
def StudentModel(db_table):
    @dataclass
    class Student(model.DynamodbItem):
        name: str
        age: int

        class Meta:
            dynamo_table = db_table

    return Student


def test_id_auto_generates(StudentModel, model_get):
    model = StudentModel(name="test", age=10)
    model.save()
    assert model.id is not None
    item = model_get(model)
    assert item["pk"] == model.__class__._create_pk()
    assert item["sk"] == model.__class__._create_sk(id=model.id)
    assert item["data"]["id"] == model.id
    assert item["data"]["name"] == "test"
    assert item["data"]["age"] == 10
    assert item["data"]["id"] is not None


def test_id_is_unique(StudentModel):
    model1 = StudentModel(name="test", age=10)
    model2 = StudentModel(name="test2", age=11)
    model1.save()
    model2.save()
    assert model1.id != model2.id


def test_create_record_from_manager(StudentModel, model_get):
    model = StudentModel.items.create(name="test", age=10)
    assert model.id is not None
    item = model_get(model)

    assert item["data"]["id"] == model.id
    assert item["data"]["name"] == "test"
    assert item["data"]["age"] == 10
    assert item["data"]["id"] is not None


def test_raises_exception_when_field_is_not_on_model(StudentModel):
    with pytest.raises(model.UnknownFieldException):
        StudentModel.items.create(last_name="test", age=10)


def test_get_record_from_manager(put_item, StudentModel):
    m = StudentModel.items.create(name="test", age=10)
    model = StudentModel.items.get(id=m.id)
    assert model.id == m.id
    assert model.name == m.name
    assert model.age == m.age


def test_override_pk(db_table, model_get):
    @dataclass
    class Class(model.DynamodbItem):
        name: str
        age: int
        _country: str = "USA"

        @classmethod
        def _create_pk(cls, **kwargs):
            return f"DATA#FAKA#{kwargs.get('_country')}"

        class Meta:
            dynamo_table = db_table

    m = Class(name="test", age=10, _country="UK")
    m.save()
    assert m.id is not None
    item = model_get(m)

    assert item["pk"] == m.__class__._create_pk(_country="UK")
    assert item["sk"] == m.__class__._create_sk(id=m.id)
    assert item["data"]["name"] == "test"
    assert item["data"]["age"] == 10
    assert item["data"]["id"] is not None
