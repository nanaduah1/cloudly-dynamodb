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


def test_model_with_nested_object(db_table, model_get):
    @dataclass
    class Address(model.Serializable):
        street: str
        city: str
        state: str
        zip: str

    @dataclass
    class Person(model.DynamodbItem):
        name: str
        age: int
        address: Address = model.ObjectField(Address)

        class Meta:
            dynamo_table = db_table

    m = Person(
        name="test",
        age=10,
        address=Address(street="123", city="test", state="test", zip="12345"),
    )

    m.save()

    item = model_get(m)
    assert item["data"]["address"]["street"] == "123"
    assert item["data"]["address"]["city"] == "test"
    assert item["data"]["address"]["state"] == "test"
    assert item["data"]["address"]["zip"] == "12345"


def test_get_record_from_manager_with_nested_object(put_item, db_table):
    @dataclass
    class Address(model.Serializable):
        street: str
        city: str
        state: str
        zip: str

    @dataclass
    class Person(model.DynamodbItem):
        name: str
        age: int
        address: Address = model.ObjectField(Address)

        class Meta:
            dynamo_table = db_table

    m = Person(
        name="test",
        age=10,
        address=Address(street="123", city="test", state="test", zip="12345"),
    )

    m.save()

    item: Person = Person.items.get(id=m.id)
    assert item.id == m.id
    assert item.name == m.name
    assert item.age == m.age
    assert item.address.street == m.address.street
    assert item.address.city == m.address.city
    assert item.address.state == m.address.state
    assert item.address.zip == m.address.zip


def test_update_record_from_manager_with_nested_object(put_item, db_table):
    @dataclass
    class Address(model.Serializable):
        street: str
        city: str
        state: str
        zip: str

    @dataclass
    class Person(model.DynamodbItem):
        name: str
        age: int
        address: Address = model.ObjectField(Address)

        class Meta:
            dynamo_table = db_table

    m = Person(
        name="test",
        age=10,
        address=Address(street="123", city="test", state="test", zip="12345"),
    )

    m.save()

    m.address.city = "test2"
    m.address.state = "test2"
    m.save()

    item: Person = Person.items.get(id=m.id)
    assert item.id == m.id
    assert item.name == m.name
    assert item.age == m.age
    assert item.address.street == m.address.street
    assert item.address.city == m.address.city
    assert item.address.state == m.address.state
    assert item.address.zip == m.address.zip


def test_multi_level_nested_object_saves(db_table, get_item):
    @dataclass
    class City(model.Serializable):
        name: str
        state: str

    @dataclass
    class Address(model.Serializable):
        street: str
        zip: str
        city: City = model.ObjectField(City)

    @dataclass
    class Person(model.DynamodbItem):
        name: str
        age: int
        address: Address = model.ObjectField(Address)

        class Meta:
            dynamo_table = db_table

    m = Person.items.create(
        name="test",
        age=10,
        address=Address(
            street="123", zip="12345", city=City(name="test", state="test")
        ),
    )

    db_item: Person = Person.items.get(id=m.id)
    assert db_item.address.city.name == "test"
    assert db_item.address.city.state == "test"
