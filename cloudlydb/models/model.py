from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4

from cloudlydb.core.dynamodb import PutItemCommand


class UnknownFieldException(Exception):
    def __init__(self, message: str, *fields):
        super().__init__(f"{message} Bad field(s): {fields}")


def _fully_qualified_name(cls):
    return cls.__module__ + "." + cls.__name__


class ItemManager:
    def __init__(self):
        self._model_class = None
        self._instance = None

    def __get__(self, instance, owner):
        # We can only attach to a subclass of DynamodbItem
        assert issubclass(
            owner, DynamodbItem
        ), "ItemManager must be attached to a DynamodbItem"
        self._model_class = owner
        if instance:
            self._instance = instance

        return self

    def __set__(self, instance, value):
        raise ValueError("Cannot set ItemManager")

    def create(self, **kwargs):
        """
        Create a new item in the database using config from the attached model class
        """

        cleaned_data = self.__validate_data(**kwargs)

        instance = self._instance or self._model_class(**cleaned_data)

        sk = instance._create_sk()
        pk = instance._create_pk()

        assert pk is not None, "pk must be provided"
        assert sk is not None, "sk must be provided"

        # Make sure we have an id
        cleaned_data["id"] = instance.id or self._model_class._new_id()

        create_command = PutItemCommand(
            self._model_class.Meta.dynamo_table,
            data=cleaned_data,
            key=dict(pk=pk, sk=sk),
        )
        response = create_command.execute()

        if response is None:
            raise Exception("Failed to create item")

        _data = {**cleaned_data}

        # Exclude the id from the data being used to initialize the model
        # since it is not part of the model's dataclass fields

        id = _data.pop("id")
        new_item = self._model_class(**_data)
        new_item.__dict__["_pk"] = pk
        new_item.__dict__["_sk"] = sk
        new_item.id = id

        return new_item

    def __validate_data(self, **kwargs) -> dict:
        assert (
            self._model_class is not None
        ), "ModelManager must be attached to a model class"

        assert (
            self._model_class.Meta.dynamo_table is not None
        ), f"dynamo_table must be set in {self._model_class.__name__}'s Meta class"

        # Get all fields from the model class. Exclude private fields
        public_fields = tuple(f for f, _ in kwargs.items() if not f.startswith("_"))
        all_fields = tuple(f for f, _ in self._model_class.__dataclass_fields__.items())

        # Check that all fields in kwargs are in the model class
        bad_fields = tuple(
            f for f in public_fields if f not in all_fields and f != "id"
        )
        if bad_fields:
            raise UnknownFieldException("Unexpected field(s).", bad_fields)

        return {k: v for k, v in kwargs.items() if k in public_fields or k == "id"}

    def update(self, pk: str, sk: str, **kwargs):
        """
        Update an existing item in the database using config from the attached model class
        """

        self.__validate_data(pk, sk, kwargs)

    def get(self, pk: str, sk: str):
        pass

    def all(self, pk: str, **kwargs):
        pass

    def delete(self, pk: str, **kwargs):
        pass


class IdField:
    """
    A descriptor that auto-generates an id for a model
    """

    def __init__(self):
        self.name = None

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = instance.__dict__.get(self.name)
        return value

    def __set__(self, instance, value):
        instance.__dict__[self.name] = value

    def __set_name__(self, owner, name):
        self.name = name


@dataclass
class DynamodbItem(ABC):
    id = IdField()
    items = ItemManager()

    def save(self):
        res = self.items.create(**self.__dict__)
        self.id = res.id

    def delete(self):
        pk = self.__dict__.get("_pk")
        sk = self.__dict__.get("_sk")
        if pk and sk:
            self.items.delete(pk=pk, sk=sk)

    def _create_pk(self):
        discriminator = getattr(self.__class__.Meta, "model_name", None)
        if discriminator is None:
            discriminator = _fully_qualified_name(self.__class__)

        self.__dict__["_pk"] = discriminator
        return discriminator

    def _create_sk(self):
        sk = f"{self.__class__.__name__}#{self.id}"
        self.__dict__["_sk"] = sk
        return sk

    @classmethod
    def _new_id(cls):
        return f"{datetime.now().timestamp()}{uuid4()}"
