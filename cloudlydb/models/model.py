from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4

from cloudlydb.core.dynamodb import PutItemCommand


def _fully_qualified_name(cls):
    return cls.__module__ + "." + cls.__name__


class ItemManager:
    def __init__(self):
        self._class = None

    def __get__(self, instance, owner):
        self._class = owner
        return self

    def create(self, pk: str, sk: str, **kwargs):
        """
        Create a new item in the database using config from the attached model class
        """

        cleaned_data = self.__validate_data(pk, sk, kwargs)
        create_command = PutItemCommand(
            self._class.Meta.dynamo_table, data=cleaned_data, key=dict(pk=pk, sk=sk)
        )
        response = create_command.execute()

        if response is None:
            raise Exception("Failed to create item")

        _data = {**cleaned_data}

        # Extract the id from the data being used to initialize the model
        # since it is not part of the model's dataclass fields
        id = _data.pop("id")
        new_item = self._class(**_data)
        new_item.__dict__["_pk"] = pk
        new_item.__dict__["_sk"] = sk
        new_item.__dict__["id"] = id

        return new_item

    def __validate_data(self, pk: str, sk: str, kwargs: dict) -> dict:
        assert self._class is not None, "ModelManager must be attached to a model class"
        assert pk is not None, "pk must be provided"
        assert sk is not None, "sk must be provided"

        assert (
            self._class.Meta.dynamo_table is not None
        ), f"dynamo_table must be set in {self._class.__name__}'s Meta class"

        # Get all fields from the model class. Exclude private fields
        public_fields = tuple(f for f, _ in kwargs.items() if not f.startswith("_"))
        all_fields = tuple(f for f, _ in self._class.__dataclass_fields__.items())

        # Check that all fields in kwargs are in the model class
        bad_fields = tuple(
            f for f in public_fields if f not in all_fields and f != "id"
        )
        if bad_fields:
            raise Exception(
                f"{self._class.__name__} does not contain fields: {bad_fields}"
            )

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


@dataclass
class DynamodbItem(ABC):
    items = ItemManager()

    def save(self):
        if self.id is None:
            self.__dict__["id"] = f"{datetime.now().timestamp()}{uuid4()}"

        sk = self._create_sk()
        pk = self._create_pk()

        self.items.create(pk=pk, sk=sk, **self.__dict__)

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

    @property
    def id(self):
        return self.__dict__.get("id")
