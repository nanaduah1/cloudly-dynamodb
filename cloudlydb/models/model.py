from abc import ABC
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Iterable
from uuid import uuid4

from cloudlydb.core.dynamodb import PutItemCommand, UpdateItemCommand, QueryTableCommand


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

        pk = self._model_class._create_pk()
        sk = instance._create_sk()

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

        return self._model_class._from_item_dict(response)

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

        assert pk is not None, "pk must be provided"
        assert sk is not None, "sk must be provided"
        cleaned_data = self.__validate_data(**kwargs)
        update_command = UpdateItemCommand(
            self._model_class.Meta.dynamo_table,
            key=dict(pk=pk, sk=sk),
            data=cleaned_data,
        )
        response = update_command.execute()
        if response is None:
            raise Exception("Failed to update item")

        return True

    def get(self, pk: str, sk: str, index_name=None) -> "DynamodbItem":
        """
        Get an item from the database using config from the attached model class
        """

        assert pk is not None, "pk must be provided"
        assert sk is not None, "sk must be provided"

        get_command = QueryTableCommand(
            self._model_class.Meta.dynamo_table,
            index_name=index_name,
        )
        items = get_command.with_pk(pk).with_sk(sk).execute()

        if not items:
            return None

        obj = self._model_class._from_item_dict(items[0])
        return obj

    def all(
        self,
        pk: str,
        predicate: Callable[[QueryTableCommand], QueryTableCommand] = None,
        index_name: str = None,
        limit: int = None,
        ascending: bool = False,
    ) -> Iterable["DynamodbItem"]:
        """
        Fetch all items matching the pk and sk filter.
        pk must always be exact. sk can be a beginswith filter (e.g. "sk__beginswith")
        """

        assert pk is not None, "pk must be provided"
        assert predicate is not None, "predicate must be provided"
        assert callable(predicate), "predicate must be callable"

        query_command = QueryTableCommand(
            self._model_class.Meta.dynamo_table,
            index_name=index_name,
            max_records=limit,
            scan_forward=ascending,
        )

        query_command = predicate(query_command)
        results = query_command.with_pk(pk).execute()
        return (self._model_class._from_item_dict(item) for item in results)

    def delete(self, pk: str, sk: str):
        """
        Delete an item from the database using config from the attached model class
        """

        assert pk is not None, "pk must be provided"
        assert sk is not None, "sk must be provided"

        data_table = self._model_class.Meta.dynamo_table
        data_table.delete_item(Key=dict(pk=pk, sk=sk))
        return True


class IdField:
    """
    A descriptor that auto-generates an id for a model
    """

    def __init__(self):
        self._name = None

    def __get__(self, instance, owner):
        if instance is None:
            return self
        value = instance.__dict__.get(self._name)
        return value

    def __set__(self, instance, value):
        instance.__dict__[self._name] = value

    def __set_name__(self, owner, name):
        self._name = name


@dataclass
class DynamodbItem(ABC):
    id = IdField()
    items = ItemManager()

    def save(self) -> bool:
        res = self.items.create(**self.__dict__)
        self.id = res.id
        return True

    def delete(self) -> bool:
        pk = self._create_pk()
        sk = self._create_sk()
        return self.items.delete(pk=pk, sk=sk)

    @classmethod
    def _create_pk(cls):
        discriminator = getattr(cls.Meta, "model_name", None)
        if discriminator is None:
            discriminator = _fully_qualified_name(cls)

        return discriminator

    def _create_sk(self):
        sk = f"{self.__class__.__name__}#{self.id}"
        return sk

    @classmethod
    def _new_id(cls):
        return f"{datetime.now().timestamp()}{uuid4()}"

    @classmethod
    def _from_item_dict(cls, item: dict):
        data = item.get("data", {})
        item_id = data.get("id")
        data_without_id = {k: v for k, v in data.items() if k != "id"}
        obj = cls(**data_without_id)
        obj.id = item_id
        return obj

    def __str__(self):
        return f"<{self.__class__.__name__} {self.id}>"
