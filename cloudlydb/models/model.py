from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Callable, Iterable
from uuid import uuid4


from cloudlydb.core.dynamodb import (
    PutItemCommand,
    UpdateItemCommand,
    QueryTableCommand,
    QueryResults as UntypedQueryResults,
    Table,
)

QueryPredicate = Callable[[QueryTableCommand], QueryTableCommand]


class UnknownFieldException(Exception):
    def __init__(self, message: str, *fields):
        super().__init__(f"{message} Bad field(s): {fields}")


def _fully_qualified_name(cls):
    return cls.__module__ + "." + cls.__name__


class QueryResults:
    def __init__(self, response: UntypedQueryResults, model_class):
        self._response = response
        self._model_class = model_class
        self.__next = 0

    def __iter__(self) -> Iterable["DynamodbItem"]:
        return self

    def __next__(self) -> "DynamodbItem":
        try:
            item = self._model_class._from_item_dict(self._response[self.__next])
            self.__next += 1
            return item
        except IndexError:
            raise StopIteration

    def __getitem__(self, index: int) -> "DynamodbItem":
        return self._model_class._from_item_dict(self._response[index])

    @property
    def last_evaluated_key(self) -> str:
        return self._response.last_evaluated_key()


class IItemKeyFactory(ABC):
    def __init__(self, model_class, **kwargs):
        self._model_class = model_class
        self._kwargs = kwargs

    @abstractmethod
    def for_create(self) -> dict:
        pass

    @abstractmethod
    def for_update(self) -> dict:
        pass

    @abstractmethod
    def for_query(self) -> dict:
        pass

    @abstractmethod
    def for_delete(self) -> dict:
        pass


class DefaultItemKeyFactory(IItemKeyFactory):
    sk_prefix = None
    pk_prefix = None
    id_field = "id"

    def for_create(self) -> dict:
        pk_prefix = self.pk_prefix or _fully_qualified_name(self._model_class)
        sk_prefix = self.sk_prefix or self._model_class.__name__
        sk = f"{sk_prefix}#{self._kwargs.get(self.id_field)}"
        pk = pk_prefix

        return dict(pk=pk, sk=sk)

    def for_update(self) -> dict:
        return self.for_create()

    def for_query(self) -> dict:
        return self.for_create()

    def for_delete(self) -> dict:
        return self.for_create()


class Serializable:
    def to_dict(self):
        return _serialize(self.__dict__)


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

    def _get_database_table(self):
        table = getattr(self._model_class.Meta, "table", None)
        if table is None:
            table = getattr(self._model_class.Meta, "table", None)
            table = table or getattr(self._model_class.Meta, "dynamo_table", None)
            assert (
                table is not None
            ), f"table must be set in {self._model_class.__name__}'s Meta class"

            if isinstance(table, str):
                table = Table.from_name(self._database_table)
                setattr(self._model_class.Meta, "table", table)

        return table

    def _get_key_factory(self, **kwargs):
        key_factory_class = getattr(self._model_class.Meta, "key", None)
        key_factory_class = key_factory_class or DefaultItemKeyFactory
        key_factory = key_factory_class(self._model_class, **kwargs)
        return key_factory

    def create(self, **kwargs):
        """
        Create a new item in the database using config from the attached model class
        """

        cleaned_data = self.__validate_data(**kwargs)
        instance = self._instance or self._model_class(**cleaned_data)

        # Make sure we have an id
        if instance.id is None:
            instance.id = instance._new_id_()

        item_id = instance.id
        cleaned_data["id"] = item_id

        key_factory = self._get_key_factory(id=item_id, **kwargs)
        key = key_factory.for_create()
        pk = key.get("pk")
        sk = key.get("sk")

        assert pk is not None, f"invalid pk value '{pk}'"
        assert sk is not None, f"invalid sk value '{sk}'"

        fully_serialized_data = self._serialize(cleaned_data)
        table = self._get_database_table()
        create_command = PutItemCommand(
            database_table=table,
            data=fully_serialized_data,
            key=key,
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

    def update(self, **kwargs):
        """
        Update an existing item in the database using config from the attached model class
        """

        key_factory = self._get_key_factory(**kwargs)
        key = key_factory.for_update()
        pk = key.get("pk")
        sk = key.get("sk")

        assert pk is not None, f"invalid pk value '{pk}'"
        assert sk is not None, f"invalid sk value '{sk}'"

        cleaned_data = self.__validate_data(**kwargs)
        fully_serialized_data = self._serialize(cleaned_data)
        table = self._get_database_table()
        update_command = UpdateItemCommand(
            database_table=table,
            data=fully_serialized_data,
            key=key,
        )
        update_command.execute()
        return True

    def _serialize(self, data: dict) -> dict:
        return _serialize(data)

    def get(
        self,
        index_name=None,
        predicate: QueryPredicate = None,
        **kwargs,
    ) -> "DynamodbItem":
        """
        Get an item from the database using config from the attached model class
        """

        key_factory = self._get_key_factory(**kwargs)
        key = key_factory.for_query()
        pk = key.get("pk")
        sk = key.get("sk")

        assert pk is not None, f"invalid pk value '{pk}'"
        assert sk is not None, f"invalid sk value '{sk}'"

        table = self._get_database_table()
        get_command = (
            QueryTableCommand(
                database_table=table,
                index_name=index_name,
            )
            .with_pk(pk)
            .with_sk(sk)
        )
        if predicate:
            get_command = predicate(get_command)
        items = get_command.execute()

        if not items:
            return None

        obj = self._model_class._from_item_dict(items[0])
        return obj

    def all(
        self,
        predicate: QueryPredicate = None,
        index_name: str = None,
        limit: int = 50,
        ascending: bool = False,
        last_evaluated_key: str = None,
        **kwargs,
    ) -> QueryResults:
        """
        Fetch all items matching the pk and sk filter.
        pk must always be exact. sk can be a beginswith filter (e.g. "sk__beginswith")
        """

        key_factory = self._get_key_factory(**kwargs)
        key = key_factory.for_query()
        pk = key.get("pk")
        sk = key.get("sk")

        assert pk is not None, f"invalid pk value '{pk}'"
        assert predicate is None or callable(
            predicate
        ), "predicate must be None or callable"

        table = self._get_database_table()
        query_command = (
            QueryTableCommand(
                database_table=table,
                index_name=index_name,
                max_records=limit,
                scan_forward=ascending,
                last_evaluated_key=last_evaluated_key,
            )
            .with_pk(pk)
            .sk_beginswith(sk)
        )

        if predicate:
            query_command = predicate(query_command)

        results = query_command.execute()
        return QueryResults(results, self._model_class)

    def delete(self, **kwargs):
        """
        Delete an item from the database using config from the attached model class
        """

        assert id is not None, "id must be provided"

        key_factory = self._get_key_factory(**kwargs)
        key = key_factory.for_delete()
        pk = key.get("pk")
        sk = key.get("sk")
        assert pk is not None, f"invalid pk value '{pk}'"
        assert sk is not None, f"invalid sk value '{sk}'"

        table = self._get_database_table()
        table.delete_item(Key=key)
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
class DynamodbItem(ABC, Serializable):
    """
    Base class for all models. Must be subclassed and decorated with @dataclass
    """

    id = IdField()
    items = ItemManager()

    class Meta:
        key = DefaultItemKeyFactory
        dynamo_table = None
        dynamo_table_name = None
        model_name = None
        sk_prefix = None

    def save(self) -> bool:
        if self.id is None:
            res = self.items.create(**self.__dict__)
            self.id = res.id
        else:
            # TODO: We should only update fields that have changed
            self.items.update(**self.__dict__)
        return True

    def delete(self) -> bool:
        return self.items.delete(**self.__dict__)

    def _new_id_(self):
        return f"{datetime.utcnow().timestamp()}-{uuid4()}"

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


def _serialize(obj: dict) -> dict:
    """
    Recursively serialize all fields that are instances of Serializable
    """
    for k, v in obj.items():
        if isinstance(v, Serializable):
            obj[k] = v.to_dict()
        elif isinstance(v, float):
            obj[k] = Decimal(str(v))
    return obj


class ObjectField:
    """
    A descriptor that auto-generates an id for a model
    """

    def __init__(self, type_class):
        assert issubclass(
            type_class, Serializable
        ), "type_class must be subclass of model.Serializable"

        # Assert is dataclass
        assert hasattr(type_class, "__dataclass_fields__"), (
            "type_class must be a dataclass. "
            "Add @dataclass decorator to the class definition."
        )

        self._name = None
        self._type_class = type_class

    def __get__(self, instance, owner):
        if instance is None:
            return None
        value = instance.__dict__.get(self._name)
        if isinstance(value, dict):
            return self._type_class(**value)
        return value

    def __set__(self, instance, value):
        instance.__dict__[self._name] = value

    def __set_name__(self, owner, name):
        self._name = name

    def to_dict(self):
        return self._type_class().to_dict()
