from abc import ABC
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

    def get_database_table(self):
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

        key = self._model_class.get_key(id=item_id, **kwargs)
        pk = key.get("pk")
        sk = key.get("sk")

        assert pk is not None, f"invalid pk value '{pk}'"
        assert sk is not None, f"invalid sk value '{sk}'"

        fully_serialized_data = self._serialize(cleaned_data)
        table = self.get_database_table()
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

    def update(self, id: str, **kwargs):
        """
        Update an existing item in the database using config from the attached model class
        """

        assert id is not None, "id must be provided"

        key = self._model_class.get_key(id=id, **kwargs)
        pk = key.get("pk")
        sk = key.get("sk")

        assert pk is not None, f"invalid pk value '{pk}'"
        assert sk is not None, f"invalid sk value '{sk}'"

        cleaned_data = self.__validate_data(**kwargs)
        fully_serialized_data = self._serialize(cleaned_data)
        table = self.get_database_table()
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

        key = self._model_class.get_key(**kwargs)
        pk = key.get("pk")
        sk = key.get("sk")
        assert pk is not None, f"invalid pk value '{pk}'"
        assert sk is not None, f"invalid sk value '{sk}'"

        table = self.get_database_table()
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

        pk = self._model_class._create_pk(**kwargs)
        assert pk is not None, f"invalid pk value '{pk}'"
        assert predicate is None or callable(
            predicate
        ), "predicate must be None or callable"

        table = self.get_database_table()
        query_command = QueryTableCommand(
            database_table=table,
            index_name=index_name,
            max_records=limit,
            scan_forward=ascending,
            last_evaluated_key=last_evaluated_key,
        )

        if predicate:
            query_command = predicate(query_command)
        else:
            # If no predicate is provided, use the sk prefix to filter
            sk_prefix = (
                self._model_class.Meta.__dict__.get("sk_prefix")
                or self._model_class.__name__
            )
            query_command = query_command.sk_beginswith(sk_prefix)

        results = query_command.with_pk(pk).execute()
        return QueryResults(results, self._model_class)

    def delete(self, **kwargs):
        """
        Delete an item from the database using config from the attached model class
        """

        assert id is not None, "id must be provided"

        key = self._model_class.get_key(**kwargs)
        pk = key.get("pk")
        sk = key.get("sk")
        assert pk is not None, f"invalid pk value '{pk}'"
        assert sk is not None, f"invalid sk value '{sk}'"

        table = self.get_database_table()
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
class DynamodbItem(ABC):
    """
    Base class for all models. Must be subclassed and decorated with @dataclass
    """

    id = IdField()
    items = ItemManager()

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

    @classmethod
    def get_key(cls, **kwargs):
        pk = cls._create_pk(**kwargs)
        sk = cls._create_sk(**kwargs)
        return dict(pk=pk, sk=sk)

    @classmethod
    def _create_pk(cls, **kwargs):
        discriminator = getattr(cls.Meta, "model_name", None)
        if discriminator is None:
            discriminator = _fully_qualified_name(cls)

        return discriminator

    @classmethod
    def _create_sk(cls, **kwargs):
        id = kwargs.get("id")
        sk = f"{cls.__name__}#{id}"
        return sk

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

    class Meta:
        dynamo_table = None
        dynamo_table_name = None
        model_name = None
        sk_prefix = None


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


class Serializable:
    def to_dict(self):
        return _serialize(self.__dict__)


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
            return self
        value = instance.__dict__.get(self._name)
        if isinstance(value, dict):
            return self._type_class(**value)
        return value

    def __set__(self, instance, value):
        instance.__dict__[self._name] = value

    def __set_name__(self, owner, name):
        self._name = name
