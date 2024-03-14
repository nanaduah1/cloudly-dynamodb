from abc import ABC, abstractmethod
import base64
from dataclasses import dataclass
from datetime import datetime, timezone
import os
from typing import Any, Callable, Dict, Iterable, List, Tuple
import boto3
from botocore.exceptions import ClientError

dynamo_exceptions = boto3.client("dynamodb").exceptions
ConditionalCheckFailedException = dynamo_exceptions.ConditionalCheckFailedException
ResourceNotFoundException = dynamo_exceptions.ResourceNotFoundException
InternalServerError = dynamo_exceptions.InternalServerError


@dataclass
class ItemKey(ABC):
    data: dict

    @abstractmethod
    def build(self) -> Dict[str, str]:
        """Returns a key value pair of table keys and values
        Example { 'pk': 'a5s66', 'sk' : '143434', 'GSI_1_pk': '234' }
        """
        pass


class BadItemDefinition(Exception):
    def __init__(self, message):
        explaination = "Check that the pk and sk are are valid and match an existing item. \
            This error can also be caused if you are updating an item with a new \
            map field that do not exist on the item. In this case, you need to \
            use the special :$ notation to replace the entire new map field. \
            Example: {'data': {'new_map_field:$': {'new_field': 'new_value'}}}"
        super().__init__(message + "\n" + explaination)


class ConditionalExecuteMixin:
    class ConditionUnmetError(Exception):
        pass

    def conditional_execute(self, execute_func: Callable, params: dict):
        if self.condition_expression_attr_names:
            assert isinstance(
                self.condition_expression_attr_names, dict
            ), "condition_expression_attr_names must be a dict"

            params["ExpressionAttributeNames"] = params.get(
                "ExpressionAttributeNames", {}
            )
            params["ExpressionAttributeNames"].update(
                self.condition_expression_attr_names
            )

        if self.condition_expression_attr_values:
            assert isinstance(
                self.condition_expression_attr_values, dict
            ), "condition_expression_attr_values must be a dict"

            params["ExpressionAttributeValues"] = params.get(
                "ExpressionAttributeValues", {}
            )
            params["ExpressionAttributeValues"].update(
                self.condition_expression_attr_values
            )

        if getattr(self, "condition_expression", None):
            params["ConditionExpression"] = self.condition_expression

        try:
            return execute_func(**params)
        except ResourceNotFoundException as e:
            raise e
        except ConditionalCheckFailedException as e:
            raise ConditionalExecuteMixin.ConditionUnmetError(
                e.response["Error"]["Message"]
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ValidationException":
                raise BadItemDefinition(e.response["Error"]["Message"])
            raise e


@dataclass(frozen=True)
class PutItemCommand(ConditionalExecuteMixin):
    database_table: Any
    data: dict
    key: dict = None
    data_shaper: Callable[[dict], dict] = None
    condition_expression: str = None
    condition_expression_attr_names: dict = None
    condition_expression_attr_values: dict = None

    def execute(self):
        data = self.data
        keys = self.key

        item = {
            **keys,
            "data": data,
            "created": datetime.now(timezone.utc).isoformat(),
        }
        params = {"Item": item}
        self.conditional_execute(self.database_table.put_item, params)
        return item


@dataclass(frozen=True)
class SetExpression:
    data: dict

    def build(self) -> Tuple[dict, dict, str]:
        attr_names, attr_values, expressions = self._build_for(self.data)
        update_expr = f'SET {", ".join(expressions)}'
        return (attr_names, attr_values, update_expr)

    def _build_for(
        self, obj, parent_field="", field_prefix=""
    ) -> Tuple[dict, dict, list]:
        expressions = []
        attr_names = {}
        attr_values = {}

        for fld, value in obj.items():
            # filed names can have :$ in them to indicate the
            # value should replace the field name instead of
            # updating individual fields
            is_replace = ":$" in fld
            cleaned_field_name = fld.replace(":$", "")
            if is_replace is False and value and isinstance(value, dict):
                response = self._build_for(value, cleaned_field_name, parent_field)
                (inner_attr_names, inner_attr_vals, inner_exprs) = response
                attr_names.update(inner_attr_names)
                attr_values.update(inner_attr_vals)
                expressions.extend(inner_exprs)
                attr_names[f"#{cleaned_field_name}"] = cleaned_field_name
            else:
                prefix = f"#{parent_field}." if parent_field else ""
                prefix = f"#{field_prefix}.{prefix}" if field_prefix else prefix
                field_name = f"{field_prefix}{parent_field}{cleaned_field_name}"
                expressions.append(f"{prefix}#{field_name} = :{field_name}")
                attr_names[f"#{field_name}"] = cleaned_field_name
                attr_values[f":{field_name}"] = value
        return (attr_names, attr_values, expressions)


@dataclass(frozen=True)
class AddExpression:
    data: dict

    def build(self) -> Tuple[dict, dict, str]:
        # TEMP HACK: we have to pop the updatedAt since it has a string value
        # This auto field isn't needed for an accumulator

        self.data.pop("updatedAt", "")
        attr_names, attr_values, expressions = self._build_for(self.data)
        update_expr = f'ADD {", ".join(expressions)}'
        return (attr_names, attr_values, update_expr)

    def _build_for(
        self, obj, parent_field="", field_prefix=""
    ) -> Tuple[dict, dict, list]:
        expressions = []
        attr_names = {}
        attr_values = {}

        for fld, value in obj.items():
            if isinstance(value, dict):
                response = self._build_for(value, fld, parent_field)
                (inner_attr_names, inner_attr_vals, inner_exprs) = response
                attr_names.update(inner_attr_names)
                attr_values.update(inner_attr_vals)
                expressions.extend(inner_exprs)
                attr_names[f"#{fld}"] = fld
            else:
                prefix = f"#{parent_field}." if parent_field else ""
                prefix = f"#{field_prefix}.{prefix}" if field_prefix else prefix
                field_name = f"{field_prefix}{parent_field}{fld}"
                expressions.append(f"{prefix}#{field_name} :{field_name}")
                attr_names[f"#{field_name}"] = fld
                attr_values[f":{field_name}"] = value
        return (attr_names, attr_values, expressions)


@dataclass(frozen=True)
class UpdateItemCommand(ConditionalExecuteMixin):
    database_table: Any
    key: dict
    data: dict
    expression_class: Any = None
    condition_expression: str = None
    condition_expression_attr_names: dict = None
    condition_expression_attr_values: dict = None
    retutrn_values: str = "ALL_NEW"

    def execute(self):
        assert isinstance(self.key, dict), "key must be a dict"
        assert isinstance(self.data, dict), "data must be a dict"

        now = datetime.now(timezone.utc).isoformat()
        item = {"data": self.data, "updatedAt": now}
        ExpressionClass = self.expression_class or SetExpression
        cmd = ExpressionClass(item)
        attr_names, exp_vals, update_expr = cmd.build()

        params = {
            "Key": self.key,
            "ExpressionAttributeNames": attr_names,
            "ExpressionAttributeValues": exp_vals,
            "UpdateExpression": update_expr,
            "ReturnValues": self.retutrn_values,
        }

        return self.conditional_execute(self.database_table.update_item, params)


class KeyEncoder:
    @staticmethod
    def encode(pk: str, sk: str) -> str:
        token = f"{pk}||{sk}"
        b64_token = base64.b64encode(token.encode("utf-8")).decode("utf-8")
        return b64_token

    @staticmethod
    def decode(token: str) -> Tuple[str, str]:
        decoded = base64.b64decode(token.encode("utf-8")).decode("utf-8")
        pk, sk = decoded.split("||")
        return pk, sk


class QueryResults:
    def __init__(self, response: dict):
        self.__items = response.get("Items", [])
        self.__last_evaluated_key = response.get("LastEvaluatedKey")
        self.__next = 0
        self.count = len(self.__items)

    def __iter__(self) -> dict:
        return self

    def __next__(self) -> dict:
        try:
            item = self.__items[self.__next]
            self.__next += 1
            return item
        except IndexError:
            raise StopIteration

    def __getitem__(self, index: int) -> dict:
        return self.__items[index]

    def __len__(self) -> int:
        return len(self.__items)

    def __bool__(self) -> bool:
        return bool(self.__items)

    def __repr__(self) -> str:
        return f"QueryResults({self.__items})"

    def last_evaluated_key(self) -> str:
        if not self.__last_evaluated_key:
            return None

        return KeyEncoder.encode(**self.__last_evaluated_key)


@dataclass(frozen=True)
class QueryTableCommand:
    database_table: Any
    index_name: str = None
    scan_forward: bool = False
    max_records: int = 25
    last_evaluated_key: str = None

    def execute(self) -> QueryResults:
        query_expression, expr_attr_vals = self._build_query()
        query = dict(
            KeyConditionExpression=query_expression,
            ExpressionAttributeValues=expr_attr_vals,
            ScanIndexForward=self.scan_forward,
            Limit=self.max_records,
        )

        if self.index_name:
            query["IndexName"] = self.index_name

        if "projection" in self.__dict__ and isinstance(self.projection, Iterable):
            self._build_projection(query)

        if self.last_evaluated_key:
            pk, sk = KeyEncoder.decode(self.last_evaluated_key)
            query["ExclusiveStartKey"] = {"pk": pk, "sk": sk}

        response = self.database_table.query(**query)
        return QueryResults(response)

    def _build_projection(self, query):
        exp_attr_names = {}
        projection = []
        for fld in self.projection:
            parts = fld.split(".")
            for part in parts:
                exp_attr_names[f"#{part}"] = part
            projection.append(".".join([f"#{part}" for part in parts]))

        query["ProjectionExpression"] = ", ".join(projection)
        query["ExpressionAttributeNames"] = exp_attr_names

    def _build_projection(self, query):
        exp_attr_names = {}
        projection = []
        for fld in self.projection:
            parts = fld.split(".")
            for part in parts:
                exp_attr_names[f"#{part}"] = part
            projection.append(".".join([f"#{part}" for part in parts]))

        query["ProjectionExpression"] = ", ".join(projection)
        query["ExpressionAttributeNames"] = exp_attr_names

    def _build_projection(self, query):
        exp_attr_names = {}
        projection = []
        for fld in self.projection:
            parts = fld.split(".")
            for part in parts:
                exp_attr_names[f"#{part}"] = part
            projection.append(".".join([f"#{part}" for part in parts]))

        query["ProjectionExpression"] = ", ".join(projection)
        query["ExpressionAttributeNames"] = exp_attr_names

    def _update_key(self, **kwargs):
        key = self.__dict__.get("key", {})
        key.update(kwargs)
        self.__dict__["key"] = key

    def with_pk(self, pk: Any, pk_name: str = None):
        self._update_key(pk=pk, pk_name=pk_name)
        return self

    def with_sk(self, sk: Any, sk_name: str = None):
        self._update_key(sk=sk, sk_name=sk_name)
        return self

    def sk_beginswith(self, sk: str, sk_name: str = None):
        self.with_sk(sk, sk_name)
        self._update_key(sk_op="beginswith")
        return self

    def sk_between(self, sk1: str, sk2: str, sk_name: str = None):
        """sk is between sk1 and sk2"""

        # We store the sk as a tuple
        self.with_sk((sk1, sk2), sk_name)
        self._update_key(sk_op="BETWEEN")
        return self

    def sk_gt(self, value: any, sk_name: str = None):
        """sk is greater than value"""

        self.with_sk(value, sk_name)
        self._update_key(sk_op=">")
        return self

    def sk_gte(self, value: any, sk_name: str = None):
        """sk is greater than or equal to value"""

        self.with_sk(value, sk_name)
        self._update_key(sk_op=">=")
        return self

    def sk_lte(self, value: any, sk_name: str = None):
        """sk is less than or equal to value"""

        self.with_sk(value, sk_name)
        self._update_key(sk_op="<=")
        return self

    def sk_lt(self, value: any, sk_name: str = None):
        """sk is less than value"""

        self.with_sk(value, sk_name)
        self._update_key(sk_op="<")
        return self

    def only(self, *fields: List[str]):
        """Only return the fields specified in the list"""

        self.__dict__["projection"] = set(fields)
        return self

    def _build_query(self):
        key = self.__dict__.get("key", {})
        pk = key.get("pk")
        sk = key.get("sk")
        attr_vals = {":sk": sk, ":pk": pk}

        pk_name = key.get("pk_name") if key.get("pk_name") else "pk"
        pk_expr = f"{pk_name} = :pk"

        sk_name = key.get("sk_name") if key.get("sk_name") else "sk"
        sk_expr = f"{sk_name} = :sk"

        # Construct sk expression
        sk_op = key.get("sk_op")
        if sk_op == "beginswith":
            sk_expr = f"begins_with({sk_name}, :sk)"
        elif sk_op == "BETWEEN":
            sk1, sk2 = sk

            sk_expr = f"{sk_name} BETWEEN :sk1 AND :sk2"
            attr_vals = {":sk1": sk1, ":sk2": sk2, ":pk": pk}
        elif sk_op:
            sk_expr = f"{sk_name} {sk_op} :sk"

        query = f"{pk_expr} AND {sk_expr}"

        return query, attr_vals


class Table:
    @staticmethod
    def from_name(table_name: str):
        """
        Returns a Table object from table name.
        You can set DYANMODB_ENDPOINT_URL environment variable
        to point to a local dynamodb instance
        """
        import boto3

        endpoint_url = os.getenv("DYANMODB_ENDPOINT_URL")
        client = boto3.resource("dynamodb", endpoint_url=endpoint_url)
        return client.Table(table_name)

    @staticmethod
    def from_env(env_var: str):
        """
        Returns a Table object from an environment variable
        You can set DYANMODB_ENDPOINT_URL environment variable
        to point to a local dynamodb instance
        """
        return Table.from_name(os.getenv(env_var))


@dataclass
class AccumulateCommand:
    """
    The accumulation is done in memory and then written to the database
    as a single update command.

    WARNING: This is not thread safe and should only be used in a single
    threaded environment.
    """

    database_table: Any

    def write_stat(self, key: dict, data: dict, path: str = None):
        results = self._get_current_record(key)
        if results is None:
            # We are free to insert a new record
            return self._insert_new_stats(key, data, path)

        current_record = results.get("data", {})
        return self._updated_record(current_record, key, data, path)

    def _insert_new_stats(self, key: dict, data: dict, path: str = None):
        data["timestamp"] = datetime.now(timezone.utc).isoformat()
        if path:
            data = {path: data}
        put_command = PutItemCommand(
            database_table=self.database_table, data=data, key=key
        )
        put_command.execute()
        return data

    def _updated_record(self, current_record, key: dict, data: dict, path: str = None):
        merged_data = DictPairAccumulator(current_record).add(data, path)
        put_command = UpdateItemCommand(
            database_table=self.database_table, data=merged_data, key=key
        )
        results = put_command.execute()
        return results.get("Attributes", {}).get("data", {})

    def _get_current_record(self, key: dict):
        try:
            return self.database_table.get_item(Key=key).get("Item")
        except ResourceNotFoundException:
            return None


@dataclass
class DictPairAccumulator:
    """
    Adds stats values from data to current_record.
    The final results contains only keys from  new_data
    such that the values are the sum of the values in both.
    Fields in new_data that are not in current_record are added.

    IMPORTANT: Values are only numeric or dicts

    Example:
        current_record = {'data':{ stats: {'a': 1, 'b': 2}}}
        data = {'a': 2, 'c': 3}
        path = 'stats'
        result = {'data':{ stats: {'a': 3, 'c': 3}}}
    """

    original: dict

    def add(self, data: dict, path: str = None):
        current_stats = self.original
        if path:
            data = {path: data}

        # No need to merge if we don't have any an original
        if not self.original:
            return data

        results = {}
        stack = [(current_stats, data, results)]
        while stack:
            original, new_value, state = stack.pop()
            for key, value in new_value.items():
                if key not in original:
                    # If the value is a dict, we need to create it
                    # using the special :$ notation to indicate that
                    # the value is a dict and should replace the entire value
                    if isinstance(value, dict):
                        state[key + ":$"] = value
                    else:
                        state[key] = value
                    continue

                if not isinstance(value, dict):
                    state[key] = value + original[key]
                    continue

                state[key] = {}
                stack.append((original[key], value, state[key]))

        return results
