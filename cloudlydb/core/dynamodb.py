from abc import ABC, abstractmethod
import base64
from dataclasses import dataclass, field
from datetime import datetime
import os
from typing import Any, Callable, Dict, Iterable, List, Tuple

from botocore.exceptions import ClientError


@dataclass
class ItemKey(ABC):
    data: dict

    @abstractmethod
    def build(self) -> Dict[str, str]:
        """Returns a key value pair of table keys and values
        Example { 'pk': 'a5s66', 'sk' : '143434', 'GSI_1_pk': '234' }
        """
        pass


@dataclass(frozen=True)
class PutItemCommand:
    database_table: Any
    data: dict
    key: dict = None
    data_shaper: Callable[[dict], dict] = None

    def execute(self):
        data = self.data
        keys = self.key

        item = {
            **keys,
            "data": data,
            "created": datetime.utcnow().isoformat(),
        }
        self.database_table.put_item(Item=item)
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
            if value and isinstance(value, dict):
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
                expressions.append(f"{prefix}#{field_name} = :{field_name}")
                attr_names[f"#{field_name}"] = fld
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
class UpdateItemCommand:
    database_table: Any
    key: dict
    data: dict
    expression_class: Any = None

    def execute(self):
        now = datetime.utcnow().isoformat()
        item = {"data": self.data, "updatedAt": now}
        ExpressionClass = self.expression_class or SetExpression
        cmd = ExpressionClass(item)
        attr_names, exp_vals, update_expr = cmd.build()
        self.database_table.update_item(
            Key=self.key,
            ExpressionAttributeNames=attr_names,
            ExpressionAttributeValues=exp_vals,
            UpdateExpression=update_expr,
        )


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
    key: dict = field(default_factory=dict)
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

        if self.last_evaluated_key:
            pk, sk = KeyEncoder.decode(self.last_evaluated_key)
            query["ExclusiveStartKey"] = {"pk": pk, "sk": sk}

        if "projection" in self.__dict__ and isinstance(self.projection, Iterable):
            self._build_projection(query)

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

    def with_pk(self, pk: Any, pk_name: str = None):
        self.key["pk"] = pk
        self.key["pk_name"] = pk_name
        return self

    def with_sk(self, sk: Any, sk_name: str = None):
        self.key["sk"] = sk
        self.key["sk_name"] = sk_name
        return self

    def sk_beginswith(self, sk: str, sk_name: str = None):
        self.with_sk(sk, sk_name)
        self.key["sk_op"] = "beginswith"
        return self

    def sk_between(self, sk1: str, sk2: str, sk_name: str = None):
        """sk is between sk1 and sk2"""

        # We store the sk as a tuple
        self.with_sk((sk1, sk2), sk_name)
        self.key["sk_op"] = "BETWEEN"
        return self

    def sk_gt(self, value: any, sk_name: str = None):
        """sk is greater than value"""

        self.with_sk(value, sk_name)
        self.key["sk_op"] = ">"
        return self

    def sk_gte(self, value: any, sk_name: str = None):
        """sk is greater than or equal to value"""

        self.with_sk(value, sk_name)
        self.key["sk_op"] = ">="
        return self

    def sk_lte(self, value: any, sk_name: str = None):
        """sk is less than or equal to value"""

        self.with_sk(value, sk_name)
        self.key["sk_op"] = "<="
        return self

    def sk_lt(self, value: any, sk_name: str = None):
        """sk is less than value"""

        self.with_sk(value, sk_name)
        self.key["sk_op"] = "<"
        return self

    def only(self, *fields: List[str]):
        """Only return the fields specified in the list"""

        self.__dict__["projection"] = set(fields)
        return self

    def _build_query(self):
        key = self.key
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
    database_table: Any

    def write_stat(self, key: dict, data: dict):
        try:
            update_command = UpdateItemCommand(
                self.database_table,
                key=key,
                data=data,
                expression_class=AddExpression,
            )
            update_command.execute()
        except ClientError:
            data["timestamp"] = datetime.utcnow().isoformat()
            put_command = PutItemCommand(self.database_table, data, key)
            put_command.execute()
