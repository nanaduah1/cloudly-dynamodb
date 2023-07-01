from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
import os
from typing import Any, Callable, Dict, List, Tuple

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

        for field, value in obj.items():
            if isinstance(value, dict):
                response = self._build_for(value, field, parent_field)
                (inner_attr_names, inner_attr_vals, inner_exprs) = response
                attr_names.update(inner_attr_names)
                attr_values.update(inner_attr_vals)
                expressions.extend(inner_exprs)
                attr_names[f"#{field}"] = field
            else:
                prefix = f"#{parent_field}." if parent_field else ""
                prefix = f"#{field_prefix}.{prefix}" if field_prefix else prefix
                field_name = f"{field_prefix}{parent_field}{field}"
                expressions.append(f"{prefix}#{field_name} = :{field_name}")
                attr_names[f"#{field_name}"] = field
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

        for field, value in obj.items():
            if isinstance(value, dict):
                response = self._build_for(value, field, parent_field)
                (inner_attr_names, inner_attr_vals, inner_exprs) = response
                attr_names.update(inner_attr_names)
                attr_values.update(inner_attr_vals)
                expressions.extend(inner_exprs)
                attr_names[f"#{field}"] = field
            else:
                prefix = f"#{parent_field}." if parent_field else ""
                prefix = f"#{field_prefix}.{prefix}" if field_prefix else prefix
                field_name = f"{field_prefix}{parent_field}{field}"
                expressions.append(f"{prefix}#{field_name} :{field_name}")
                attr_names[f"#{field_name}"] = field
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


@dataclass(frozen=True)
class QueryTableCommand:
    database_table: Any
    index_name: str = None
    scan_forward: bool = False
    max_records: int = 25
    key: dict = field(default_factory=dict)

    def execute(self) -> List[dict]:
        query_expression, expr_attr_vals = self._build_query()
        query = dict(
            KeyConditionExpression=query_expression,
            ExpressionAttributeValues=expr_attr_vals,
            ScanIndexForward=self.scan_forward,
            Limit=self.max_records,
        )

        if self.index_name:
            query["IndexName"] = self.index_name

        response = self.database_table.query(**query)

        return response.get("Items", [])

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

    def _build_query(self):
        key = self.key
        pk = key.get("pk")
        sk = key.get("sk")

        pk_name = key.get("pk_name") if key.get("pk_name") else "pk"
        pk_expr = f"{pk_name} = :pk"

        sk_name = key.get("sk_name") if key.get("sk_name") else "sk"
        sk_expr = f"{sk_name} = :sk"
        sk_op = key.get("sk_op")
        if sk_op == "beginswith":
            sk_expr = f"begins_with({sk_name}, :sk)"

        query = f"{pk_expr} AND {sk_expr}"
        attr_vals = {":sk": sk, ":pk": pk}

        return query, attr_vals


class Table:
    @staticmethod
    def from_name(table_name: str):
        import boto3

        return boto3.resource("dynamodb").Table(table_name)

    @staticmethod
    def from_env(env_var: str):
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
