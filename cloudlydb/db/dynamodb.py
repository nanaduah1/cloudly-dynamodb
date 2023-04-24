from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Tuple, Type
import uuid


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
    key_class: Type[ItemKey]
    data_shaper: Callable[[dict], dict] = None

    def execute(self):
        data = self.data

        if self.data_shaper and callable(self.data_shaper):
            data = self.data_shaper(data)

        now = datetime.utcnow()
        if not "id" in data or not data["id"]:
            data["id"] = f"{now.timestamp()}-{uuid.uuid4()}"

        if not "timestamp" in data or not data["timestamp"]:
            data["timestamp"] = now.isoformat()

        keys = self.key_class(self.data).build()

        item = {
            **keys,
            "data": data,
            "created": data["timestamp"],
        }
        self.database_table.put_item(Item=item)
        return item


@dataclass(frozen=True)
class SimpleUpdateExpression:
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
class UpdateItemCommand:
    database_table: Any
    key: dict
    data: dict

    def execute(self):
        now = datetime.utcnow().isoformat()
        item = {"data": self.data, "updatedAt": now}
        attr_names, exp_vals, update_expr = SimpleUpdateExpression(item).build()
        self.database_table.update_item(
            key=self.key,
            ExpressionAttributeNames=attr_names,
            ExpressionAttributeValues=exp_vals,
            UpdateExpression=update_expr,
        )


@dataclass(frozen=True)
class QueryTableCommand:
    database_table: Any
    key: dict = field(default_factory=dict)
    index_name: str = None
    scan_forward: bool = False

    def execute(self) -> List[dict]:
        query_expression, expr_attr_vals = self._build_query()
        response = self.database_table.query(
            KeyConditionExpression=query_expression,
            ExpressionAttributeValues=expr_attr_vals,
            ScanIndexForward=self.scan_forward,
        )

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
