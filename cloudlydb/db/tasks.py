from dataclasses import dataclass
from typing import Any, Callable

from cloudlydb.db.dynamodb import (
    ItemKey,
    PutItemCommand,
    QueryTableCommand,
    UpdateItemCommand,
)


@dataclass
class PutItem:
    key_class: ItemKey
    database_table: Any
    data_shaper: Callable[[dict], dict] = None

    def process(self, input: Any) -> Any:
        data = {**input}
        data.pop("_request", "")
        put_cmd = PutItemCommand(
            data=data,
            key_class=self.key_class,
            database_table=self.database_table,
            data_shaper=self.data_shaper,
        )
        return put_cmd.execute()


@dataclass
class UpdateItem:
    database_table: Any
    key: dict

    def process(self, input: Any) -> Any:
        data = {**input}
        data.pop("_request", "")
        update_cmd = UpdateItemCommand(
            data=data,
            key=self.key,
            database_table=self.database_table,
        )
        update_cmd.execute()
        return {**data}


@dataclass
class QueryItems:
    database_table: Any
    query: Callable[[QueryTableCommand], QueryTableCommand]

    def process(self, input: Any) -> Any:
        query_cmd = QueryTableCommand(self.database_table)
        return self.query(query_cmd).execute()
