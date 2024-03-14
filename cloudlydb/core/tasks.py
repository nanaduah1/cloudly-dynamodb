import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from cloudlydb.core.dynamodb import (
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

        now = datetime.now(timezone.utc)
        if "id" not in data or not data["id"]:
            data["id"] = f"{now.timestamp()}-{uuid.uuid4()}"

        if "timestamp" not in data or not data["timestamp"]:
            data["timestamp"] = now.isoformat()

        if self.data_shaper and callable(self.data_shaper):
            data = self.data_shaper(data)

        key = self.key_class(data).build()

        # IMPORTANT: exclude the request metadata
        data.pop("_request", "")
        put_cmd = PutItemCommand(
            data=data,
            key=key,
            database_table=self.database_table,
            data_shaper=self.data_shaper,
        )
        return put_cmd.execute()


@dataclass
class UpdateItem:
    database_table: Any
    key_factory: Callable[[Any], dict]

    def process(self, input: Any) -> Any:
        data = {**input}
        key = self.key_factory(data)

        # IMPORTANT: exclude the request metadata
        data.pop("_request", "")
        update_cmd = UpdateItemCommand(
            key=key,
            data=data,
            database_table=self.database_table,
        )
        update_cmd.execute()
        return {**data}


@dataclass
class QueryItems:
    database_table: Any
    query: Callable[[Any, QueryTableCommand], QueryTableCommand]
    index_name: str = None
    scan_forward: bool = False
    max_records: int = 25

    def process(self, input: Any):
        query_cmd = QueryTableCommand(
            self.database_table,
            index_name=self.index_name,
            scan_forward=self.scan_forward,
            max_records=self.max_records,
        )
        return self.query(input, query_cmd).execute()
