from decimal import Decimal
from cloudlydb.core.dynamodb import AccumulateCommand


def test_accumulate_command(db_table, put_item, get_item):
    item_key = {"pk": "test#123", "sk": "id=1234"}
    put_item({**item_key, "data": {"age": 10, "boys": 12}})
    tested = AccumulateCommand(database_table=db_table)
    tested.write_stat(item_key, {"boys": 1, "girls": 2})
    expected = {"age": Decimal(10), "boys": Decimal(13), "girls": Decimal(2)}

    updated_item = get_item(**item_key).get("Item").get("data")
    updated_item.pop("timestamp", None)
    assert updated_item == expected


def test_can_write_to_a_target_field(db_table, get_item, put_item):
    item_key = {"pk": "test#1234", "sk": "id=1234"}
    put_item({**item_key, "data": {"age": 10, "boys": 12}})
    tested = AccumulateCommand(database_table=db_table)
    tested.write_stat(item_key, {"boys": 1, "girls": 2}, path="stats")
    tested.write_stat(item_key, {"boys": 1, "girls": 2}, path="stats")
    tested.write_stat(item_key, {"boys": 1, "girls": 2}, path="stats")

    updated_item = get_item(**item_key).get("Item")
    assert updated_item["data"]["age"] == 10
    assert updated_item["data"]["stats"]["boys"] == 3
    assert updated_item["data"]["stats"]["girls"] == 6


def test_can_write_to_a_new_record(db_table, get_item, put_item):
    item_key = {"pk": "test#1235", "sk": "id=1234"}
    tested = AccumulateCommand(database_table=db_table)
    tested.write_stat(item_key, {"boys": 1, "girls": 2}, path="stats")
    tested.write_stat(item_key, {"boys": 1, "girls": 2}, path="stats")
    tested.write_stat(item_key, {"boys": 1, "girls": 2}, path="stats")

    updated_item = get_item(**item_key).get("Item")
    assert updated_item["data"]["stats"]["boys"] == 3
    assert updated_item["data"]["stats"]["girls"] == 6


def test_can_write_nested_stats(db_table, get_item, put_item):
    key = {"sk": "ANALYTICS#123", "pk": "ANALYICS#entity=PAYMENT"}
    stats = {
        "allTime": {"total": 100, "count": 10},
        "d20231202": {"total": 100, "count": 10},
    }

    tested = AccumulateCommand(database_table=db_table)
    tested.write_stat(key, stats.copy())
    tested.write_stat(key, stats.copy())

    updated_item = get_item(**key).get("Item")
    assert updated_item["data"]["allTime"]["total"] == 200
    assert updated_item["data"]["allTime"]["count"] == 20
    assert updated_item["data"]["d20231202"]["total"] == 200
    assert updated_item["data"]["d20231202"]["count"] == 20
