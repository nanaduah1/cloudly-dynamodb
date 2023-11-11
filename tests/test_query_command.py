from dataclasses import dataclass
from cloudlydb.core.dynamodb import QueryTableCommand, QueryResults


@dataclass
class FakeDb:
    data: dict = None

    def query(self, **kwargs):
        self.data = kwargs
        return {"Items": []}


def test_query_exact():
    db = FakeDb()
    tested = QueryTableCommand(database_table=db)
    tested.with_pk("1234").with_sk("4567")
    response = tested.execute()

    assert response is not None
    assert isinstance(response, QueryResults)

    query_expr = db.data.get("KeyConditionExpression")
    assert query_expr == "pk = :pk AND sk = :sk"
    assert db.data.get("ExpressionAttributeValues") == {":sk": "4567", ":pk": "1234"}
    assert db.data.get("ScanIndexForward") is False


def test_query_exact_scan_forward():
    db = FakeDb()
    tested = QueryTableCommand(database_table=db, scan_forward=True)
    tested.with_pk("1234").with_sk("4567")
    response = tested.execute()

    assert response is not None
    assert isinstance(response, QueryResults)

    query_expr = db.data.get("KeyConditionExpression")
    assert query_expr == "pk = :pk AND sk = :sk"
    assert db.data.get("ExpressionAttributeValues") == {":sk": "4567", ":pk": "1234"}
    assert db.data.get("ScanIndexForward") is True


def test_query_custom_pk_sk():
    db = FakeDb()
    tested = QueryTableCommand(database_table=db)
    tested.with_pk("1234", "sid").with_sk("4567", "cid")
    response = tested.execute()

    assert response is not None
    assert isinstance(response, QueryResults)

    query_expr = db.data.get("KeyConditionExpression")
    assert query_expr == "sid = :pk AND cid = :sk"
    assert db.data.get("ExpressionAttributeValues") == {":sk": "4567", ":pk": "1234"}
    assert db.data.get("ScanIndexForward") is False


def test_query_beginswith():
    db = FakeDb()
    tested = QueryTableCommand(database_table=db)
    tested.with_pk("1234").sk_beginswith("4567")
    response = tested.execute()

    assert response is not None
    assert isinstance(response, QueryResults)

    query_expr = db.data.get("KeyConditionExpression")
    assert query_expr == "pk = :pk AND begins_with(sk, :sk)"
    assert db.data.get("ExpressionAttributeValues") == {":sk": "4567", ":pk": "1234"}
    assert db.data.get("ScanIndexForward") is False


def test_query_with_only_select_fields():
    db = FakeDb()
    tested = QueryTableCommand(database_table=db)
    tested.with_pk("1234").with_sk("4567").only(
        "field1", "data.field2", "data.field3.xyz"
    )
    response = tested.execute()

    assert response is not None
    assert isinstance(response, list)

    query_expr = db.data.get("KeyConditionExpression")
    assert query_expr == "pk = :pk AND sk = :sk"
    assert db.data.get("ExpressionAttributeValues") == {":sk": "4567", ":pk": "1234"}
    assert db.data.get("ScanIndexForward") is False
    assert "#data.#field2" in db.data.get("ProjectionExpression")
    assert "#field1" in db.data.get("ProjectionExpression")
    assert "#data.#field3.#xyz" in db.data.get("ProjectionExpression")

    assert db.data.get("ExpressionAttributeNames") == {
        "#data": "data",
        "#field2": "field2",
        "#field1": "field1",
        "#field3": "field3",
        "#xyz": "xyz",
    }


def test_only_select_fields_returned(db_table, put_item):
    put_item(
        {"pk": "1234", "sk": "4567", "data": {"name": "Kay", "age": 10, "car": "BMW"}}
    )

    tested = (
        QueryTableCommand(database_table=db_table)
        .with_pk("1234")
        .sk_beginswith("45")
        .only("data.name", "data.car")
    )

    response = tested.execute()
    assert response is not None
    assert isinstance(response, list)
    assert len(response) == 1
    assert response[0] == {"data": {"name": "Kay", "car": "BMW"}}
