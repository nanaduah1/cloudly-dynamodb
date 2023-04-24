from dataclasses import dataclass
from cloudlydb.db.dynamodb import QueryTableCommand


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
    assert isinstance(response, list)

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
    assert isinstance(response, list)

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
    assert isinstance(response, list)

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
    assert isinstance(response, list)

    query_expr = db.data.get("KeyConditionExpression")
    assert query_expr == "pk = :pk AND begins_with(sk, :sk)"
    assert db.data.get("ExpressionAttributeValues") == {":sk": "4567", ":pk": "1234"}
    assert db.data.get("ScanIndexForward") is False
