from dataclasses import dataclass
from cloudlydb.core.dynamodb import (
    SetExpression,
    UpdateItemCommand,
    ConditionalExecuteMixin,
    PutItemCommand,
)
import pytest


@dataclass
class FakeDb:
    data: dict = None

    def update_item(self, **kwargs):
        self.data = kwargs
        return {}


from cloudlydb.core.dynamodb import SetExpression


def test_update_simple_object():
    tested = SetExpression({"name": "nana", "age": 10})
    exp_names, exp_vals, update_expr = tested.build()

    assert exp_names == {"#name": "name", "#age": "age"}
    assert exp_vals == {":name": "nana", ":age": 10}
    assert update_expr == "SET #name = :name, #age = :age"


def test_update_object_with_inner_map():
    tested = SetExpression({"name": "nana", "car": {"num": 10, "vin": "3232"}})
    exp_names, exp_vals, update_expr = tested.build()

    assert exp_names == {
        "#name": "name",
        "#carnum": "num",
        "#carvin": "vin",
        "#car": "car",
    }
    assert exp_vals == {":name": "nana", ":carnum": 10, ":carvin": "3232"}
    assert (
        update_expr
        == "SET #name = :name, #car.#carnum = :carnum, #car.#carvin = :carvin"
    )


def test_update_object_with_inner_inner_map():
    tested = SetExpression(
        {
            "name": "nana",
            "car": {"num": 10, "vin": "3232", "own": {"name": "abu", "age": 22}},
        }
    )
    exp_names, exp_vals, update_expr = tested.build()
    assert exp_names == {
        "#name": "name",
        "#carnum": "num",
        "#carvin": "vin",
        "#car": "car",
        "#carownage": "age",
        "#carownname": "name",
        "#own": "own",
    }
    assert exp_vals == {
        ":name": "nana",
        ":carnum": 10,
        ":carvin": "3232",
        ":carownname": "abu",
        ":carownage": 22,
    }
    assert (
        update_expr
        == "SET #name = :name, #car.#carnum = :carnum, #car.#carvin = :carvin, #car.#own.#carownname = :carownname, #car.#own.#carownage = :carownage"
    )


def test_update_command_with_condition_umet_condition(put_item, db_table):
    put_item({"pk": "1234", "sk": "4567", "data": {"name": "nana", "age": 10}})

    tested = UpdateItemCommand(
        database_table=db_table,
        key={"pk": "1234", "sk": "4567"},
        data={"name": "nana", "age": 10},
        condition_expression="attribute_exists(#d.#agent.#name)",
        condition_expression_attr_names={
            "#d": "data",
            "#agent": "agent",
            "#name": "name",
        },
    )

    with pytest.raises(ConditionalExecuteMixin.ConditionUnmetError):
        response = tested.execute()
        assert response is not None
        print(response)


def test_update_command_with_condition_met_condition(put_item, db_table):
    put_item({"pk": "1234", "sk": "4567", "data": {"name": "nana", "age": 10}})

    tested = UpdateItemCommand(
        database_table=db_table,
        key={"pk": "1234", "sk": "4567"},
        data={"name": "nana", "age": 10},
        condition_expression="attribute_exists(#d.#name)",
        condition_expression_attr_names={
            "#d": "data",
            "#name": "name",
        },
    )

    response = tested.execute()
    assert response is not None
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_put_item_command_with_condition_unmet(put_item, db_table):
    put_item({"pk": "1234", "sk": "4567", "data": {"name": "nana", "age": 10}})

    tested = PutItemCommand(
        database_table=db_table,
        key={"pk": "1234", "sk": "4567"},
        data={"name": "nana", "age": 10},
        condition_expression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
    )

    with pytest.raises(ConditionalExecuteMixin.ConditionUnmetError):
        response = tested.execute()
        assert response is not None
        print(response)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_put_item_with_met_condition(db_table):
    tested = PutItemCommand(
        database_table=db_table,
        key={"pk": "3333", "sk": "4444567"},
        data={"name": "nana", "age": 10},
        condition_expression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
    )

    response = tested.execute()
    assert response is not None


def test_can_do_partial_update(db_table, put_item):
    put_item({"pk": "3333", "sk": "4444567", "data": {"name": "nana", "age": 10}})

    tested = UpdateItemCommand(
        database_table=db_table,
        key={"pk": "3333", "sk": "4444567"},
        data={"agentId": "nana", "agentCode": "212121"},
    )

    response = tested.execute()
    assert response is not None
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_can_do_partial_update_with_specific_replace(db_table, put_item):
    put_item({"pk": "3333", "sk": "4444567", "data": {"name": "nana", "age": 10}})

    tested = UpdateItemCommand(
        database_table=db_table,
        key={"pk": "3333", "sk": "4444567"},
        data={"agentId": "nana", "agentCode": "212121", "billing:$": {"code": "1234"}},
    )

    response = tested.execute()
    assert response is not None
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
