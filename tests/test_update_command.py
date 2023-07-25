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
