from cloudlydb.core.dynamodb import DictPairAccumulator


def test_can_combine_two_flat_stats():
    original = {"a": 1, "b": 2}
    stats2 = {"c": 3, "d": 4}
    expected = {"c": 3, "d": 4}
    tested = DictPairAccumulator(original)
    actual = tested.add(stats2)

    assert actual == expected


def test_can_combine_two_flat_stats_with_overlapping_keys():
    original = {"a": 1, "b": 2}
    stats2 = {"b": 3, "d": 4}
    expected = {"b": 5, "d": 4}
    tested = DictPairAccumulator(original)
    actual = tested.add(stats2)

    assert actual == expected


def test_can_combine_two_nested_stats():
    original = {"a": {"b": 1, "c": 2}}
    stats2 = {"a": {"b": 3, "d": 4}}
    expected = {"a": {"b": 4, "d": 4}}
    tested = DictPairAccumulator(original)
    actual = tested.add(stats2)

    assert actual == expected


def test_that_the_special_insert_operator_is_used_for_new_keys_with_dict_values():
    original = {"a": 3, "b": 1, "c": 2}
    stats2 = {"a": 1, "b": 3, "d": {"x": 4}}
    expected = {"a": 4, "b": 4, "d:$": {"x": 4}}
    tested = DictPairAccumulator(original)
    actual = tested.add(stats2)

    assert actual == expected


def test_that_we_can_specify_a_path_to_where_we_want_the_accumulation_to_happen():
    original = {"a": {"b": 1, "c": 2}}
    stats2 = {"b": 3, "d": 4}
    expected = {"a": {"b": 4, "d": 4}}
    tested = DictPairAccumulator(original)
    actual = tested.add(stats2, path="a")

    assert actual == expected


def test_add_with_path_to_empty_dict():
    original = {}
    stats2 = {"a": 1, "b": 2}
    expected = {"stats": {"a": 1, "b": 2}}
    tested = DictPairAccumulator(original)
    actual = tested.add(stats2, path="stats")

    assert actual == expected
