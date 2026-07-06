from opensourcelaw.definitions import defs, example_asset


def test_definitions_load() -> None:
    assert defs is not None
    assert example_asset() == 1
