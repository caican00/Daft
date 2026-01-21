import daft
from daft import col, lit


def test_dataframe_select_default_column_alias_binary_op() -> None:
    df = daft.from_pydict({"a": [1, 2, 3]})

    res = df.select(col("a") + 1).to_pydict()
    assert res == {"(a + 1)": [2, 3, 4]}


def test_dataframe_select_default_column_alias_string_literal() -> None:
    df = daft.from_pydict({"a": [1]})

    res = df.select(lit("a")).to_pydict()
    assert res == {'"a"': ["a"]}


def test_dataframe_select_default_column_alias_fallback_does_not_error() -> None:
    df = daft.from_pydict({"a": [1, 2, 3]})

    # Use an expression that we may not have explicitly normalized yet.
    res = df.select(col("a").is_null()).to_pydict()

    assert len(res) == 1
    assert res[next(iter(res))] == [False, False, False]
