import polars as pl
import random
import string
from polars_graphframes import get_cluster_ids


def test_simple_test_case() -> None:
    df = pl.DataFrame(
        {
            'user': [0, 0, 1, 1, 2, 2, 3],
            'phone': [123, 123, 123, 123, 222, 222, 333],
            'email': ['aaa', 'aaa', 'bbb', 'bbb', 'aaa', 'aaa', 'ccc']
        }
    )

    result = df.with_columns(
        cluster_id=get_cluster_ids(
            node_definition='user',
            edge_definitions=['phone', 'email'],
        )
    )

    print(result)
    assert result.select('cluster_id').equals(pl.DataFrame({'cluster_id' : [0] * 6 + [1]}))


def test_get_cluster_ids_one_cc() -> None:
    num_rows = 100_000

    user_ids = [random.randint(0, 5_000) for _ in range(num_rows)]
    phone_numbers = [random.randint(0, 10_000) for _ in range(num_rows)]
    emails = [f"user{random.randint(0, 10_000)}@example.com" for _ in range(num_rows)]
    additional_col1 = [random.randint(0, 0) for _ in range(num_rows)]
    additional_col2 = [random.choice(['A', 'B', 'C', 'D', 'E']) for _ in range(num_rows)]

    df = pl.DataFrame(
        {
            'user': user_ids,
            'phone': phone_numbers,
            'email': emails,
            'additional_col1': additional_col1,
            'additional_col2': additional_col2,
        }
    )

    result = df.with_columns(
        cluster_id=get_cluster_ids(
            node_definition='user',
            edge_definitions=['phone', 'email', 'additional_col1', 'additional_col2'],
        )
    )

    print(result.head(5))
    assert result.select('cluster_id').equals(pl.DataFrame({'cluster_id' : [0] * num_rows}))


def test_get_cluster_ids_nine_ccs() -> None:
    num_rows = 100_000

    user_ids = [random.randint(1, 20_000) for _ in range(num_rows)]
    df = pl.DataFrame({'user': user_ids}).with_columns(pl.col('user').cast(pl.Utf8).str.head(1).alias('phone')).sort('phone')

    result = df.with_columns(
        cluster_id=get_cluster_ids(
            node_definition='user',
            edge_definitions=['phone'],
        )
    )

    print(result.head(5))
    assert result.select('cluster_id').equals(result.select(pl.col('user').cast(pl.Utf8).str.head(1).cast(pl.UInt64).sub(1).alias('cluster_id')))


def test_get_cluster_ids_all_isolated() -> None:
    num_rows = 100_000

    user_ids = list(range(num_rows))
    phone_numbers = list(range(num_rows))
    emails = list(range(num_rows))
    additional_col1 = list(range(num_rows))
    additional_col2 = list(range(num_rows))

    df = pl.DataFrame(
        {
            'user': user_ids,
            'phone': phone_numbers,
            'email': emails,
            'additional_col1': additional_col1,
            'additional_col2': additional_col2,
        }
    )

    result = df.with_columns(
        cluster_id=get_cluster_ids(
            node_definition='user',
            edge_definitions=['phone', 'email', 'additional_col1', 'additional_col2'],
        )
    )

    print(result.head(5))
    assert result.select('cluster_id').equals(pl.DataFrame({'cluster_id' : list(range(num_rows))}))
