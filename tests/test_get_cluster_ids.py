import polars as pl
import random
import string
from polars_graphframes import get_cluster_ids


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
            node_definition=pl.col('user').hash(),
            edge_definitions=[pl.col('phone').hash(), pl.col('email').hash(), pl.col('additional_col1').hash(), pl.col('additional_col2').hash()],
        )
    )

    print(result.head(5))
    assert result.select('cluster_id').equals(pl.DataFrame({'cluster_id' : [0] * num_rows}))

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
            node_definition=pl.col('user').hash(),
            edge_definitions=[pl.col('phone').hash(), pl.col('email').hash(), pl.col('additional_col1').hash(), pl.col('additional_col2').hash()],
        )
    )

    print(result.head(5))
    assert result.select('cluster_id').equals(pl.DataFrame({'cluster_id' : list(range(num_rows))}))

if __name__ == '__main__':
    test_get_cluster_ids_one_cc()
    test_get_cluster_ids_all_isolated()
    print('Successfully tested!')