import polars as pl
from polars_graphframes import get_cluster_ids

pl.Config.set_verbose(True)

df = pl.DataFrame(
    {
        'user': [0, 0, 1, 1, 2, 2],
        'phone': [123, 123, 123, 123, 222, 222],
        'email': ['aaa', 'aaa', 'bbb', 'bbb', 'aaa', 'aaa']
    }
)
result = df.with_columns(
    cluster_id=get_cluster_ids(
        node_definition=pl.col('user'),
        edge_definitions=['phone', 'email'],
    )
)
print(result)
