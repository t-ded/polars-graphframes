from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Iterable

import polars as pl
from polars.plugins import register_plugin_function

from polars_graphframes._internal import __version__ as __version__

if TYPE_CHECKING:
    from polars_graphframes.typing import IntoExpr

LIB = Path(__file__).parent


def get_cluster_ids(node_definition: IntoExpr, edge_definitions: Iterable[IntoExpr]) -> pl.Expr:
    return register_plugin_function(
        args=[node_definition] + edge_definitions,
        plugin_path=LIB,
        function_name="get_cluster_ids",
        is_elementwise=False,
    )

