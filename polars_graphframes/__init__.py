from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Iterable

import polars as pl
from polars.plugins import register_plugin_function
from polars import functions as F

from polars_graphframes._internal import __version__ as __version__

if TYPE_CHECKING:
    from polars_graphframes.typing import IntoExpr

LIB = Path(__file__).parent


def get_cluster_ids(node_definition: IntoExpr, edge_definitions: Iterable[IntoExpr]) -> pl.Expr:

    if isinstance(node_definition, str):
        node_definition = F.col(node_definition)
    edge_definitions_exprs: list[pl.Expr | pl.Series] = [F.col(edge_definition) if isinstance(edge_definition, str) else edge_definition for edge_definition in edge_definitions]

    return register_plugin_function(
        args=[node_definition.hash(seed=0)] + [edge_definition.hash(1 + i) for i, edge_definition in enumerate(edge_definitions_exprs)],
        plugin_path=LIB,
        function_name="get_cluster_ids",
        is_elementwise=False,
    )

