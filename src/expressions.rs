#![allow(clippy::unused_unit)]

use std::collections::HashMap;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use petgraph::graphmap::UnGraphMap;
use petgraph::algo::tarjan_scc;


#[polars_expr(output_type=UInt64)]
fn get_cluster_ids(inputs: &[Series]) -> PolarsResult<Series> {

    let node_definition = inputs[0].u64()?;
    let edgelist = (1..inputs.len())
        .flat_map(|i| {
            node_definition
                .iter()
                .flatten()
                .zip(inputs[i].u64().unwrap().iter().flatten())
        });
    let graph = UnGraphMap::<_, ()>::from_edges(edgelist);

    let cluster_id_mapping = tarjan_scc(&graph)
        .into_iter()
        .enumerate()
        .flat_map(|(idx, cc)| {
            cc.into_iter().map(move |node| (node, idx as u64))
        })
        .collect::<HashMap<_, _>>();

    let out = node_definition.apply_values(|x| *cluster_id_mapping.get(&x).unwrap());
    Ok(out.into_series())
}