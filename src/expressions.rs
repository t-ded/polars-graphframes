#![allow(clippy::unused_unit)]

use std::collections::HashMap;
use std::iter;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use petgraph::graphmap::UnGraphMap;
use petgraph::algo::tarjan_scc;
use std::iter::{zip, empty};
use std::thread::current;
use petgraph::visit::Walker;


#[polars_expr(output_type=UInt64)]
fn get_cluster_ids(inputs: &[Series]) -> PolarsResult<Series> {

    let mut edgelist: Vec<(u64, u64)> = Vec::with_capacity((&inputs[0]).len() * (inputs.len() - 1));
    for i in 1..inputs.len() {
        edgelist.extend(
            zip(
                (&inputs[0]).u64()?.iter().filter_map(|x| x),
                (&inputs[i]).u64()?.iter().filter_map(|x| x),
            )
        )
    }
    let graph = UnGraphMap::<_, ()>::from_edges(edgelist);

    let mut cluster_id_mapping = HashMap::new();
    for (idx, cc) in tarjan_scc(&graph).into_iter().enumerate() {
        for node in cc {
            cluster_id_mapping.insert(node, idx as u64);
        }
    }

    let s = &inputs[0];
    let ca = s.u64()?;
    let out = ca.apply_values(|x| *cluster_id_mapping.get(&x).unwrap());
    Ok(out.into_series())
}