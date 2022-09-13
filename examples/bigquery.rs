use gcprs::auth;
use gcprs::bigquery;
use bigquery::{Bq, BqTable, BqListParam};

use anyhow;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    //let spauth = auth::GcpAuth::from_service_account().await.unwrap();
    let spauth = auth::GcpAuth::from_user_auth().await.unwrap();
    let project = "blocks-gcp-82d5249c";//"blocks-gn-okazaki";
    let bigquery = bigquery::Bq::new(spauth, &project).unwrap();
    let mut list_params = BqListParam::new();
    let datasets = bigquery.list_dataset(&list_params).await?;
    //println!("{:?}", datasets);

    //let edge_table = BqTable::new(&project, "sbm_tsp_simple", "edges");
    //let edges = bigquery
    //    .list_tabledata(&edge_table, &list_params)
    //    .await?
    //    .iter()
    //    .map(|r| Edge::from_json(&r.to_string()))
    //    .collect::<Vec<_>>();
    //println!("edges: {:?}", edges);
    //let node_table = BqTable::new(&project, "sbm_tsp_simple", "nodes");
    //let mut nodes = bigquery
    //    .list_tabledata(&node_table, &list_params)
    //    .await?
    //    .iter()
    //    .map(|r| Node::from_json(&r.to_string()))
    //    .collect::<Vec<_>>();
    //println!("nodes: {:?}", nodes);
    //println!("matrix: {:?}", build_matrix(&edges, &mut nodes));

    //let tables = bigquery.list_tables(&datasets[5], &list_params).await?;
    //println!("tables: {:?}", tables);
    list_params.max_results(10);
    let bqtable = bigquery::BqTable {
        dataset: bigquery::BqDataset {
            dataset_id: "optimization_prod".to_string(),
            project_id: project.to_string(),
        },
        table_id: "locations_F".to_string(),
        created_at: None,
        expired_at: None,
    };
    let data = bigquery
            .list_tabledata(&bqtable, &list_params)
            .await?;
    let jstr = serde_json::to_string(&data).unwrap();
    println!("{:}", jstr);
    //println!(
    //    "{}",
    //    bigquery
    //        .list_tabledata(&tables[4], &list_params)
    //        .await?
    //        .iter()
    //        .map(|r| r.to_string())
    //        .collect::<Vec<_>>()
    //        .join(",")
    //);
    //println!(
    //    "{}",
    //    bigquery
    //        .list_tabledata(&tables[4], &list_params)
    //        .await?
    //        .iter()
    //        .map(|r| r.to_string())
    //        .collect::<Vec<_>>()
    //        .join(",")
    //);
    //println!(
    //    "{}",
    //    bigquery
    //        .list_tabledata(&tables[0], &list_params)
    //        .await?
    //        .iter()
    //        .map(|r| r.to_string())
    //        .collect::<Vec<_>>()
    //        .join(",")
    //);
    Ok(())
}
