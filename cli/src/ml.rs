mod common;

use crate::df::{print_dataframe, register_source, session_context};
use anyhow::Result;
use clap::{Args, Subcommand};
use datafusion::prelude::SessionContext;
use datafusion::arrow::array;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::from_slice::FromSlice;
use linfa::prelude::*;
use linfa::DatasetBase;
use linfa_clustering::{Dbscan, KMeans};
use ndarray::*;
use std::sync::Arc;

#[derive(Debug, Args)]
pub struct MlArgs {
    #[clap(subcommand)]
    pub ml_sub_command: MlSubCommand,

    /// Input files.
    ///
    /// You can use glob format for a single table.
    /// Multiple tables are also supported. To use it, add `-i <filename>` arguments as you need.
    #[clap(short = 'i', long = "inputs")]
    pub inputs: Vec<String>,

    /// Output raw JSON
    #[clap(short = 'j', long = "json", default_value = "false")]
    pub json: bool,

    /// Output statistics
    #[clap(short = 's', long = "stats", default_value = "false")]
    pub stats: bool,

    /// Output file. Optional.
    ///
    /// The result is always shown in stdout. This option write the result to the file.
    #[clap(short = 'o', long = "output", default_value = None)]
    pub output: Option<String>,

    /// If Output argument file exists, force to remove.
    #[clap(short = 'r', long = "remove", default_value = "false")]
    pub remove: bool,
}

#[derive(Debug, Subcommand)]
pub enum MlSubCommand {
    /// DBScan
    Dbscan(DbscanArgs),
    /// KMeans
    Kmeans(KmeansArgs),
}

#[derive(Default, Debug, Args)]
pub struct DbscanArgs {
    /// epsilon
    #[clap(short = 'e', long = "epsilon")]
    epsilon: f64,

    /// minimum points to be clustered
    #[clap(short = 'p', long = "min_point")]
    min_point: usize,

    /// target columns to use clustering
    #[clap(short = 'c', long = "columns")]
    columns: Vec<String>,
}

#[derive(Default, Debug, Args)]
pub struct KmeansArgs {
    /// epsilon
    #[clap(short = 't', long = "tolerance", default_value = "1e-3")]
    tolerance: f64,

    /// minimum points to be clustered
    #[clap(short = 'p', long = "max_iteration", default_value = "100")]
    max_iterations: u64,

    /// minimum points to be clustered
    #[clap(short = 'n', long = "num_clusters")]
    num_clusters: usize,

    /// target columns to use clustering
    #[clap(short = 'c', long = "columns")]
    columns: Vec<String>,

    /// model file name
    #[clap(short = 'm', long = "load_model_file", default_value = None)]
    load_model_file: Option<String>,

    /// output model file name
    #[clap(short = 's', long = "save_model_file", default_value = None)]
    save_model_file: Option<String>,
}

async fn run_kmeans(show_stats: bool, as_json: bool, args: KmeansArgs, ctx: SessionContext) -> Result<()> {
    let mut base_dataset = common::BaseData::new(args.columns);
    base_dataset.make_dataset(&ctx).await?;

    let dataset_arr = Array::from_vec(base_dataset.base_dataset())
        .into_shape((base_dataset.total_rows(), base_dataset.fields().len()))?;
    let dataset = DatasetBase::from(dataset_arr.clone());

    let model = if let Some(model_file) = args.load_model_file {
        let reader = std::fs::File::open(model_file).expect("Failed to open file");
        serde_json::from_reader(reader).expect("Failed to deserialize model")
    } else {
        KMeans::params(args.num_clusters)
            .max_n_iterations(args.max_iterations)
            .tolerance(args.tolerance)
            .fit(&dataset)
            .expect("Kmeans fitted")
    };

    let dataset = model.predict(dataset);
    //println!("{:?}", dataset);
    if show_stats {
        println!("shilhouette score: {}", dataset.silhouette_score()?);
    }

    base_dataset.add_field(Field::new("label", DataType::UInt16, true));

    let schema = base_dataset.fields_to_schema();
    let dataset_arr_trans = dataset_arr.reversed_axes();
    let mut columns: Vec<array::ArrayRef> = Vec::new();
    for n in 0..base_dataset.columns().len() {
        columns.push(Arc::new(array::Float64Array::from_slice(
            dataset_arr_trans.slice(s!(n, ..)).to_vec(),
        )))
    }
    let label_data: Vec<u16> = dataset.targets.iter().map(|v| *v as u16).collect();
    columns.push(Arc::new(array::UInt16Array::from_slice(label_data)));
    let batch = RecordBatch::try_new(schema, columns)?;
    //println!("{:?}", batch);

    let df = common::labeled_dataframe(&ctx, base_dataset.columns(), batch).await;

    print_dataframe(df, as_json).await?;

    if let Some(model_file) = args.save_model_file {
        let writer = std::fs::File::create(model_file).expect("Failed to open file");
        serde_json::to_writer(writer, &model).expect("Failed to serialize model");
    }

    Ok(())
}

async fn run_dbscan(show_stats: bool, as_json: bool, args: DbscanArgs, ctx: SessionContext) -> Result<()> {
    let mut base_dataset = common::BaseData::new(args.columns);
    base_dataset.make_dataset(&ctx).await?;

    let dataset_arr = Array::from_iter(base_dataset.base_dataset())
        .into_shape((base_dataset.total_rows(), base_dataset.fields().len()))?;
    let dataset = DatasetBase::from(dataset_arr.clone());

    let clusters = Dbscan::params(args.min_point)
        .tolerance(args.epsilon)
        .transform(dataset)
        .unwrap();
    //println!("{:?}", clusters);
    if show_stats {
        println!("shilhouette score: {}", clusters.silhouette_score()?);
    }

    // add label column to new table
    base_dataset.add_field(Field::new("label", DataType::UInt16, true));

    let schema = base_dataset.fields_to_schema();
    let dataset_arr_trans = dataset_arr.reversed_axes();
    let mut columns: Vec<array::ArrayRef> = Vec::new();
    for n in 0..base_dataset.columns().len() {
        columns.push(Arc::new(array::Float64Array::from_slice(
            dataset_arr_trans.slice(s!(n, ..)).to_vec(),
        )))
    }

    // if there is no class assigned, assign an independent class
    let mut label_data: Vec<u16> = Vec::new();
    let mut noise = base_dataset.total_rows() + 1;
    for t in clusters.targets.iter() {
        if let Some(v) = t {
            label_data.push(*v as u16)
        } else {
            label_data.push(noise as u16);
            noise += 1;
        }
    }
    columns.push(Arc::new(array::UInt16Array::from_slice(label_data)));

    let batch = RecordBatch::try_new(schema, columns)?;
    //println!("{:?}", batch);

    let df = common::labeled_dataframe(&ctx, base_dataset.columns(), batch).await;

    print_dataframe(df, as_json).await?;

    Ok(())
}

pub async fn handle(mlargs: MlArgs) -> Result<()> {
    let ctx = session_context();
    register_source(&ctx, mlargs.inputs).await?;

    match mlargs.ml_sub_command {
        MlSubCommand::Kmeans(args) => {
            anyhow::ensure!(
                0 < args.columns.len(),
                "no columns specified. please set target column with '--columns' option."
            );

            run_kmeans(mlargs.stats, mlargs.json, args, ctx).await?;

            Ok(())
        }
        MlSubCommand::Dbscan(args) => {
            anyhow::ensure!(
                0 < args.columns.len(),
                "no columns specified. please set target column with '--columns' option."
            );

            run_dbscan(mlargs.stats, mlargs.json, args, ctx).await?;

            Ok(())
        }
    }
}
