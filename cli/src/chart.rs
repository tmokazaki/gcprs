use crate::df::{register_source, session_context};
use crate::ml::common::array_value;
use anyhow::Result;
use clap::{Args, Subcommand};
use datafusion::arrow::util::display::array_value_to_string;
use ndarray::*;
use plotly::{
    common::Visible,
    layout::{Center, DragMode, Layout, Mapbox, MapboxStyle, Margin},
    Plot, ScatterMapbox,
};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;

#[derive(Debug, Args)]
pub struct ChartArgs {
    #[clap(subcommand)]
    pub chart_sub_command: ChartSubCommand,

    /// Input files.
    ///
    /// You can use glob format for a single table.
    /// Multiple tables are also supported. To use it, add `-i <filename>` arguments as you need.
    #[clap(short = 'i', long = "inputs")]
    pub inputs: Vec<String>,

    /// Output file
    ///
    /// The result is always shown in stdout. This option write the result to the file.
    #[clap(short = 'o', long = "output", default_value = None)]
    pub output: String,
}

#[derive(Debug, Subcommand)]
pub enum ChartSubCommand {
    /// Create Scatter on Map
    ///
    /// Input data must have columns which have `longitude` and `latitude` value.
    /// If you set label on each data, use `--data_label` option.
    /// If you create multiple series of data on a same map, use `--regend_label` option.
    ScatterMapbox(ScatterMapboxArgs),
}

#[derive(Default, Debug, Args)]
pub struct ScatterMapboxArgs {
    /// longitude column name. Must be numeric type.
    #[clap(short = 'n', long = "longitude")]
    longitude: String,

    /// latitude column name. Must be numeric type.
    #[clap(short = 't', long = "latitude")]
    latitude: String,

    /// data point label column name
    #[clap(short = 'd', long = "data_label")]
    data_label: Option<String>,

    /// regend label column name
    #[clap(short = 'r', long = "regend_label")]
    regend_label: Option<String>,
}

pub fn write_file(plot: Plot, filename: String) -> Result<()> {
    let path = Path::new(&filename);
    if let Some(output_ex) = path.extension().and_then(OsStr::to_str) {
        match output_ex {
            "html" => {
                plot.write_html(&filename);
            }
            "png" => {
                plot.write_image(filename, plotly::ImageFormat::PNG, 800, 600, 1.0);
            }
            "svg" => {
                plot.write_image(filename, plotly::ImageFormat::SVG, 800, 600, 1.0);
            }
            _ => anyhow::bail!("unsupported file format: {}", output_ex),
        };
        Ok(())
    } else {
        anyhow::bail!("unsupported file format: {}", filename)
    }
}

struct ScatterMapData {
    longitude: Vec<f64>,
    latitude: Vec<f64>,
    label: Vec<String>,
    regend: String,
}

impl ScatterMapData {
    fn new(regend: String) -> Self {
        ScatterMapData {
            longitude: Vec::new(),
            latitude: Vec::new(),
            label: Vec::new(),
            regend,
        }
    }

    fn push_lng(&mut self, v: f64) -> &Self {
        self.longitude.push(v);
        self
    }

    fn push_lat(&mut self, v: f64) -> &Self {
        self.latitude.push(v);
        self
    }

    fn push_label(&mut self, v: String) -> &Self {
        self.label.push(v);
        self
    }
}

pub async fn handle(cargs: ChartArgs) -> Result<()> {
    let ctx = session_context();

    register_source(&ctx, cargs.inputs).await?;

    match cargs.chart_sub_command {
        ChartSubCommand::ScatterMapbox(args) => {
            let mut series_map: HashMap<String, ScatterMapData> = HashMap::new();
            let mut query_target = vec![args.longitude.clone(), args.latitude.clone()];
            if let Some(label) = args.data_label.as_ref() {
                query_target.push(label.clone());
            }
            if let Some(regend) = args.regend_label.as_ref() {
                query_target.push(regend.clone());
            }
            let sql = format!("select {} from t0", query_target.join(","));
            println!("sql: {}", sql);

            let df = ctx.sql(&sql).await?;
            let regends = if let Some(regend) = args.regend_label.as_ref() {
                let regend_df = df.clone().select_columns(&[regend])?.distinct()?;
                let batches = regend_df.collect().await?;
                let mut regends: Vec<String> = Vec::new();
                for batch in batches.iter() {
                    for row in 0..batch.num_rows() {
                        for col in 0..batch.num_columns() {
                            let column = batch.column(col);
                            let v = array_value_to_string(column, row).unwrap();
                            regends.push(v);
                        }
                    }
                }
                regends
            } else {
                vec![String::from("")]
            };
            // create chart data store
            for r in regends {
                series_map.insert(r.clone(), ScatterMapData::new(r));
            }

            let batches = df.collect().await?;
            for batch in batches.iter() {
                let regend_column = args
                    .regend_label
                    .as_ref()
                    .map(|r| batch.column_by_name(r))
                    .flatten();

                if let (Some(longitude_column), Some(latitude_column)) = (
                    batch.column_by_name(&args.longitude),
                    batch.column_by_name(&args.latitude),
                ) {
                    // transform columner data into chart data.
                    for row in 0..batch.num_rows() {
                        let label = if let Some(label) = args.data_label.as_ref() {
                            if let Some(label_column) = batch.column_by_name(label) {
                                Some(array_value_to_string(label_column, row).unwrap())
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        if longitude_column.is_null(row) || latitude_column.is_null(row) {
                            anyhow::bail!("unexpected input")
                        }
                        let lon_v = array_value(longitude_column, row).unwrap();
                        let lat_v = array_value(latitude_column, row).unwrap();
                        let r = regend_column
                            .map(|c| array_value_to_string(c, row).unwrap())
                            .unwrap_or(String::from(""));
                        let d = series_map.get_mut(&r).unwrap();
                        d.push_lng(lon_v);
                        d.push_lat(lat_v);
                        d.push_label(label.unwrap_or_else(|| String::from("")));
                    }
                }
            }

            let mut plot = Plot::new();
            let mut lon_av = 0.0;
            let mut lat_av = 0.0;
            for (_, v) in series_map.iter() {
                let mut trace = ScatterMapbox::new(v.latitude.clone(), v.longitude.clone())
                    .visible(Visible::True)
                    .name(v.regend.clone());
                if 0 < v.label.len() {
                    trace = trace.text_array(v.label.clone());
                }
                lon_av = lon_av + Array::from_vec(v.longitude.clone()).mean().unwrap();
                lat_av = lat_av + Array::from_vec(v.latitude.clone()).mean().unwrap();

                plot.add_trace(trace);
            }
            lon_av = lon_av / series_map.len() as f64;
            lat_av = lat_av / series_map.len() as f64;

            let layout = Layout::new()
                .auto_size(true)
                .drag_mode(DragMode::Zoom)
                .margin(Margin::new().top(0).left(0).bottom(0).right(0))
                .mapbox(
                    Mapbox::new()
                        .style(MapboxStyle::OpenStreetMap)
                        .center(Center::new(lat_av, lon_av))
                        .zoom(6),
                );
            plot.set_layout(layout);

            write_file(plot, cargs.output)?;

            Ok(())
        }
    }
}
