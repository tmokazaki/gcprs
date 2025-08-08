use anyhow::Result;
use csv::Writer;
use serde::Serialize;
use std::{io, io::Write};
use tabled::{builder::Builder, settings::Style};

pub trait TableView {
    fn columns(&self) -> Vec<String>;
    fn values(&self) -> Vec<String>;
}

#[derive(Default)]
pub enum OutputFormat {
    Json,
    Csv,
    #[default]
    Stdout,
}

pub fn render<T: TableView + Serialize>(
    data: &Vec<T>,
    output: OutputFormat,
    newline_delimited: bool,
) -> Result<()> {
    let mut writer = io::BufWriter::new(io::stdout());
    match output {
        OutputFormat::Json => {
            if newline_delimited {
                for data in data.iter() {
                    writer.write(serde_json::to_string(&data)?.as_bytes())?;
                    writer.write("\n".as_bytes())?;
                }
            } else {
                writer.write(serde_json::to_string(&data)?.as_bytes())?;
            }
        }
        OutputFormat::Csv => {
            let mut csv_writer = Writer::from_writer(writer);
            let mut first = true;
            for d in data {
                if first {
                    csv_writer.write_record(d.columns())?;
                    first = false;
                }
                csv_writer.serialize(d.values())?;
            }
            csv_writer.flush()?;
        }
        _ => {
            let mut builder = Builder::default();
            let header = if 0 < data.len() {
                data[0].columns()
            } else {
                vec![]
            };
            builder.set_header(header);
            for pj in data {
                builder.push_record(pj.values());
            }

            let mut table = builder.build();
            table.with(Style::markdown());

            writer.write(table.to_string().as_bytes())?;
        }
    }
    Ok(())
}
