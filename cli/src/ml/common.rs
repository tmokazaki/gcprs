use anyhow::Result;
use datafusion::arrow::array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{DataFrame, SessionContext};
use std::sync::Arc;

macro_rules! get_value {
    ($array_type:ty, $column: ident, $row: ident) => {{
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        let s = array.value($row);

        Ok(s as f64)
    }};
}

pub fn array_value(column: &array::ArrayRef, row: usize) -> Result<f64> {
    if column.is_null(row) {
        anyhow::bail!("null is not supported")
    }
    match column.data_type() {
        DataType::Int8 => get_value!(array::Int8Array, column, row),
        DataType::Int16 => get_value!(array::Int16Array, column, row),
        DataType::Int32 => get_value!(array::Int32Array, column, row),
        DataType::Int64 => get_value!(array::Int64Array, column, row),
        DataType::UInt8 => get_value!(array::UInt8Array, column, row),
        DataType::UInt16 => get_value!(array::UInt16Array, column, row),
        DataType::UInt32 => get_value!(array::UInt32Array, column, row),
        DataType::UInt64 => get_value!(array::UInt64Array, column, row),
        DataType::Float16 => get_value!(array::Float32Array, column, row),
        DataType::Float32 => get_value!(array::Float32Array, column, row),
        DataType::Float64 => get_value!(array::Float64Array, column, row),
        _ => anyhow::bail!("unsupported format"),
    }
}

pub struct BaseData {
    columns: Vec<String>,
    fields: Vec<Field>,
    base_dataset: Vec<f64>,
    total_rows: usize,
}

impl BaseData {
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            columns,
            fields: Vec::new(),
            base_dataset: Vec::new(),
            total_rows: 0,
        }
    }

    pub fn base_dataset(&self) -> Vec<f64> {
        self.base_dataset.clone()
    }

    pub fn columns(&self) -> &Vec<String> {
        &self.columns
    }

    pub fn fields(&self) -> &Vec<Field> {
        &self.fields
    }

    pub fn add_field(&mut self, field: Field) {
        self.fields.push(field)
    }

    pub fn fields_to_schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(
            self.fields.iter().map(|f| f.to_owned()).collect(),
        ))
    }

    pub fn total_rows(&self) -> usize {
        self.total_rows
    }

    fn clear(&mut self) {
        self.fields.clear();
        self.base_dataset.clear();
        self.total_rows = 0;
    }

    pub async fn make_dataset(&mut self, ctx: &SessionContext) -> Result<()> {
        self.clear();

        let query_target = self.columns.join(",");
        let sql = format!("select {query_target} from t0 group by {query_target}");
        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;
        for (i, batch) in batches.iter().enumerate() {
            let schema = batch.schema();
            self.total_rows += batch.num_rows();
            for row in 0..batch.num_rows() {
                for col in 0..batch.num_columns() {
                    let field = schema.field(col);
                    //println!("{}, {:?}", field, args.columns);
                    if self.columns.contains(field.name()) {
                        let column = batch.column(col);
                        if column.is_null(row) {
                            anyhow::bail!("unexpected input")
                        }
                        if i == 0 && row == 0 {
                            self.fields
                                .push(field.to_owned().with_name(format!("{}_", field.name())));
                        }
                        let v = array_value(column, row).unwrap();
                        self.base_dataset.push(v);
                    }
                }
            }
        }
        //println!(
        //    "{}, {}, {}",
        //    self.base_dataset.len(),
        //    self.total_rows,
        //    self.fields.len()
        //);
        Ok(())
    }
}

pub async fn labeled_dataframe(
    ctx: &SessionContext,
    columns: &Vec<String>,
    batch: RecordBatch,
) -> DataFrame {
    // add new label table
    ctx.register_batch("t0_", batch)
        .expect("register label table failed");
    let join_str: Vec<String> = columns
        .iter()
        .map(|c| format!("t0.{c} = t0_.{c}_"))
        .collect();
    let sql = format!(
        "select t0.*, t0_.label as label from t0 join t0_ on {}",
        join_str.join(" and ")
    );
    // join original table and label table to add a clustered label
    ctx.sql(&sql).await.expect("join query failure")
}
