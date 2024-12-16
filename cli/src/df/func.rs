use datafusion::arrow::{
    array::{ArrayRef, Float64Array},
    datatypes::DataType,
};
use datafusion::error::Result;
use datafusion::logical_expr::Volatility;
use datafusion::physical_plan::Accumulator;
use datafusion::prelude::create_udf;
use datafusion::scalar::ScalarValue;
use datafusion_common::cast::as_float64_array;
use datafusion_expr::{create_udaf, AggregateUDF, ColumnarValue, ScalarUDF};
use std::sync::Arc;

pub fn udf_pow() -> ScalarUDF {
    let pow = Arc::new(|args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let base = as_float64_array(&args[0]).expect("cast failed");
        let exponent = as_float64_array(&args[1]).expect("cast failed");
        let array = base
            .iter()
            .zip(exponent.iter())
            .map(|(base, exponent)| match (base, exponent) {
                (Some(base), Some(exponent)) => Some(base.powf(exponent)),
                _ => None,
            })
            .collect::<Float64Array>();
        Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
    });

    create_udf(
        "pow",
        // expects two f64 input args
        vec![DataType::Float64, DataType::Float64],
        // returns f64
        DataType::Float64,
        Volatility::Immutable,
        pow,
    )
}

pub fn udaf_string_agg() -> AggregateUDF {
    create_udaf(
        // the name; used to represent it in plan descriptions and in the registry, to use in SQL.
        "string_agg",
        // the input type; DataFusion guarantees that the first entry of `values` in `update` has this type.
        vec![DataType::Utf8],
        // the return type; DataFusion expects this to match the type returned by `evaluate`.
        Arc::new(DataType::Utf8),
        Volatility::Immutable,
        // This is the accumulator factory; DataFusion uses it to create new accumulators.
        Arc::new(|_| Ok(Box::new(StringAgg::new()))),
        // This is the description of the state. `state()` must match the types here.
        Arc::new(vec![DataType::Utf8]),
    )
}

/// A UDAF has state across multiple rows, and thus we require a `struct` with that state.
#[derive(Debug)]
struct StringAgg {
    string: String,
}

impl StringAgg {
    // how the struct is initialized
    pub fn new() -> Self {
        StringAgg {
            string: String::new(),
        }
    }
}

// UDAFs are built using the trait `Accumulator`, that offers DataFusion the necessary functions
// to use them.
impl Accumulator for StringAgg {
    // This function serializes our state to `ScalarValue`, which DataFusion uses
    // to pass this state between execution stages.
    // Note that this can be arbitrary data.
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::from(self.string.as_str())])
    }

    // DataFusion expects this function to return the final value of this aggregator.
    // in this case, this is the formula of the geometric mean
    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::from(self.string.as_str()))
    }

    // DataFusion calls this function to update the accumulator's state for a batch
    // of inputs rows. In this case the product is updated with values from the first column
    // and the count is updated based on the row count
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = &values[0];
        (0..arr.len()).try_for_each(|index| {
            let v = ScalarValue::try_from_array(arr, index)?;

            if let ScalarValue::Utf8(Some(value)) = v {
                if 0 < self.string.len() {
                    // self.string.push_str(",");
                    self.string.push_str("\n");
                }
                self.string.push_str(&value);
            } else {
                unreachable!("")
            }
            Ok(())
        })
    }

    // Optimization hint: this trait also supports `update_batch` and `merge_batch`,
    // that can be used to perform these operations on arrays instead of single values.
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        let arr = &states[0];
        (0..arr.len()).try_for_each(|index| {
            let v = states
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<Result<Vec<_>>>()?;
            if let ScalarValue::Utf8(Some(string)) = &v[0] {
                if 0 < self.string.len() {
                    // self.string.push_str(",");
                    self.string.push_str("\n");
                }
                self.string.push_str(string);
            } else {
                unreachable!("")
            }
            Ok(())
        })
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}
