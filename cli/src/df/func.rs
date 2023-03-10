use datafusion::arrow::{
    array::{ArrayRef, Float64Array},
    datatypes::DataType,
};
use datafusion::logical_expr::Volatility;
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::prelude::create_udf;
use datafusion_common::cast::as_float64_array;
use datafusion_expr::ScalarUDF;
use std::sync::Arc;

pub fn udf_pow() -> ScalarUDF {
    let pow = make_scalar_function(|args: &[ArrayRef]| {
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
        Ok(Arc::new(array) as ArrayRef)
    });

    create_udf(
        "pow",
        // expects two f64 input args
        vec![DataType::Float64, DataType::Float64],
        // returns f64
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        pow,
    )
}
