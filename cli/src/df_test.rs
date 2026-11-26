#[cfg(test)]
mod tests {
    #![allow(clippy::all)]
    use super::super::*;
    use datafusion::arrow::array::{Array, Float64Array, Int64Array, StringArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::record_batch::RecordBatch;
    use std::io::Write;

    /// Read a string cell regardless of the physical string type (Utf8 / Utf8View / ...).
    fn string_value(batch: &RecordBatch, col: usize, row: usize) -> String {
        let casted = cast(batch.column(col), &DataType::Utf8).unwrap();
        casted
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row)
            .to_string()
    }

    fn write_file(dir: &tempfile::TempDir, name: &str, content: &str) -> String {
        let path = dir.path().join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        path.to_string_lossy().to_string()
    }

    async fn query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        ctx.sql(sql).await.unwrap().collect().await.unwrap()
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    #[tokio::test]
    async fn test_register_csv() {
        let dir = tempfile::tempdir().unwrap();
        let csv = write_file(&dir, "data.csv", "x,y\n1,a\n2,b\n3,c\n");
        let ctx = session_context();
        register_source(&ctx, vec![csv]).await.unwrap();

        let batches = query(&ctx, "select sum(x) as s, count(*) as c from t0").await;
        let s = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(s.value(0), 6);
        let c = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(c.value(0), 3);
    }

    #[tokio::test]
    async fn test_register_json() {
        let dir = tempfile::tempdir().unwrap();
        let json = write_file(
            &dir,
            "data.json",
            "{\"a\":1,\"b\":\"x\"}\n{\"a\":2,\"b\":\"y\"}\n",
        );
        let ctx = session_context();
        register_source(&ctx, vec![json]).await.unwrap();

        let batches = query(&ctx, "select b from t0 where a = 2").await;
        assert_eq!(total_rows(&batches), 1);
        let b = string_value(&batches[0], 0, 0);
        assert_eq!(b, "y");
    }

    #[tokio::test]
    async fn test_register_njson_extension() {
        let dir = tempfile::tempdir().unwrap();
        let json = write_file(&dir, "data.njson", "{\"a\":10}\n{\"a\":20}\n{\"a\":30}\n");
        let ctx = session_context();
        register_source(&ctx, vec![json]).await.unwrap();

        let batches = query(&ctx, "select count(*) from t0").await;
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(c.value(0), 3);
    }

    #[tokio::test]
    async fn test_register_multiple_sources_and_join() {
        let dir = tempfile::tempdir().unwrap();
        let users = write_file(&dir, "users.csv", "id,name\n1,alice\n2,bob\n");
        let scores = write_file(
            &dir,
            "scores.json",
            "{\"id\":1,\"score\":90}\n{\"id\":2,\"score\":70}\n",
        );
        let ctx = session_context();
        register_source(&ctx, vec![users, scores]).await.unwrap();

        let batches = query(
            &ctx,
            "select t0.name from t0 join t1 on t0.id = t1.id where t1.score > 80",
        )
        .await;
        assert_eq!(total_rows(&batches), 1);
        let name = string_value(&batches[0], 0, 0);
        assert_eq!(name, "alice");
    }

    #[tokio::test]
    async fn test_register_unsupported_extension() {
        let dir = tempfile::tempdir().unwrap();
        let txt = write_file(&dir, "data.txt", "hello");
        let ctx = session_context();
        assert!(register_source(&ctx, vec![txt]).await.is_err());
    }

    #[tokio::test]
    async fn test_register_without_extension() {
        let ctx = session_context();
        assert!(register_source(&ctx, vec!["no_extension".to_string()])
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_udf_pow() {
        let dir = tempfile::tempdir().unwrap();
        let csv = write_file(&dir, "data.csv", "x\n2.0\n3.0\n");
        let ctx = session_context();
        register_source(&ctx, vec![csv]).await.unwrap();
        ctx.register_udf(udf_pow());

        let batches = query(&ctx, "select pow(x, 2) from t0 order by x").await;
        let v = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v.value(0), 4.0);
        assert_eq!(v.value(1), 9.0);
    }

    #[tokio::test]
    async fn test_udaf_string_agg_contains_all_values() {
        let dir = tempfile::tempdir().unwrap();
        let csv = write_file(&dir, "data.csv", "b\nx\ny\n");
        let ctx = session_context();
        register_source(&ctx, vec![csv]).await.unwrap();
        ctx.register_udaf(udaf_string_agg());

        let batches = query(&ctx, "select string_agg(b) from t0").await;
        let v = string_value(&batches[0], 0, 0);
        let joined = v.as_str();
        // The number of separators may vary with partitioning, so only check the values.
        let values: Vec<&str> = joined.split('\n').filter(|s| !s.is_empty()).collect();
        assert_eq!(values.len(), 2);
        assert!(values.contains(&"x"));
        assert!(values.contains(&"y"));
    }
}
