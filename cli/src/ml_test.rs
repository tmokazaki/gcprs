#[cfg(test)]
mod tests {
    #![allow(clippy::all)]
    use super::super::*;
    use crate::df::{register_source, session_context};
    use std::io::Write;

    /// Two well separated groups of points.
    const POINTS_CSV: &str = "x,y\n1.0,1.0\n1.1,0.9\n0.9,1.2\n8.0,8.0\n8.2,7.9\n7.9,8.1\n";

    async fn context_with_points() -> (tempfile::TempDir, SessionContext) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("points.csv");
        std::fs::File::create(&path)
            .unwrap()
            .write_all(POINTS_CSV.as_bytes())
            .unwrap();
        let ctx = session_context();
        register_source(&ctx, vec![path.to_string_lossy().to_string()])
            .await
            .unwrap();
        (dir, ctx)
    }

    async fn dataset_array(ctx: &SessionContext) -> Array2<f64> {
        let mut base = common::BaseData::new(vec!["x".to_string(), "y".to_string()]);
        base.make_dataset(ctx).await.unwrap();
        Array::from_vec(base.base_dataset())
            .into_shape_with_order((base.total_rows(), base.fields().len()))
            .unwrap()
    }

    #[tokio::test]
    async fn test_make_dataset_and_reshape() {
        let (_dir, ctx) = context_with_points().await;
        let mut base = common::BaseData::new(vec!["x".to_string(), "y".to_string()]);
        base.make_dataset(&ctx).await.unwrap();

        assert_eq!(base.total_rows(), 6);
        assert_eq!(base.fields().len(), 2);
        assert_eq!(base.base_dataset().len(), 12);

        let arr = Array::from_vec(base.base_dataset())
            .into_shape_with_order((base.total_rows(), base.fields().len()))
            .unwrap();
        assert_eq!(arr.shape(), &[6, 2]);
        // Rows keep (x, y) pairs together (row-major order).
        let rows: Vec<(f64, f64)> = arr.rows().into_iter().map(|r| (r[0], r[1])).collect();
        assert!(rows.contains(&(1.0, 1.0)));
        assert!(rows.contains(&(8.2, 7.9)));
    }

    #[test]
    fn test_reshape_rejects_wrong_shape() {
        let res = Array::from_vec(vec![1.0, 2.0, 3.0]).into_shape_with_order((2, 2));
        assert!(res.is_err());
    }

    fn assert_two_groups(arr: &Array2<f64>, labels: &[usize]) {
        let group_of = |small: bool| -> Vec<usize> {
            arr.rows()
                .into_iter()
                .zip(labels)
                .filter(|(r, _)| (r[0] < 5.0) == small)
                .map(|(_, l)| *l)
                .collect()
        };
        let small = group_of(true);
        let large = group_of(false);
        assert_eq!(small.len(), 3);
        assert_eq!(large.len(), 3);
        assert!(small.iter().all(|l| *l == small[0]));
        assert!(large.iter().all(|l| *l == large[0]));
        assert_ne!(small[0], large[0]);
    }

    #[tokio::test]
    async fn test_kmeans_separates_clusters() {
        let (_dir, ctx) = context_with_points().await;
        let arr = dataset_array(&ctx).await;
        let dataset = DatasetBase::from(arr.clone());

        let model = KMeans::params(2)
            .max_n_iterations(100)
            .tolerance(1e-3)
            .fit(&dataset)
            .unwrap();
        let predicted = model.predict(dataset);
        let labels: Vec<usize> = predicted.targets.iter().copied().collect();
        assert_two_groups(&arr, &labels);
    }

    #[tokio::test]
    async fn test_kmeans_model_serde_roundtrip() {
        let (_dir, ctx) = context_with_points().await;
        let arr = dataset_array(&ctx).await;
        let dataset = DatasetBase::from(arr.clone());
        let model = KMeans::params(2).fit(&dataset).unwrap();

        let json = serde_json::to_string(&model).unwrap();
        let restored: KMeans<f64, linfa_nn::distance::L2Dist> =
            serde_json::from_str(&json).unwrap();
        assert_eq!(model.centroids(), restored.centroids());
    }

    #[tokio::test]
    async fn test_dbscan_separates_clusters() {
        let (_dir, ctx) = context_with_points().await;
        let arr = dataset_array(&ctx).await;

        let clusters = Dbscan::params(2)
            .tolerance(1.0)
            .transform(DatasetBase::from(arr.clone()))
            .unwrap();
        let labels: Vec<usize> = clusters
            .targets
            .iter()
            .map(|l| l.expect("every point should belong to a cluster"))
            .collect();
        assert_two_groups(&arr, &labels);
    }
}
