use gcprs::{auth, gcs};

#[tokio::main]
async fn main() {
    //let spauth = auth::GcpAuth::from_service_account().await.unwrap();
    let spauth = auth::GcpAuth::from_user_auth().await.unwrap();
    //let scopes = &["https://www.googleapis.com/auth/cloud-platform.read-only"];
    //let tok = sa.token(scopes).await.unwrap();
    //println!("token is: {:?}", tok);
    //let bucket = "blocks-gn-okazaki-optimization-job-store";
    let bucket = "concise-kayak-852-optimization-job-store";
    let cloud_storage = gcs::Gcs::new(spauth, bucket.to_string()).unwrap();
    let mut params = gcs::GcsListParam::new();
    params.prefix(&"shift/");
    let objects = cloud_storage
        //.list_objects(Some("binpacking/"), Some(5), Some("/"))
        .list_objects(&params)
        .await
        .unwrap();
    //println!("{:?}", objects);
    println!("{:?}", objects.len());
    //for mut obj in objects {
    //    if cloud_storage.get_object(&mut obj).await.is_ok() {
    //        println!("{:?}", obj);
    //    }
    //}
}
