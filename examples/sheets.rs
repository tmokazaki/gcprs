use gcprs::{auth, sheets};

#[tokio::main]
async fn main() {
    //let spauth = auth::GcpAuth::from_service_account().await.unwrap();
    let spauth = auth::GcpAuth::from_user_auth().await.unwrap();
    //let scopes = &["https://www.googleapis.com/auth/cloud-platform.read-only"];
    //let tok = sa.token(scopes).await.unwrap();
    //println!("token is: {:?}", tok);
    //let bucket = "blocks-gn-okazaki-optimization-job-store";
    let spreadsheet = sheets::SpreadSheet::new(spauth).unwrap();
    let sheet_id = "1N376Q6UDGHdgoz6PeYnliqljaMNCQ8qU-gAIvJ0aDdo";
    let sheet_name = "シート1";
    let mut params = sheets::ValuesGetParam::new(sheet_id.to_string(), sheet_name.to_string());
    //let mut params = gcs::GcsListParam::new();
    //params.prefix(&"shift/");
    let result = spreadsheet.get_values(&params)
        .await
        .unwrap();
    println!("{:?}", result);
    //for mut obj in objects {
    //    if cloud_storage.get_object(&mut obj).await.is_ok() {
    //        println!("{:?}", obj);
    //    }
    //}
}
