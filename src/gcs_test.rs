#[cfg(test)]
mod tests {
    use super::super::*;
    use chrono::{TimeZone, Utc};
    use google_storage1::api::{Bucket, Object};
    use mime;

    #[test]
    fn test_gcs_bucket_from_api_bucket() {
        let mut api_bucket = Bucket::default();
        api_bucket.id = Some("test-bucket-id".to_string());
        api_bucket.name = Some("test-bucket".to_string());
        api_bucket.location = Some("US".to_string());
        api_bucket.storage_class = Some("STANDARD".to_string());
        api_bucket.location_type = Some("multi-region".to_string());
        api_bucket.self_link = Some("https://example.com/bucket".to_string());
        api_bucket.project_number = Some(123456789);

        let gcs_bucket = GcsBucket::from(api_bucket);

        assert_eq!(gcs_bucket.id, Some("test-bucket-id".to_string()));
        assert_eq!(gcs_bucket.name, Some("test-bucket".to_string()));
        assert_eq!(gcs_bucket.location, Some("US".to_string()));
        assert_eq!(gcs_bucket.storage_class, Some("STANDARD".to_string()));
        assert_eq!(gcs_bucket.location_type, Some("multi-region".to_string()));
        assert_eq!(
            gcs_bucket.self_link,
            Some("https://example.com/bucket".to_string())
        );
        assert_eq!(gcs_bucket.project_number, Some(123456789));
    }

    #[test]
    fn test_gcs_bucket_from_api_bucket_ref() {
        let mut api_bucket = Bucket::default();
        api_bucket.id = Some("test-id".to_string());
        api_bucket.name = Some("test-name".to_string());

        let gcs_bucket = GcsBucket::from(&api_bucket);

        assert_eq!(gcs_bucket.id, Some("test-id".to_string()));
        assert_eq!(gcs_bucket.name, Some("test-name".to_string()));
    }

    #[test]
    fn test_gcs_bucket_serialization() {
        let bucket = GcsBucket {
            id: Some("bucket-id".to_string()),
            name: Some("bucket-name".to_string()),
            location: Some("US".to_string()),
            storage_class: Some("STANDARD".to_string()),
            location_type: Some("region".to_string()),
            self_link: Some("https://example.com".to_string()),
            project_number: Some(987654321),
        };

        let json = serde_json::to_string(&bucket).unwrap();
        let deserialized: GcsBucket = serde_json::from_str(&json).unwrap();

        assert_eq!(bucket.id, deserialized.id);
        assert_eq!(bucket.name, deserialized.name);
        assert_eq!(bucket.location, deserialized.location);
        assert_eq!(bucket.storage_class, deserialized.storage_class);
        assert_eq!(bucket.location_type, deserialized.location_type);
        assert_eq!(bucket.self_link, deserialized.self_link);
        assert_eq!(bucket.project_number, deserialized.project_number);
    }

    #[test]
    fn test_gcs_object_new() {
        let object = GcsObject::new("my-bucket".to_string(), "my-file.txt".to_string());

        assert_eq!(object.bucket, "my-bucket");
        assert_eq!(object.name, Some("my-file.txt".to_string()));
        assert!(object.content_type.is_none());
        assert!(object.size.is_none());
        assert!(object.self_link.is_none());
        assert!(object.created_at.is_none());
        assert!(object.updated_at.is_none());
        assert!(object.content.is_none());
    }

    #[test]
    fn test_gcs_object_get_mime() {
        let mut object = GcsObject::new("bucket".to_string(), "file.txt".to_string());

        // No content type
        assert!(object.get_mime().is_none());

        // Valid content type
        object.content_type = Some("text/plain".to_string());
        let mime = object.get_mime().unwrap();
        assert_eq!(mime.type_(), mime::TEXT);
        assert_eq!(mime.subtype(), mime::PLAIN);

        // Invalid content type defaults to octet-stream
        object.content_type = Some("invalid-mime".to_string());
        let mime = object.get_mime().unwrap();
        assert_eq!(mime.type_(), mime::APPLICATION);
        assert_eq!(mime.subtype(), mime::OCTET_STREAM);
    }

    #[test]
    fn test_gcs_object_mime_setter() {
        let mut object = GcsObject::new("bucket".to_string(), "file.txt".to_string());

        // Valid mime type
        object.mime("text/html".to_string());
        assert_eq!(object.content_type, Some("text/html".to_string()));

        // Invalid mime type defaults to octet stream
        object.mime("not-a-mime".to_string());
        assert_eq!(
            object.content_type,
            Some("application/octet_stream".to_string())
        );
    }

    #[test]
    fn test_gcs_object_url() {
        let object = GcsObject::new("my-bucket".to_string(), "path/to/file.txt".to_string());
        assert_eq!(object.url(), "gs://my-bucket/path/to/file.txt");

        let mut object_no_name = GcsObject::new("my-bucket".to_string(), "".to_string());
        object_no_name.name = None;
        assert_eq!(object_no_name.url(), "gs://my-bucket/");
    }

    #[test]
    fn test_gcs_object_from_object() {
        let mut api_object = Object::default();
        api_object.name = Some("test-object.txt".to_string());
        api_object.content_type = Some("text/plain".to_string());
        api_object.size = Some(1024);
        api_object.self_link = Some("https://example.com/object".to_string());
        api_object.time_created = Some(Utc.timestamp_opt(1609459200, 0).unwrap());
        api_object.updated = Some(Utc.timestamp_opt(1609545600, 0).unwrap());

        let bucket = "test-bucket".to_string();
        let gcs_object = GcsObject::from_object(&bucket, &api_object);

        assert_eq!(gcs_object.bucket, "test-bucket");
        assert_eq!(gcs_object.name, Some("test-object.txt".to_string()));
        assert_eq!(gcs_object.content_type, Some("text/plain".to_string()));
        assert_eq!(gcs_object.size, Some(1024));
        assert_eq!(
            gcs_object.self_link,
            Some("https://example.com/object".to_string())
        );
        assert!(gcs_object.created_at.is_some());
        assert!(gcs_object.updated_at.is_some());
        assert!(gcs_object.content.is_none());
    }

    #[test]
    fn test_gcs_object_into_api_object() {
        let mut gcs_object = GcsObject::new("bucket".to_string(), "file.txt".to_string());
        gcs_object.size = Some(2048);
        gcs_object.content_type = Some("application/json".to_string());
        gcs_object.self_link = Some("https://example.com".to_string());
        gcs_object.created_at = Some(Utc.timestamp_opt(1609459200, 0).unwrap());
        gcs_object.updated_at = Some(Utc.timestamp_opt(1609545600, 0).unwrap());

        let api_object: Object = gcs_object.into();

        assert_eq!(api_object.name, Some("file.txt".to_string()));
        assert_eq!(api_object.size, Some(2048));
        assert_eq!(
            api_object.content_type,
            Some("application/json".to_string())
        );
        assert_eq!(
            api_object.self_link,
            Some("https://example.com".to_string())
        );
        assert!(api_object.time_created.is_some());
        assert!(api_object.updated.is_some());
    }

    #[test]
    fn test_gcs_object_ref_into_api_object() {
        let gcs_object = GcsObject::new("bucket".to_string(), "file.txt".to_string());
        let api_object: Object = (&gcs_object).into();

        assert_eq!(api_object.name, Some("file.txt".to_string()));
    }

    #[test]
    fn test_gcs_object_serialization() {
        let object = GcsObject {
            bucket: "test-bucket".to_string(),
            content_type: Some("text/plain".to_string()),
            name: Some("test.txt".to_string()),
            size: Some(1024),
            self_link: Some("https://example.com".to_string()),
            created_at: Some(Utc.timestamp_opt(1609459200, 0).unwrap()),
            updated_at: Some(Utc.timestamp_opt(1609545600, 0).unwrap()),
            content: Some("test content".to_string()),
        };

        let json = serde_json::to_string(&object).unwrap();

        // Content SHOULD be serialized when it's Some
        assert!(json.contains("\"content\""));
        assert!(json.contains("test content"));

        let deserialized: GcsObject = serde_json::from_str(&json).unwrap();
        assert_eq!(object.bucket, deserialized.bucket);
        assert_eq!(object.name, deserialized.name);
        assert_eq!(object.content_type, deserialized.content_type);
        assert_eq!(object.size, deserialized.size);
        assert_eq!(object.content, deserialized.content);
    }

    #[test]
    fn test_gcs_object_serialization_with_none_content() {
        let object = GcsObject {
            bucket: "test-bucket".to_string(),
            content_type: Some("text/plain".to_string()),
            name: Some("test.txt".to_string()),
            size: Some(1024),
            self_link: None,
            created_at: None,
            updated_at: None,
            content: None,
        };

        let json = serde_json::to_string(&object).unwrap();

        // Content field should not appear in JSON when None
        assert!(!json.contains("\"content\""));
    }

    #[test]
    fn test_gcs_insert_param_new() {
        let param = GcsInsertParam::new();
        // Just test that it can be created
        assert!(format!("{:?}", param).contains("GcsInsertParam"));
    }

    #[test]
    fn test_gcs_list_param_new() {
        let param = GcsListParam::new();

        assert!(param.prefix.is_none());
        assert!(param.max_results.is_none());
        assert!(param.delimiter.is_none());
        assert!(param.next_token.is_none());
        assert!(param.start_offset.is_none());
        assert!(param.end_offset.is_none());
    }

    #[test]
    fn test_gcs_list_param_prefix() {
        let mut param = GcsListParam::new();

        // Test with leading slash
        param.prefix("/path/to/files");
        assert_eq!(param.prefix, Some("path/to/files".to_string()));

        // Test without leading slash
        let mut param2 = GcsListParam::new();
        param2.prefix("another/path");
        assert_eq!(param2.prefix, Some("another/path".to_string()));
    }

    #[test]
    fn test_gcs_list_param_builder() {
        let mut param = GcsListParam::new();

        param
            .prefix("test/")
            .max_results(100)
            .delimiter("/")
            .next_token("token123")
            .start_offset("start")
            .end_offset("end");

        assert_eq!(param.prefix, Some("test/".to_string()));
        assert_eq!(param.max_results, Some(100));
        assert_eq!(param.delimiter, Some("/".to_string()));
        assert_eq!(param.next_token, Some("token123".to_string()));
        assert_eq!(param.start_offset, Some("start".to_string()));
        assert_eq!(param.end_offset, Some("end".to_string()));
    }

    #[test]
    fn test_gcs_list_param_default() {
        let param: GcsListParam = Default::default();

        assert!(param.prefix.is_none());
        assert!(param.max_results.is_none());
        assert!(param.delimiter.is_none());
        assert!(param.next_token.is_none());
        assert!(param.start_offset.is_none());
        assert!(param.end_offset.is_none());
    }

    #[test]
    fn test_gcs_list_param_clone() {
        let mut original = GcsListParam::new();
        original.prefix("test").max_results(50);

        let cloned = original.clone();

        assert_eq!(cloned.prefix, Some("test".to_string()));
        assert_eq!(cloned.max_results, Some(50));
    }

    #[test]
    fn test_gcs_bucket_clone() {
        let original = GcsBucket {
            id: Some("id".to_string()),
            name: Some("name".to_string()),
            location: Some("US".to_string()),
            storage_class: Some("STANDARD".to_string()),
            location_type: Some("region".to_string()),
            self_link: Some("link".to_string()),
            project_number: Some(123),
        };

        let cloned = original.clone();

        assert_eq!(original.id, cloned.id);
        assert_eq!(original.name, cloned.name);
        assert_eq!(original.location, cloned.location);
        assert_eq!(original.storage_class, cloned.storage_class);
        assert_eq!(original.location_type, cloned.location_type);
        assert_eq!(original.self_link, cloned.self_link);
        assert_eq!(original.project_number, cloned.project_number);
    }

    #[test]
    fn test_gcs_object_clone() {
        let original = GcsObject {
            bucket: "bucket".to_string(),
            content_type: Some("text/plain".to_string()),
            name: Some("file.txt".to_string()),
            size: Some(1024),
            self_link: Some("link".to_string()),
            created_at: Some(Utc::now()),
            updated_at: Some(Utc::now()),
            content: Some("content".to_string()),
        };

        let cloned = original.clone();

        assert_eq!(original.bucket, cloned.bucket);
        assert_eq!(original.content_type, cloned.content_type);
        assert_eq!(original.name, cloned.name);
        assert_eq!(original.size, cloned.size);
        assert_eq!(original.self_link, cloned.self_link);
        assert_eq!(original.created_at, cloned.created_at);
        assert_eq!(original.updated_at, cloned.updated_at);
        assert_eq!(original.content, cloned.content);
    }

    #[test]
    fn test_gcs_insert_param_clone() {
        let original = GcsInsertParam::new();
        let cloned = original.clone();

        // Just verify they can be cloned
        assert!(format!("{:?}", original).contains("GcsInsertParam"));
        assert!(format!("{:?}", cloned).contains("GcsInsertParam"));
    }

    #[test]
    fn test_gcs_object_with_empty_bucket() {
        let object = GcsObject::new("".to_string(), "file.txt".to_string());
        assert_eq!(object.url(), "gs:///file.txt");
    }

    #[test]
    fn test_gcs_object_with_special_characters() {
        let object = GcsObject::new(
            "my-bucket".to_string(),
            "path/with spaces/file.txt".to_string(),
        );
        assert_eq!(object.url(), "gs://my-bucket/path/with spaces/file.txt");
    }

    #[test]
    fn test_gcs_list_param_empty_prefix() {
        let mut param = GcsListParam::new();
        param.prefix("");
        assert_eq!(param.prefix, Some("".to_string()));
    }

    #[test]
    fn test_gcs_list_param_multiple_slashes_prefix() {
        let mut param = GcsListParam::new();
        param.prefix("///path/to/files");
        // Only the first slash is removed
        assert_eq!(param.prefix, Some("//path/to/files".to_string()));
    }

    #[test]
    fn test_gcs_object_mime_type_variations() {
        let mut object = GcsObject::new("bucket".to_string(), "file".to_string());

        // Test various mime types
        object.mime("application/json".to_string());
        assert_eq!(object.content_type, Some("application/json".to_string()));

        object.mime("image/png".to_string());
        assert_eq!(object.content_type, Some("image/png".to_string()));

        object.mime("video/mp4".to_string());
        assert_eq!(object.content_type, Some("video/mp4".to_string()));

        // Test invalid mime type with special characters
        object.mime("!!!invalid!!!".to_string());
        assert_eq!(
            object.content_type,
            Some("application/octet_stream".to_string())
        );
    }

    #[test]
    fn test_gcs_object_from_object_with_none_values() {
        let api_object = Object::default();
        let bucket = "test-bucket".to_string();
        let gcs_object = GcsObject::from_object(&bucket, &api_object);

        assert_eq!(gcs_object.bucket, "test-bucket");
        assert!(gcs_object.name.is_none());
        assert!(gcs_object.content_type.is_none());
        assert!(gcs_object.size.is_none());
        assert!(gcs_object.self_link.is_none());
        assert!(gcs_object.created_at.is_none());
        assert!(gcs_object.updated_at.is_none());
        assert!(gcs_object.content.is_none());
    }

    #[test]
    fn test_gcs_bucket_with_none_values() {
        let api_bucket = Bucket::default();
        let gcs_bucket = GcsBucket::from(api_bucket);

        assert!(gcs_bucket.id.is_none());
        assert!(gcs_bucket.name.is_none());
        assert!(gcs_bucket.location.is_none());
        assert!(gcs_bucket.storage_class.is_none());
        assert!(gcs_bucket.location_type.is_none());
        assert!(gcs_bucket.self_link.is_none());
        assert!(gcs_bucket.project_number.is_none());
    }

    #[test]
    fn test_gcs_object_into_api_object_with_minimal_fields() {
        let gcs_object = GcsObject::new("bucket".to_string(), "file.txt".to_string());
        let api_object: Object = gcs_object.into();

        assert_eq!(api_object.name, Some("file.txt".to_string()));
        assert!(api_object.size.is_none());
        assert!(api_object.content_type.is_none());
        assert!(api_object.self_link.is_none());
        assert!(api_object.time_created.is_none());
        assert!(api_object.updated.is_none());
    }

    #[test]
    fn test_gcs_list_param_chaining() {
        let mut param = GcsListParam::new();
        let result = param.prefix("test").max_results(100).delimiter("/");

        // Verify chaining returns mutable reference
        assert_eq!(result.prefix, Some("test".to_string()));
        assert_eq!(result.max_results, Some(100));
        assert_eq!(result.delimiter, Some("/".to_string()));
    }

    #[test]
    fn test_gcs_object_get_mime_with_uppercase() {
        let mut object = GcsObject::new("bucket".to_string(), "file.txt".to_string());
        object.content_type = Some("TEXT/PLAIN".to_string());

        // mime parsing should handle case
        let mime = object.get_mime();
        assert!(mime.is_some());
    }

    #[test]
    fn test_gcs_object_mime_setter_chaining() {
        let mut object = GcsObject::new("bucket".to_string(), "file.txt".to_string());
        let result = object.mime("text/plain".to_string());

        // Verify chaining returns mutable reference
        assert_eq!(result.content_type, Some("text/plain".to_string()));
    }
}
