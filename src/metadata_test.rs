#[cfg(test)]
mod tests {
    use super::super::*;

    #[test]
    fn test_request_type_display() {
        assert_eq!(RequestType::AccessToken.to_string(), "auth-request-type/at");
        assert_eq!(RequestType::IdToken.to_string(), "auth-request-type/it");
        assert_eq!(RequestType::MdsPing.to_string(), "auth-request-type/mds");
        assert_eq!(RequestType::ReauthStart.to_string(), "auth-request-type/re-start");
        assert_eq!(RequestType::ReauthContinue.to_string(), "auth-request-type/re-cont");
    }

    #[test]
    fn test_credential_type_display() {
        assert_eq!(CredentialType::User.to_string(), "cred-type/u");
        assert_eq!(CredentialType::ServiceAccountAssertion.to_string(), "cred-type/sa");
        assert_eq!(CredentialType::ServiceAccountJwt.to_string(), "cred-type/jwt");
        assert_eq!(CredentialType::ServiceAccountMds.to_string(), "cred-type/mds");
        assert_eq!(CredentialType::ServiceAccountImpersonate.to_string(), "cred-type/imp");
    }

    #[test]
    fn test_request_type_clone() {
        let rt = RequestType::AccessToken;
        let cloned = rt.clone();
        assert_eq!(rt.to_string(), cloned.to_string());
        
        let rt = RequestType::IdToken;
        let cloned = rt.clone();
        assert_eq!(rt.to_string(), cloned.to_string());
    }

    #[test]
    fn test_credential_type_clone() {
        let ct = CredentialType::User;
        let cloned = ct.clone();
        assert_eq!(ct.to_string(), cloned.to_string());
        
        let ct = CredentialType::ServiceAccountMds;
        let cloned = ct.clone();
        assert_eq!(ct.to_string(), cloned.to_string());
    }

    #[test]
    fn test_service_account_info_struct() {
        let info = ServiceAccountInfo {
            aliases: vec!["alias1".to_string(), "alias2".to_string()],
            email: "test@example.com".to_string(),
            scopes: vec!["scope1".to_string(), "scope2".to_string()],
        };

        assert_eq!(info.aliases.len(), 2);
        assert_eq!(info.aliases[0], "alias1");
        assert_eq!(info.email, "test@example.com");
        assert_eq!(info.scopes.len(), 2);
        assert_eq!(info.scopes[0], "scope1");
    }

    #[test]
    fn test_service_account_info_serialization() {
        let info = ServiceAccountInfo {
            aliases: vec!["alias1".to_string()],
            email: "test@example.com".to_string(),
            scopes: vec!["https://www.googleapis.com/auth/cloud-platform".to_string()],
        };

        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("\"email\":\"test@example.com\""));
        assert!(json.contains("\"aliases\":[\"alias1\"]"));
        assert!(json.contains("cloud-platform"));

        let deserialized: ServiceAccountInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(info.email, deserialized.email);
        assert_eq!(info.aliases, deserialized.aliases);
        assert_eq!(info.scopes, deserialized.scopes);
    }

    #[test]
    fn test_service_account_info_deserialization() {
        let json = r#"{
            "aliases": ["default", "123456789"],
            "email": "service-account@project.iam.gserviceaccount.com",
            "scopes": [
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/userinfo.email"
            ]
        }"#;

        let info: ServiceAccountInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.aliases.len(), 2);
        assert_eq!(info.aliases[0], "default");
        assert_eq!(info.aliases[1], "123456789");
        assert_eq!(info.email, "service-account@project.iam.gserviceaccount.com");
        assert_eq!(info.scopes.len(), 2);
    }

    #[test]
    fn test_metadata_api_new() {
        let api = MetadataApi::new();
        // Just verify it can be created
        assert!(format!("{:?}", api).contains("MetadataApi"));
    }

    #[test]
    fn test_metadata_api_clone() {
        let api = MetadataApi::new();
        let cloned = api.clone();
        // Both should be empty structs
        assert!(format!("{:?}", api) == format!("{:?}", cloned));
    }

    #[test]
    fn test_metadata_root_constant() {
        assert_eq!(METADATA_ROOT, "http://metadata.google.internal/computeMetadata/v1/");
    }

    #[test]
    fn test_request_type_access_token_constant() {
        assert_eq!(REQUEST_TYPE_ACCESS_TOKEN, "auth-request-type/at");
    }

    #[test]
    fn test_service_account_info_with_empty_vectors() {
        let info = ServiceAccountInfo {
            aliases: vec![],
            email: "test@example.com".to_string(),
            scopes: vec![],
        };

        assert_eq!(info.aliases.len(), 0);
        assert!(info.aliases.is_empty());
        assert_eq!(info.email, "test@example.com");
        assert_eq!(info.scopes.len(), 0);
        assert!(info.scopes.is_empty());
    }

    #[test]
    fn test_service_account_info_debug() {
        let info = ServiceAccountInfo {
            aliases: vec!["alias".to_string()],
            email: "test@example.com".to_string(),
            scopes: vec!["scope".to_string()],
        };

        let debug_str = format!("{:?}", info);
        assert!(debug_str.contains("ServiceAccountInfo"));
        assert!(debug_str.contains("email"));
        assert!(debug_str.contains("test@example.com"));
    }

    #[test]
    fn test_new_client_creation() {
        // Just verify the client can be created
        let _client = new_client();
        // If we get here without panic, the client was created successfully
    }

    #[test]
    fn test_request_type_all_variants() {
        let variants = vec![
            RequestType::AccessToken,
            RequestType::IdToken,
            RequestType::MdsPing,
            RequestType::ReauthStart,
            RequestType::ReauthContinue,
        ];

        for variant in variants {
            let cloned = variant.clone();
            assert_eq!(variant.to_string(), cloned.to_string());
        }
    }

    #[test]
    fn test_credential_type_all_variants() {
        let variants = vec![
            CredentialType::User,
            CredentialType::ServiceAccountAssertion,
            CredentialType::ServiceAccountJwt,
            CredentialType::ServiceAccountMds,
            CredentialType::ServiceAccountImpersonate,
        ];

        for variant in variants {
            let cloned = variant.clone();
            assert_eq!(variant.to_string(), cloned.to_string());
        }
    }

    #[test]
    fn test_service_account_info_json_roundtrip() {
        let original = ServiceAccountInfo {
            aliases: vec!["alias1".to_string(), "alias2".to_string(), "alias3".to_string()],
            email: "complex-email@subdomain.example.com".to_string(),
            scopes: vec![
                "https://www.googleapis.com/auth/cloud-platform".to_string(),
                "https://www.googleapis.com/auth/compute".to_string(),
                "https://www.googleapis.com/auth/storage.read_only".to_string(),
            ],
        };

        let json = serde_json::to_string(&original).unwrap();
        let deserialized: ServiceAccountInfo = serde_json::from_str(&json).unwrap();

        assert_eq!(original.aliases, deserialized.aliases);
        assert_eq!(original.email, deserialized.email);
        assert_eq!(original.scopes, deserialized.scopes);
    }

    #[test]
    fn test_service_account_info_partial_json() {
        // Test that we can deserialize JSON with minimal fields
        let json = r#"{
            "aliases": [],
            "email": "minimal@example.com",
            "scopes": []
        }"#;

        let info: ServiceAccountInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.email, "minimal@example.com");
        assert!(info.aliases.is_empty());
        assert!(info.scopes.is_empty());
    }

    #[test]
    fn test_https_connector_type_alias() {
        // Verify that HttpsConnector type alias is properly defined
        // This is a compile-time test - if it compiles, the type is correct
        let _connector: Option<HttpsConnector> = None;
    }

    #[test]
    fn test_service_account_info_derive_traits() {
        // Test that ServiceAccountInfo implements Clone (through derive)
        let info = ServiceAccountInfo {
            aliases: vec!["test".to_string()],
            email: "test@example.com".to_string(),
            scopes: vec!["scope".to_string()],
        };
        
        let cloned = info.clone();
        assert_eq!(info.email, cloned.email);
        assert_eq!(info.aliases, cloned.aliases);
        assert_eq!(info.scopes, cloned.scopes);
    }
}