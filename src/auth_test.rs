#[cfg(test)]
mod tests {
    #![allow(clippy::all)]
    use super::super::*;
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    #[test]
    fn test_installed_flow_browser_delegate_clone() {
        let delegate = InstalledFlowBrowserDelegate;
        let cloned = delegate.clone();
        // Both should be unit structs
        assert!(std::mem::size_of_val(&delegate) == std::mem::size_of_val(&cloned));
    }

    #[test]
    fn test_gcp_auth_clone() {
        // Note: We can't easily test the actual GcpAuth clone without a real authenticator,
        // but we can verify the struct implements Clone trait
        // This is a compile-time test
        fn assert_clone<T: Clone>() {}
        assert_clone::<GcpAuth>();
    }

    #[test]
    fn test_new_client() {
        // Verify that new_client() creates a client successfully
        let _client = new_client();
        // If we get here without panic, the client was created successfully
    }

    #[test]
    fn test_get_iat_with_valid_timestamp() {
        let timestamp = 1609459200; // 2021-01-01 00:00:00 UTC
        let claim = json!({
            "iat": timestamp,
            "exp": timestamp + 3600,
            "iss": "test-issuer"
        });

        let iat = get_iat(&claim);
        assert!(iat.is_some());

        let expected = Utc.timestamp_opt(timestamp, 0).unwrap();
        assert_eq!(iat.unwrap(), expected);
    }

    #[test]
    fn test_get_iat_with_missing_field() {
        let claim = json!({
            "exp": 1609462800,
            "iss": "test-issuer"
        });

        let iat = get_iat(&claim);
        assert!(iat.is_none());
    }

    #[test]
    fn test_get_iat_with_non_number_value() {
        let claim = json!({
            "iat": "not-a-number",
            "exp": 1609462800,
            "iss": "test-issuer"
        });

        let iat = get_iat(&claim);
        assert!(iat.is_none());
    }

    #[test]
    fn test_get_exp_with_valid_timestamp() {
        let timestamp = 1609462800; // 2021-01-01 01:00:00 UTC
        let claim = json!({
            "iat": 1609459200,
            "exp": timestamp,
            "iss": "test-issuer"
        });

        let exp = get_exp(&claim);
        assert!(exp.is_some());

        let expected = Utc.timestamp_opt(timestamp, 0).unwrap();
        assert_eq!(exp.unwrap(), expected);
    }

    #[test]
    fn test_get_exp_with_missing_field() {
        let claim = json!({
            "iat": 1609459200,
            "iss": "test-issuer"
        });

        let exp = get_exp(&claim);
        assert!(exp.is_none());
    }

    #[test]
    fn test_get_exp_with_non_number_value() {
        let claim = json!({
            "iat": 1609459200,
            "exp": "not-a-number",
            "iss": "test-issuer"
        });

        let exp = get_exp(&claim);
        assert!(exp.is_none());
    }

    #[test]
    fn test_get_iat_and_exp_with_float_timestamps() {
        let claim = json!({
            "iat": 1609459200.5,
            "exp": 1609462800.9
        });

        let iat = get_iat(&claim);
        let exp = get_exp(&claim);

        // Float values in JSON won't convert to i64, so they should return None
        assert!(iat.is_none());
        assert!(exp.is_none());
    }

    #[test]
    fn test_google_oauth2_certs_url_constant() {
        assert_eq!(
            GOOGLE_OAUTH2_CERTS_URL,
            "https://www.googleapis.com/oauth2/v1/certs"
        );
    }

    #[test]
    fn test_https_connector_type_alias() {
        // Verify that HttpsConnector type alias is properly defined
        // This is a compile-time test - if it compiles, the type is correct
        let _connector: Option<HttpsConnector> = None;
    }

    #[test]
    fn test_get_iat_with_null_value() {
        let claim = json!({
            "iat": null,
            "exp": 1609462800,
            "iss": "test-issuer"
        });

        let iat = get_iat(&claim);
        assert!(iat.is_none());
    }

    #[test]
    fn test_get_exp_with_null_value() {
        let claim = json!({
            "iat": 1609459200,
            "exp": null,
            "iss": "test-issuer"
        });

        let exp = get_exp(&claim);
        assert!(exp.is_none());
    }

    #[test]
    fn test_get_iat_with_array_value() {
        let claim = json!({
            "iat": [1609459200],
            "exp": 1609462800,
            "iss": "test-issuer"
        });

        let iat = get_iat(&claim);
        assert!(iat.is_none());
    }

    #[test]
    fn test_get_exp_with_object_value() {
        let claim = json!({
            "iat": 1609459200,
            "exp": {"timestamp": 1609462800},
            "iss": "test-issuer"
        });

        let exp = get_exp(&claim);
        assert!(exp.is_none());
    }

    #[test]
    fn test_get_iat_with_negative_timestamp() {
        let claim = json!({
            "iat": -1000,
            "exp": 1609462800
        });

        let iat = get_iat(&claim);
        assert!(iat.is_some());
        assert_eq!(iat.unwrap(), Utc.timestamp_opt(-1000, 0).unwrap());
    }

    #[test]
    fn test_get_exp_with_large_timestamp() {
        let large_timestamp = 253402300799i64; // Max timestamp before year 10000
        let claim = json!({
            "iat": 1609459200,
            "exp": large_timestamp
        });

        let exp = get_exp(&claim);
        assert!(exp.is_some());
        assert_eq!(exp.unwrap(), Utc.timestamp_opt(large_timestamp, 0).unwrap());
    }

    #[test]
    fn test_empty_claim_object() {
        let claim = json!({});

        let iat = get_iat(&claim);
        let exp = get_exp(&claim);

        assert!(iat.is_none());
        assert!(exp.is_none());
    }

    #[test]
    fn test_claim_with_nested_iat_exp() {
        let claim = json!({
            "nested": {
                "iat": 1609459200,
                "exp": 1609462800
            },
            "iss": "test-issuer"
        });

        // Should not find iat/exp in nested objects
        let iat = get_iat(&claim);
        let exp = get_exp(&claim);

        assert!(iat.is_none());
        assert!(exp.is_none());
    }

    #[test]
    fn test_claim_with_string_numbers() {
        let claim = json!({
            "iat": "1609459200",
            "exp": "1609462800"
        });

        // String representations of numbers should not be parsed
        let iat = get_iat(&claim);
        let exp = get_exp(&claim);

        assert!(iat.is_none());
        assert!(exp.is_none());
    }

    #[test]
    fn test_claim_with_boolean_values() {
        let claim = json!({
            "iat": true,
            "exp": false
        });

        let iat = get_iat(&claim);
        let exp = get_exp(&claim);

        assert!(iat.is_none());
        assert!(exp.is_none());
    }

    #[test]
    fn test_typical_jwt_claim() {
        let now = Utc::now().timestamp();
        let claim = json!({
            "iss": "https://accounts.google.com",
            "sub": "1234567890",
            "aud": "test-audience",
            "iat": now,
            "exp": now + 3600,
            "azp": "test-azp",
            "email": "test@example.com",
            "email_verified": true
        });

        let iat = get_iat(&claim);
        let exp = get_exp(&claim);

        assert!(iat.is_some());
        assert!(exp.is_some());
        assert_eq!(iat.unwrap(), Utc.timestamp_opt(now, 0).unwrap());
        assert_eq!(exp.unwrap(), Utc.timestamp_opt(now + 3600, 0).unwrap());
    }

    #[test]
    fn test_claim_with_zero_timestamps() {
        let claim = json!({
            "iat": 0,
            "exp": 0
        });

        let iat = get_iat(&claim);
        let exp = get_exp(&claim);

        assert!(iat.is_some());
        assert!(exp.is_some());
        assert_eq!(iat.unwrap(), Utc.timestamp_opt(0, 0).unwrap());
        assert_eq!(exp.unwrap(), Utc.timestamp_opt(0, 0).unwrap());
    }

    // Note: Async tests for browser_user_url are commented out as they require
    // interactive environment and can hang in CI
    // #[tokio::test]
    // async fn test_browser_user_url_with_code_needed() {
    //     let url = "https://example.com/auth";
    //     let result = browser_user_url(url, true).await;
    //     assert!(result.is_err());
    // }

    // #[tokio::test]
    // async fn test_browser_user_url_without_code_needed() {
    //     let url = "https://example.com/success";
    //     let result = browser_user_url(url, false).await;
    //     assert!(result.is_ok());
    //     assert_eq!(result.unwrap(), "");
    // }

    #[test]
    fn test_installed_flow_delegate_trait_implementation() {
        // Verify that InstalledFlowBrowserDelegate implements the trait
        fn assert_implements_trait<T: InstalledFlowDelegate>() {}
        assert_implements_trait::<InstalledFlowBrowserDelegate>();
    }

    // Note: Async test for InstalledFlowBrowserDelegate is commented out as it requires
    // interactive environment and can hang in CI
    // #[tokio::test]
    // async fn test_installed_flow_browser_delegate_present_user_url() {
    //     let delegate = InstalledFlowBrowserDelegate;
    //     let url = "https://example.com/auth";
    //
    //     // Test with need_code = true
    //     let future = delegate.present_user_url(url, true);
    //     let result = future.await;
    //     assert!(result.is_err());
    //
    //     // Test with need_code = false
    //     let future = delegate.present_user_url(url, false);
    //     let result = future.await;
    //     assert!(result.is_ok());
    // }

    #[test]
    fn test_datetime_edge_cases() {
        // Test with large but valid timestamps
        // Using smaller values that are still valid for chrono
        let claim = json!({
            "iat": -62135596800i64, // Around year 0
            "exp": 253402300799i64  // Around year 9999
        });

        // These should work without panic
        let iat = get_iat(&claim);
        let exp = get_exp(&claim);

        // Both should succeed with valid timestamps
        assert!(iat.is_some());
        assert!(exp.is_some());
    }

    #[test]
    fn test_copy_trait_for_delegate() {
        // Verify InstalledFlowBrowserDelegate implements Copy
        fn assert_copy<T: Copy>() {}
        assert_copy::<InstalledFlowBrowserDelegate>();

        let delegate1 = InstalledFlowBrowserDelegate;
        let _delegate2 = delegate1; // Copy
        let _delegate3 = delegate1; // Can still use delegate1 because it's Copy
    }
}
