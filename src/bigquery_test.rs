#[cfg(test)]
mod tests {
    use super::super::*;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
    use google_bigquery2::api::TableFieldSchema;
    use serde_json::json;

    #[test]
    fn test_bq_project_creation() {
        let project = BqProject {
            friendly_name: "Test Project".to_string(),
            id: "test-project-123".to_string(),
            numeric_id: "123456789".to_string(),
        };
        
        assert_eq!(project.friendly_name, "Test Project");
        assert_eq!(project.id, "test-project-123");
        assert_eq!(project.numeric_id, "123456789");
    }

    #[test]
    fn test_bq_list_param_new() {
        let param = BqListParam::new();
        assert!(param.max_results.is_none());
        assert!(param.page_token.is_none());
    }

    #[test]
    fn test_bq_list_param_builder() {
        let mut param = BqListParam::new();
        param.max_results(100).page_token("next_page_token");
        
        assert_eq!(param.max_results, Some(100));
        assert_eq!(param.page_token, Some("next_page_token".to_string()));
    }

    #[test]
    fn test_bq_get_query_result_param_new() {
        let param = BqGetQueryResultParam::new(
            &"job-123".to_string(),
            &"page-token-456".to_string(),
        );
        
        assert_eq!(param.job_id, "job-123");
        assert_eq!(param.page_token, "page-token-456");
        assert_eq!(param.max_results, 1000);
        assert!(param.num_result_limit.is_none());
    }

    #[test]
    fn test_bq_get_query_result_param_builder() {
        let mut param = BqGetQueryResultParam::new(
            &"job-123".to_string(),
            &"token".to_string(),
        );
        param.max_results(500).num_result_limit(10000);
        
        assert_eq!(param.max_results, 500);
        assert_eq!(param.num_result_limit, Some(10000));
    }

    #[test]
    fn test_job_status_to_status() {
        assert_eq!(JobStatus::to_status("DONE"), JobStatus::Done);
        assert_eq!(JobStatus::to_status("PENDING"), JobStatus::Pending);
        assert_eq!(JobStatus::to_status("RUNNING"), JobStatus::Running);
        assert_eq!(JobStatus::to_status("UNKNOWN"), JobStatus::Unknown);
        assert_eq!(JobStatus::to_status("OTHER"), JobStatus::Unknown);
    }

    #[test]
    fn test_bq_dataset_new() {
        let dataset = BqDataset::new("my-project", "my-dataset");
        assert_eq!(dataset.project, "my-project");
        assert_eq!(dataset.dataset, "my-dataset");
    }

    #[test]
    fn test_bq_query_to_table_param_new() {
        let query = "SELECT * FROM table".to_string();
        let param = BqQueryToTableParam::new("project-id", "dataset-id", "table-id", &query);
        
        assert_eq!(param.query, "SELECT * FROM table");
        assert_eq!(param.table_ref.project_id, Some("project-id".to_string()));
        assert_eq!(param.table_ref.dataset_id, Some("dataset-id".to_string()));
        assert_eq!(param.table_ref.table_id, Some("table-id".to_string()));
        assert_eq!(param.use_legacy_sql, false);
        assert_eq!(param.dry_run, false);
    }

    #[test]
    fn test_bq_query_to_table_param_builder() {
        let query = "SELECT * FROM table".to_string();
        let mut param = BqQueryToTableParam::new("project", "dataset", "table", &query);
        
        param
            .use_legacy_sql(true)
            .dry_run(true)
            .write_disposition(WriteDisposition::Append);
        
        assert_eq!(param.use_legacy_sql, true);
        assert_eq!(param.dry_run, true);
        matches!(param.write_disposition, WriteDisposition::Append);
    }

    #[test]
    fn test_bq_query_to_table_param_to_query_config() {
        let query = "SELECT * FROM table".to_string();
        let param = BqQueryToTableParam::new("project", "dataset", "table", &query);
        let config = param.to_query_config();
        
        assert_eq!(config.query, Some("SELECT * FROM table".to_string()));
        assert!(config.destination_table.is_some());
        assert_eq!(config.priority, Some("INTERACTIVE".to_string()));
        assert_eq!(config.write_disposition, Some("WRITE_EMPTY".to_string()));
        assert_eq!(config.use_legacy_sql, Some(false));
    }

    #[test]
    fn test_bq_query_to_table_param_write_dispositions() {
        let query = "SELECT 1".to_string();
        
        let mut param = BqQueryToTableParam::new("p", "d", "t", &query);
        param.write_disposition(WriteDisposition::Truncate);
        let config = param.to_query_config();
        assert_eq!(config.write_disposition, Some("WRITE_TRUNCATE".to_string()));
        
        let mut param = BqQueryToTableParam::new("p", "d", "t", &query);
        param.write_disposition(WriteDisposition::Append);
        let config = param.to_query_config();
        assert_eq!(config.write_disposition, Some("WRITE_APPEND".to_string()));
        
        let mut param = BqQueryToTableParam::new("p", "d", "t", &query);
        param.write_disposition(WriteDisposition::Empty);
        let config = param.to_query_config();
        assert_eq!(config.write_disposition, Some("WRITE_EMPTY".to_string()));
    }

    #[test]
    fn test_bq_query_param_new() {
        let query = "SELECT * FROM dataset.table".to_string();
        let param = BqQueryParam::new(&query);
        
        assert_eq!(param.query, "SELECT * FROM dataset.table");
        assert_eq!(param.use_legacy_sql, false);
        assert_eq!(param.max_results, 1000);
        assert!(param.num_result_limit.is_none());
        assert_eq!(param.dry_run, false);
    }

    #[test]
    fn test_bq_query_param_builder() {
        let query = "SELECT 1".to_string();
        let mut param = BqQueryParam::new(&query);
        
        param
            .use_legacy_sql(true)
            .max_results(500)
            .num_result_limit(2000)
            .dry_run(true);
        
        assert_eq!(param.use_legacy_sql, true);
        assert_eq!(param.max_results, 500);
        assert_eq!(param.num_result_limit, Some(2000));
        assert_eq!(param.dry_run, true);
    }

    #[test]
    fn test_bq_query_param_into_query_request() {
        let query = "SELECT 1".to_string();
        let param = BqQueryParam::new(&query);
        let request: QueryRequest = param.into();
        
        assert_eq!(request.query, Some("SELECT 1".to_string()));
        assert_eq!(request.max_results, Some(1000));
        assert_eq!(request.use_legacy_sql, Some(false));
        assert_eq!(request.dry_run, Some(false));
    }

    #[test]
    fn test_bq_query_param_ref_into_query_request() {
        let query = "SELECT 1".to_string();
        let param = BqQueryParam::new(&query);
        let request: QueryRequest = (&param).into();
        
        assert_eq!(request.query, Some("SELECT 1".to_string()));
        assert_eq!(request.max_results, Some(1000));
    }

    #[test]
    fn test_bq_create_table_param_new() {
        let param = BqCreateTableParam::new();
        assert!(param.description.is_none());
        assert!(param.schema.is_none());
    }

    #[test]
    fn test_bq_insert_all_param_new() {
        let param = BqInsertAllParam::new("my-dataset", "my-table");
        
        assert_eq!(param.dataset, "my-dataset");
        assert_eq!(param.table, "my-table");
        assert_eq!(param.skip_invalid_rows, false);
        assert_eq!(param.ignore_unknown_values, false);
        assert!(param.trace_id.is_none());
    }

    #[test]
    fn test_bq_insert_all_param_builder() {
        let mut param = BqInsertAllParam::new("dataset", "table");
        
        param
            .skip_invalid_rows(true)
            .ignore_unknown_value(true);
        
        assert_eq!(param.skip_invalid_rows, true);
        assert_eq!(param.ignore_unknown_values, true);
    }

    #[test]
    fn test_bq_insert_all_param_set_trace_id() {
        let mut param = BqInsertAllParam::new("dataset", "table");
        let trace_id = param.set_trace_id();
        
        assert!(trace_id.is_some());
        assert!(param.trace_id.is_some());
        // Verify it's a valid UUID format
        assert!(param.trace_id.as_ref().unwrap().len() == 36);
    }

    #[test]
    fn test_bq_table_new() {
        let table = BqTable::new("my-project", "my-dataset", "my-table");
        
        assert_eq!(table.dataset.project, "my-project");
        assert_eq!(table.dataset.dataset, "my-dataset");
        assert_eq!(table.table_id, "my-table");
        assert!(table.schemas.is_none());
        assert!(table.created_at.is_none());
        assert!(table.expired_at.is_none());
    }

    #[test]
    fn test_bq_table_schema_to_table_field_schema() {
        let schema = BqTableSchema {
            name: Some("field_name".to_string()),
            type_: BqType::STRING,
            mode: BqMode::REQUIRED,
            fields: Box::new(vec![]),
            description: Some("Test field".to_string()),
        };
        
        let field_schema = schema.to_table_field_schema();
        
        assert_eq!(field_schema.name, Some("field_name".to_string()));
        assert_eq!(field_schema.type_, Some("STRING".to_string()));
        assert_eq!(field_schema.mode, Some("REQUIRED".to_string()));
        assert!(field_schema.fields.is_none());
    }

    #[test]
    fn test_bq_table_schema_all_types() {
        let test_cases = vec![
            (BqType::STRING, "STRING"),
            (BqType::FLOAT, "NUMERIC"),
            (BqType::INTEGER, "INTEGER"),
            (BqType::BOOLEAN, "BOOLEAN"),
            (BqType::TIMESTAMP, "TIMESTAMP"),
            (BqType::DATETIME, "DATETIME"),
            (BqType::DATE, "DATE"),
            (BqType::TIME, "TIME"),
            (BqType::RECORD, "RECORD"),
            (BqType::JSON, "JSON"),
        ];
        
        for (bq_type, expected) in test_cases {
            let schema = BqTableSchema {
                name: Some("test".to_string()),
                type_: bq_type,
                mode: BqMode::NULLABLE,
                fields: Box::new(vec![]),
                description: None,
            };
            let field_schema = schema.to_table_field_schema();
            assert_eq!(field_schema.type_, Some(expected.to_string()));
        }
    }

    #[test]
    fn test_bq_table_schema_all_modes() {
        let test_cases = vec![
            (BqMode::REQUIRED, "REQUIRED"),
            (BqMode::NULLABLE, "NULLABLE"),
            (BqMode::REPEATED, "REPEATED"),
        ];
        
        for (bq_mode, expected) in test_cases {
            let schema = BqTableSchema {
                name: Some("test".to_string()),
                type_: BqType::STRING,
                mode: bq_mode,
                fields: Box::new(vec![]),
                description: None,
            };
            let field_schema = schema.to_table_field_schema();
            assert_eq!(field_schema.mode, Some(expected.to_string()));
        }
    }

    #[test]
    fn test_bq_table_schema_with_nested_fields() {
        let nested_field = BqTableSchema {
            name: Some("nested".to_string()),
            type_: BqType::STRING,
            mode: BqMode::NULLABLE,
            fields: Box::new(vec![]),
            description: None,
        };
        
        let schema = BqTableSchema {
            name: Some("parent".to_string()),
            type_: BqType::RECORD,
            mode: BqMode::REQUIRED,
            fields: Box::new(vec![nested_field]),
            description: None,
        };
        
        let field_schema = schema.to_table_field_schema();
        
        assert!(field_schema.fields.is_some());
        let fields = field_schema.fields.unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, Some("nested".to_string()));
    }

    #[test]
    fn test_bq_table_schema_from_table_field_schema() {
        let mut field_schema = TableFieldSchema::default();
        field_schema.name = Some("test_field".to_string());
        field_schema.type_ = Some("STRING".to_string());
        field_schema.mode = Some("REQUIRED".to_string());
        field_schema.description = Some("A test field".to_string());
        
        let bq_schema = BqTableSchema::from_table_field_schema(&field_schema);
        
        assert_eq!(bq_schema.name, Some("test_field".to_string()));
        assert_eq!(bq_schema.type_, BqType::STRING);
        assert_eq!(bq_schema.mode, BqMode::REQUIRED);
        assert_eq!(bq_schema.description, Some("A test field".to_string()));
    }

    #[test]
    fn test_bq_table_schema_from_table_field_schema_all_types() {
        let test_cases = vec![
            ("STRING", BqType::STRING),
            ("FLOAT", BqType::FLOAT),
            ("INTEGER", BqType::INTEGER),
            ("NUMERIC", BqType::FLOAT),
            ("BOOLEAN", BqType::BOOLEAN),
            ("TIMESTAMP", BqType::TIMESTAMP),
            ("DATE", BqType::DATE),
            ("DATETIME", BqType::DATETIME),
            ("TIME", BqType::TIME),
            ("RECORD", BqType::RECORD),
            ("JSON", BqType::JSON),
            ("UNKNOWN_TYPE", BqType::UNKNOWN),
        ];
        
        for (type_str, expected_type) in test_cases {
            let mut field_schema = TableFieldSchema::default();
            field_schema.name = Some("test".to_string());
            field_schema.type_ = Some(type_str.to_string());
            
            let bq_schema = BqTableSchema::from_table_field_schema(&field_schema);
            assert_eq!(bq_schema.type_, expected_type);
        }
    }

    #[test]
    fn test_bq_row_new() {
        let columns = vec![
            BqColumn {
                name: Some("col1".to_string()),
                value: BqValue::BqString("value1".to_string()),
            },
            BqColumn {
                name: Some("col2".to_string()),
                value: BqValue::BqInteger(42),
            },
        ];
        
        let row = BqRow::new(columns.clone());
        
        assert_eq!(row.len(), 2);
        assert_eq!(row.columns().len(), 2);
    }

    #[test]
    fn test_bq_row_get() {
        let columns = vec![
            BqColumn {
                name: Some("name".to_string()),
                value: BqValue::BqString("John".to_string()),
            },
            BqColumn {
                name: Some("age".to_string()),
                value: BqValue::BqInteger(30),
            },
        ];
        
        let row = BqRow::new(columns);
        
        match row.get("name") {
            Some(BqValue::BqString(s)) => assert_eq!(s, "John"),
            _ => panic!("Expected BqString"),
        }
        
        match row.get("age") {
            Some(BqValue::BqInteger(i)) => assert_eq!(*i, 30),
            _ => panic!("Expected BqInteger"),
        }
        
        assert!(row.get("nonexistent").is_none());
    }

    #[test]
    fn test_bq_row_to_string() {
        let columns = vec![
            BqColumn {
                name: Some("field1".to_string()),
                value: BqValue::BqString("test".to_string()),
            },
            BqColumn {
                name: Some("field2".to_string()),
                value: BqValue::BqInteger(123),
            },
        ];
        
        let row = BqRow::new(columns);
        let str = row.to_string();
        
        assert!(str.contains("field1"));
        assert!(str.contains("test"));
        assert!(str.contains("field2"));
        assert!(str.contains("123"));
    }

    #[test]
    fn test_bq_column_value_to_bq_value_string() {
        let schema = BqTableSchema {
            name: Some("test".to_string()),
            type_: BqType::STRING,
            mode: BqMode::NULLABLE,
            fields: Box::new(vec![]),
            description: None,
        };
        
        let value = BqColumn::value_to_bq_value(
            Some(json!("test_string")),
            &schema,
        );
        
        match value {
            BqValue::BqString(s) => assert_eq!(s, "test_string"),
            _ => panic!("Expected BqString"),
        }
    }

    #[test]
    fn test_bq_column_value_to_bq_value_integer() {
        let schema = BqTableSchema {
            name: Some("test".to_string()),
            type_: BqType::INTEGER,
            mode: BqMode::NULLABLE,
            fields: Box::new(vec![]),
            description: None,
        };
        
        // From string
        let value = BqColumn::value_to_bq_value(
            Some(json!("42")),
            &schema,
        );
        match value {
            BqValue::BqInteger(i) => assert_eq!(i, 42),
            _ => panic!("Expected BqInteger"),
        }
        
        // From number
        let value = BqColumn::value_to_bq_value(
            Some(json!(42)),
            &schema,
        );
        match value {
            BqValue::BqInteger(i) => assert_eq!(i, 42),
            _ => panic!("Expected BqInteger"),
        }
    }

    #[test]
    fn test_bq_column_value_to_bq_value_float() {
        let schema = BqTableSchema {
            name: Some("test".to_string()),
            type_: BqType::FLOAT,
            mode: BqMode::NULLABLE,
            fields: Box::new(vec![]),
            description: None,
        };
        
        // From string
        let value = BqColumn::value_to_bq_value(
            Some(json!("3.14")),
            &schema,
        );
        match value {
            BqValue::BqFloat(f) => assert_eq!(f, 3.14),
            _ => panic!("Expected BqFloat"),
        }
        
        // From number
        let value = BqColumn::value_to_bq_value(
            Some(json!(3.14)),
            &schema,
        );
        match value {
            BqValue::BqFloat(f) => assert_eq!(f, 3.14),
            _ => panic!("Expected BqFloat"),
        }
    }

    #[test]
    fn test_bq_column_value_to_bq_value_boolean() {
        let schema = BqTableSchema {
            name: Some("test".to_string()),
            type_: BqType::BOOLEAN,
            mode: BqMode::NULLABLE,
            fields: Box::new(vec![]),
            description: None,
        };
        
        // From string
        let value = BqColumn::value_to_bq_value(
            Some(json!("true")),
            &schema,
        );
        match value {
            BqValue::BqBool(b) => assert_eq!(b, true),
            _ => panic!("Expected BqBool"),
        }
        
        // From bool
        let value = BqColumn::value_to_bq_value(
            Some(json!(false)),
            &schema,
        );
        match value {
            BqValue::BqBool(b) => assert_eq!(b, false),
            _ => panic!("Expected BqBool"),
        }
    }

    #[test]
    fn test_bq_column_value_to_bq_value_date() {
        let schema = BqTableSchema {
            name: Some("test".to_string()),
            type_: BqType::DATE,
            mode: BqMode::NULLABLE,
            fields: Box::new(vec![]),
            description: None,
        };
        
        let value = BqColumn::value_to_bq_value(
            Some(json!("2023-12-25")),
            &schema,
        );
        
        match value {
            BqValue::BqDate(d) => {
                assert_eq!(d.year(), 2023);
                assert_eq!(d.month(), 12);
                assert_eq!(d.day(), 25);
            },
            _ => panic!("Expected BqDate"),
        }
    }

    #[test]
    fn test_bq_column_value_to_bq_value_time() {
        let schema = BqTableSchema {
            name: Some("test".to_string()),
            type_: BqType::TIME,
            mode: BqMode::NULLABLE,
            fields: Box::new(vec![]),
            description: None,
        };
        
        let value = BqColumn::value_to_bq_value(
            Some(json!("14:30:00")),
            &schema,
        );
        
        match value {
            BqValue::BqTime(t) => {
                assert_eq!(t.hour(), 14);
                assert_eq!(t.minute(), 30);
                assert_eq!(t.second(), 0);
            },
            _ => panic!("Expected BqTime"),
        }
    }

    #[test]
    fn test_bq_column_value_to_bq_value_datetime() {
        let schema = BqTableSchema {
            name: Some("test".to_string()),
            type_: BqType::DATETIME,
            mode: BqMode::NULLABLE,
            fields: Box::new(vec![]),
            description: None,
        };
        
        let value = BqColumn::value_to_bq_value(
            Some(json!("2023-12-25T14:30:00.123456")),
            &schema,
        );
        
        match value {
            BqValue::BqDateTime(dt) => {
                assert_eq!(dt.year(), 2023);
                assert_eq!(dt.month(), 12);
                assert_eq!(dt.day(), 25);
                assert_eq!(dt.hour(), 14);
                assert_eq!(dt.minute(), 30);
                assert_eq!(dt.second(), 0);
            },
            _ => panic!("Expected BqDateTime"),
        }
    }

    #[test]
    fn test_bq_column_value_to_bq_value_array() {
        let schema = BqTableSchema {
            name: Some("test".to_string()),
            type_: BqType::STRING,
            mode: BqMode::REPEATED,
            fields: Box::new(vec![]),
            description: None,
        };
        
        let value = BqColumn::value_to_bq_value(
            Some(json!(["item1", "item2", "item3"])),
            &schema,
        );
        
        match value {
            BqValue::BqRepeated(items) => {
                assert_eq!(items.len(), 3);
                match &*items[0] {
                    BqValue::BqString(s) => assert_eq!(s, "item1"),
                    _ => panic!("Expected BqString in array"),
                }
            },
            _ => panic!("Expected BqRepeated"),
        }
    }

    #[test]
    fn test_bq_column_value_to_bq_value_null() {
        let schema = BqTableSchema {
            name: Some("test".to_string()),
            type_: BqType::STRING,
            mode: BqMode::NULLABLE,
            fields: Box::new(vec![]),
            description: None,
        };
        
        let value = BqColumn::value_to_bq_value(None, &schema);
        assert!(matches!(value, BqValue::BqNull));
        
        let value = BqColumn::value_to_bq_value(Some(json!(null)), &schema);
        assert!(matches!(value, BqValue::BqNull));
    }

    #[test]
    fn test_bq_column_name_and_value() {
        let column = BqColumn {
            name: Some("test_col".to_string()),
            value: BqValue::BqString("test_value".to_string()),
        };
        
        assert_eq!(column.name(), Some("test_col".to_string()));
        match column.value() {
            BqValue::BqString(s) => assert_eq!(s, "test_value"),
            _ => panic!("Expected BqString"),
        }
    }

    #[test]
    fn test_bq_column_to_string() {
        let column = BqColumn {
            name: Some("name".to_string()),
            value: BqValue::BqString("John".to_string()),
        };
        let str = column.to_string();
        assert!(str.contains("name"));
        assert!(str.contains("John"));
        
        let column = BqColumn {
            name: None,
            value: BqValue::BqInteger(42),
        };
        let str = column.to_string();
        assert_eq!(str, "42");
        
        let column = BqColumn {
            name: Some("null_field".to_string()),
            value: BqValue::BqNull,
        };
        let str = column.to_string();
        assert_eq!(str, "");
    }

    #[test]
    fn test_bq_value_to_string() {
        assert_eq!(BqValue::BqString("test".to_string()).to_string(), r#""test""#);
        assert_eq!(BqValue::BqInteger(42).to_string(), "42");
        assert_eq!(BqValue::BqFloat(3.14).to_string(), "3.14");
        assert_eq!(BqValue::BqBool(true).to_string(), "true");
        assert_eq!(BqValue::BqNull.to_string(), "null");
        
        let date = NaiveDate::from_ymd_opt(2023, 12, 25).unwrap();
        assert_eq!(BqValue::BqDate(date).to_string(), r#""2023-12-25""#);
        
        let time = NaiveTime::from_hms_opt(14, 30, 0).unwrap();
        assert_eq!(BqValue::BqTime(time).to_string(), r#""14:30:00""#);
        
        let datetime = NaiveDateTime::new(date, time);
        assert_eq!(
            BqValue::BqDateTime(datetime).to_string(),
            r#""2023-12-25T14:30:00.000000""#
        );
        
        let timestamp = Utc.timestamp_opt(1703505000, 0).unwrap();
        assert!(BqValue::BqTimestamp(timestamp).to_string().contains("2023"));
    }

    #[test]
    fn test_bq_value_repeated_to_string() {
        let values = vec![
            Box::new(BqValue::BqString("a".to_string())),
            Box::new(BqValue::BqString("b".to_string())),
            Box::new(BqValue::BqString("c".to_string())),
        ];
        let repeated = BqValue::BqRepeated(values);
        assert_eq!(repeated.to_string(), r#"["a","b","c"]"#);
    }

    #[test]
    fn test_bq_value_struct_to_string() {
        let columns = vec![
            BqColumn {
                name: Some("field1".to_string()),
                value: BqValue::BqString("value1".to_string()),
            },
            BqColumn {
                name: Some("field2".to_string()),
                value: BqValue::BqInteger(42),
            },
        ];
        let row = BqRow::new(columns);
        let struct_val = BqValue::BqStruct(row);
        let str = struct_val.to_string();
        
        assert!(str.contains("field1"));
        assert!(str.contains("value1"));
        assert!(str.contains("field2"));
        assert!(str.contains("42"));
    }

    #[test]
    fn test_bq_job_result_default() {
        let result = BqJobResult::default();
        assert!(result.self_link.is_none());
        assert!(result.job_id.is_none());
        assert_eq!(result.status, JobStatus::Unknown);
        assert!(result.error_message.is_none());
        assert!(result.error_reason.is_none());
    }

    #[test]
    fn test_serialization_bq_project() {
        let project = BqProject {
            friendly_name: "Test".to_string(),
            id: "test-id".to_string(),
            numeric_id: "123".to_string(),
        };
        
        let json = serde_json::to_string(&project).unwrap();
        let deserialized: BqProject = serde_json::from_str(&json).unwrap();
        
        assert_eq!(project.friendly_name, deserialized.friendly_name);
        assert_eq!(project.id, deserialized.id);
        assert_eq!(project.numeric_id, deserialized.numeric_id);
    }

    #[test]
    fn test_serialization_bq_dataset() {
        let dataset = BqDataset::new("project", "dataset");
        
        let json = serde_json::to_string(&dataset).unwrap();
        let deserialized: BqDataset = serde_json::from_str(&json).unwrap();
        
        assert_eq!(dataset.project, deserialized.project);
        assert_eq!(dataset.dataset, deserialized.dataset);
    }

    #[test]
    fn test_serialization_bq_table() {
        let table = BqTable::new("project", "dataset", "table");
        
        let json = serde_json::to_string(&table).unwrap();
        let deserialized: BqTable = serde_json::from_str(&json).unwrap();
        
        assert_eq!(table.table_id, deserialized.table_id);
        assert_eq!(table.dataset.project, deserialized.dataset.project);
        assert_eq!(table.dataset.dataset, deserialized.dataset.dataset);
    }

    #[test]
    fn test_serialization_bq_insert_all_param() {
        let mut param = BqInsertAllParam::new("dataset", "table");
        param.skip_invalid_rows(true);
        
        let json = serde_json::to_string(&param).unwrap();
        let deserialized: BqInsertAllParam = serde_json::from_str(&json).unwrap();
        
        assert_eq!(param.dataset, deserialized.dataset);
        assert_eq!(param.table, deserialized.table);
        assert_eq!(param.skip_invalid_rows, deserialized.skip_invalid_rows);
    }
}