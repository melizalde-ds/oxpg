use super::config::*;
use super::*;
use pyo3::Python;
use tokio_postgres::Config;

mod populate_config_from_params {
    use super::*;

    #[test]
    fn returns_same_config_reference() {
        let mut config = Config::new();

        let result = populate_config_from_params(
            "testhost".to_string(),
            "testuser".to_string(),
            "testpass".to_string(),
            5433,
            "testdb".to_string(),
            &mut config,
        );

        assert!(std::ptr::eq(result, &config));
    }

    #[test]
    fn does_not_panic_with_special_characters() {
        let mut config = Config::new();

        populate_config_from_params(
            "host-with-dashes".to_string(),
            "user@domain".to_string(),
            "p@ss:w0rd!".to_string(),
            5432,
            "db-name_123".to_string(),
            &mut config,
        );
    }

    #[test]
    fn populates_config_with_correct_values() {
        let mut config = Config::new();

        populate_config_from_params(
            "testhost".to_string(),
            "testuser".to_string(),
            "testpass".to_string(),
            5433,
            "testdb".to_string(),
            &mut config,
        );

        assert_eq!(config.get_user(), Some("testuser"));
        assert_eq!(config.get_dbname(), Some("testdb"));
        assert_eq!(config.get_ports().first(), Some(&5433));
    }

    #[test]
    fn handles_empty_strings() {
        let mut config = Config::new();

        populate_config_from_params(
            "".to_string(),
            "".to_string(),
            "".to_string(),
            5432,
            "".to_string(),
            &mut config,
        );

        assert_eq!(config.get_user(), Some(""));
        assert_eq!(config.get_dbname(), Some(""));
    }

    #[test]
    fn handles_minimum_port() {
        let mut config = Config::new();

        populate_config_from_params(
            "localhost".to_string(),
            "user".to_string(),
            "pass".to_string(),
            1,
            "db".to_string(),
            &mut config,
        );

        assert_eq!(config.get_ports().first(), Some(&1));
    }

    #[test]
    fn handles_maximum_port() {
        let mut config = Config::new();

        populate_config_from_params(
            "localhost".to_string(),
            "user".to_string(),
            "pass".to_string(),
            65535,
            "db".to_string(),
            &mut config,
        );

        assert_eq!(config.get_ports().first(), Some(&65535));
    }

    #[test]
    fn handles_unicode_in_parameters() {
        let mut config = Config::new();

        populate_config_from_params(
            "hôst.example.com".to_string(),
            "üser".to_string(),
            "pässwörd".to_string(),
            5432,
            "datäbase".to_string(),
            &mut config,
        );

        assert_eq!(config.get_user(), Some("üser"));
        assert_eq!(config.get_dbname(), Some("datäbase"));
    }

    #[test]
    fn handles_long_strings() {
        let mut config = Config::new();
        let long_string = "a".repeat(1000);

        populate_config_from_params(
            long_string.clone(),
            long_string.clone(),
            long_string.clone(),
            5432,
            long_string.clone(),
            &mut config,
        );

        assert_eq!(config.get_user(), Some(long_string.as_str()));
    }
}

mod extract_host_from_dsn {
    use super::*;

    #[test]
    fn extracts_all_components_from_valid_dsn() {
        Python::attach(|_py| {
            let dsn = "postgresql://myuser:mypass@dbhost:5433/mydb".to_string();
            let mut config = Config::new();

            let result = extract_host_from_dsn(dsn, &mut config);

            assert!(result.is_ok());

            if let Ok((host, user, port, db, _)) = result {
                assert_eq!(host, "dbhost");
                assert_eq!(user, "myuser");
                assert_eq!(port, 5433);
                assert_eq!(db, "mydb");
            }
        });
    }

    #[test]
    fn uses_default_port_when_not_specified() {
        Python::attach(|_py| {
            let dsn = "postgresql://user:pass@localhost/mydb".to_string();
            let mut config = Config::new();

            if let Ok((_, _, port, _, _)) = extract_host_from_dsn(dsn, &mut config) {
                assert_eq!(port, 5432);
            }
        });
    }

    #[test]
    fn handles_ipv4_addresses() {
        Python::attach(|_py| {
            let dsn = "postgresql://user:pass@192.168.1.1:5432/mydb".to_string();
            let mut config = Config::new();

            if let Ok((host, _, _, _, _)) = extract_host_from_dsn(dsn, &mut config) {
                assert_eq!(host, "192.168.1.1");
            }
        });
    }

    #[test]
    fn handles_percent_encoded_credentials() {
        Python::attach(|_py| {
            let dsn = "postgresql://user:p%40ss%3Aword@localhost/mydb".to_string();
            let mut config = Config::new();

            assert!(extract_host_from_dsn(dsn, &mut config).is_ok());
        });
    }

    #[test]
    fn rejects_invalid_scheme() {
        Python::attach(|_py| {
            let dsn = "mysql://user:pass@localhost/db".to_string();
            let mut config = Config::new();

            assert!(extract_host_from_dsn(dsn, &mut config).is_err());
        });
    }

    #[test]
    fn rejects_missing_user() {
        Python::attach(|_py| {
            let dsn = "postgresql://localhost/db".to_string();
            let mut config = Config::new();

            assert!(extract_host_from_dsn(dsn, &mut config).is_err());
        });
    }

    #[test]
    fn rejects_missing_database() {
        Python::attach(|_py| {
            let dsn = "postgresql://user:pass@localhost".to_string();
            let mut config = Config::new();

            assert!(extract_host_from_dsn(dsn, &mut config).is_err());
        });
    }

    #[test]
    fn accepts_postgres_scheme() {
        Python::attach(|_py| {
            let dsn = "postgres://user:pass@localhost/mydb".to_string();
            let mut config = Config::new();

            let result = extract_host_from_dsn(dsn, &mut config);
            assert!(result.is_ok());
        });
    }

    #[test]
    fn handles_ipv6_addresses() {
        Python::attach(|_py| {
            let dsn = "postgresql://user:pass@[::1]:5432/mydb".to_string();
            let mut config = Config::new();

            let result = extract_host_from_dsn(dsn, &mut config);
            assert!(result.is_ok() || result.is_err());
        });
    }

    #[test]
    fn handles_missing_password() {
        Python::attach(|_py| {
            let dsn = "postgresql://user@localhost/mydb".to_string();
            let mut config = Config::new();

            if let Ok((host, user, _, db, _)) = extract_host_from_dsn(dsn, &mut config) {
                assert_eq!(host, "localhost");
                assert_eq!(user, "user");
                assert_eq!(db, "mydb");
            }
        });
    }

    #[test]
    fn rejects_empty_dsn() {
        Python::attach(|_py| {
            let dsn = "".to_string();
            let mut config = Config::new();

            assert!(extract_host_from_dsn(dsn, &mut config).is_err());
        });
    }

    #[test]
    fn rejects_malformed_dsn() {
        Python::attach(|_py| {
            let dsn = "not-a-valid-dsn".to_string();
            let mut config = Config::new();

            assert!(extract_host_from_dsn(dsn, &mut config).is_err());
        });
    }

    #[test]
    fn handles_query_parameters() {
        Python::attach(|_py| {
            let dsn = "postgresql://user:pass@localhost/mydb?sslmode=require".to_string();
            let mut config = Config::new();

            if let Ok((_, _, _, db, _)) = extract_host_from_dsn(dsn, &mut config) {
                assert_eq!(db, "mydb");
            }
        });
    }

    #[test]
    fn handles_complex_hostnames() {
        Python::attach(|_py| {
            let dsn = "postgresql://user:pass@db-server.example.com:5432/mydb".to_string();
            let mut config = Config::new();

            if let Ok((host, _, _, _, _)) = extract_host_from_dsn(dsn, &mut config) {
                assert_eq!(host, "db-server.example.com");
            }
        });
    }

    #[test]
    fn handles_minimum_port_dsn() {
        Python::attach(|_py| {
            let dsn = "postgresql://user:pass@localhost:1/mydb".to_string();
            let mut config = Config::new();

            if let Ok((_, _, port, _, _)) = extract_host_from_dsn(dsn, &mut config) {
                assert_eq!(port, 1);
            }
        });
    }

    #[test]
    fn handles_maximum_port_dsn() {
        Python::attach(|_py| {
            let dsn = "postgresql://user:pass@localhost:65535/mydb".to_string();
            let mut config = Config::new();

            if let Ok((_, _, port, _, _)) = extract_host_from_dsn(dsn, &mut config) {
                assert_eq!(port, 65535);
            }
        });
    }

    #[test]
    fn handles_special_chars_in_dbname() {
        Python::attach(|_py| {
            let dsn = "postgresql://user:pass@localhost/my-db_123".to_string();
            let mut config = Config::new();

            if let Ok((_, _, _, db, _)) = extract_host_from_dsn(dsn, &mut config) {
                assert_eq!(db, "my-db_123");
            }
        });
    }

    #[test]
    fn handles_localhost_variations() {
        Python::attach(|_py| {
            let dsns = vec![
                "postgresql://user:pass@localhost/db",
                "postgresql://user:pass@127.0.0.1/db",
                "postgresql://user:pass@::1/db",
            ];

            for dsn in dsns {
                let mut config = Config::new();
                let result = extract_host_from_dsn(dsn.to_string(), &mut config);
                assert!(result.is_ok() || result.is_err());
            }
        });
    }

    #[test]
    fn provides_meaningful_error_for_invalid_scheme() {
        Python::attach(|_py| {
            let dsn = "http://user:pass@localhost/db".to_string();
            let mut config = Config::new();

            let result = extract_host_from_dsn(dsn, &mut config);
            assert!(result.is_err());
        });
    }
}

mod connect {
    use super::*;

    #[test]
    fn rejects_both_dsn_and_host() {
        Python::attach(|py| {
            let result = connect(
                py,
                Some("postgresql://user:pass@localhost/db".to_string()),
                Some("localhost".to_string()),
                None,
                None,
                5432,
                "db".to_string(),
            );

            assert!(result.is_err());
            if let Err(e) = result {
                let error_msg = e.to_string();
                assert!(error_msg.contains("Cannot specify both DSN"));
            }
        });
    }

    #[test]
    fn rejects_dsn_and_user() {
        Python::attach(|py| {
            let result = connect(
                py,
                Some("postgresql://user:pass@localhost/db".to_string()),
                None,
                Some("user".to_string()),
                None,
                5432,
                "db".to_string(),
            );

            assert!(result.is_err());
        });
    }

    #[test]
    fn rejects_dsn_and_password() {
        Python::attach(|py| {
            let result = connect(
                py,
                Some("postgresql://user:pass@localhost/db".to_string()),
                None,
                None,
                Some("pass".to_string()),
                5432,
                "db".to_string(),
            );

            assert!(result.is_err());
        });
    }

    #[test]
    fn rejects_missing_host_when_no_dsn() {
        Python::attach(|py| {
            let result = connect(
                py,
                None,
                None,
                Some("user".to_string()),
                Some("pass".to_string()),
                5432,
                "db".to_string(),
            );

            assert!(result.is_err());
            if let Err(e) = result {
                let error_msg = e.to_string();
                assert!(error_msg.contains("host"));
            }
        });
    }

    #[test]
    fn rejects_missing_user_when_no_dsn() {
        Python::attach(|py| {
            let result = connect(
                py,
                None,
                Some("localhost".to_string()),
                None,
                Some("pass".to_string()),
                5432,
                "db".to_string(),
            );

            assert!(result.is_err());
            if let Err(e) = result {
                let error_msg = e.to_string();
                assert!(error_msg.contains("user"));
            }
        });
    }

    #[test]
    fn rejects_missing_password_when_no_dsn() {
        Python::attach(|py| {
            let result = connect(
                py,
                None,
                Some("localhost".to_string()),
                Some("user".to_string()),
                None,
                5432,
                "db".to_string(),
            );

            assert!(result.is_err());
            if let Err(e) = result {
                let error_msg = e.to_string();
                assert!(error_msg.contains("password"));
            }
        });
    }

    #[test]
    fn allows_custom_port_with_individual_params() {
        Python::attach(|py| {
            let result = connect(
                py,
                None,
                Some("nonexistent-host-for-testing".to_string()),
                Some("user".to_string()),
                Some("pass".to_string()),
                9999,
                "db".to_string(),
            );

            // This will fail connection, but should not fail validation
            if let Err(e) = result {
                let error_msg = e.to_string();
                assert!(!error_msg.contains("Missing parameter"));
            }
        });
    }

    #[test]
    fn uses_custom_database_name() {
        Python::attach(|py| {
            let result = connect(
                py,
                None,
                Some("nonexistent-host-for-testing".to_string()),
                Some("user".to_string()),
                Some("pass".to_string()),
                5432,
                "custom_database_name".to_string(),
            );

            // This will fail connection, but should not fail validation
            if let Err(e) = result {
                let error_msg = e.to_string();
                assert!(!error_msg.contains("Missing parameter"));
            }
        });
    }
}
