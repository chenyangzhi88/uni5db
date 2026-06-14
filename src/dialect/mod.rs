pub mod mysql;
pub mod parser;
pub mod postgres;

use crate::mode::GatewayMode;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionIsolationLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl TransactionIsolationLevel {
    pub fn as_sql_str(self) -> &'static str {
        match self {
            Self::ReadCommitted => "read committed",
            Self::RepeatableRead => "repeatable read",
            Self::Serializable => "serializable",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionPolicy {
    pub autocommit_default: bool,
    pub default_isolation: TransactionIsolationLevel,
    pub ddl_implicit_commit: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DialectProfile {
    pub mode: GatewayMode,
    pub default_schema: &'static str,
    pub transaction: TransactionPolicy,
}

pub fn profile(mode: GatewayMode) -> DialectProfile {
    match mode {
        GatewayMode::Postgres => DialectProfile {
            mode,
            default_schema: "public",
            transaction: TransactionPolicy {
                autocommit_default: true,
                default_isolation: TransactionIsolationLevel::ReadCommitted,
                ddl_implicit_commit: false,
            },
        },
        GatewayMode::MySql => DialectProfile {
            mode,
            default_schema: "public",
            transaction: TransactionPolicy {
                autocommit_default: true,
                default_isolation: TransactionIsolationLevel::RepeatableRead,
                ddl_implicit_commit: true,
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_profile_uses_postgres_transaction_defaults() {
        let profile = profile(GatewayMode::Postgres);
        assert_eq!(
            profile.transaction.default_isolation,
            TransactionIsolationLevel::ReadCommitted
        );
        assert!(!profile.transaction.ddl_implicit_commit);
    }

    #[test]
    fn mysql_profile_uses_mysql_transaction_defaults() {
        let profile = profile(GatewayMode::MySql);
        assert_eq!(
            profile.transaction.default_isolation,
            TransactionIsolationLevel::RepeatableRead
        );
        assert!(profile.transaction.ddl_implicit_commit);
    }
}
