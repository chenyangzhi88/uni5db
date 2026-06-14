use std::fmt;
use std::str::FromStr;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayMode {
    Postgres,
    MySql,
}

impl GatewayMode {
    pub fn from_env() -> Result<Self, String> {
        let Some((name, value)) = std::env::var("PG_GATEWAY_MODE")
            .ok()
            .map(|value| ("PG_GATEWAY_MODE", value))
            .or_else(|| {
                std::env::var("PG_GATEWAY_PROTOCOL")
                    .ok()
                    .map(|value| ("PG_GATEWAY_PROTOCOL", value))
            })
        else {
            return Ok(Self::Postgres);
        };

        value.parse().map_err(|_| {
            format!("{name} must be one of postgres, postgresql, pg, mysql, or my; got '{value}'")
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::MySql => "mysql",
        }
    }

    pub fn sql_dialect(self) -> crate::dialect::parser::SqlDialect {
        match self {
            Self::Postgres => crate::dialect::parser::SqlDialect::Postgres,
            Self::MySql => crate::dialect::parser::SqlDialect::MySql,
        }
    }
}

impl Default for GatewayMode {
    fn default() -> Self {
        Self::Postgres
    }
}

impl fmt::Display for GatewayMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for GatewayMode {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "postgres" | "postgresql" | "pg" => Ok(Self::Postgres),
            "mysql" | "my" => Ok(Self::MySql),
            _ => Err(()),
        }
    }
}
