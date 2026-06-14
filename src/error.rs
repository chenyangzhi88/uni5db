use pgwire::error::{ErrorInfo, PgWireError};

pub fn user_error(code: impl Into<String>, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        code.into(),
        message.into(),
    )))
}

pub fn unsupported(message: impl Into<String>) -> PgWireError {
    user_error("0A000", message)
}
