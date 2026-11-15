//! Bitwise equality for durable REAL fields held by the block state.
//! The old SqlValue representation compared floating bits, not IEEE numeric
//! equality: NaN is reflexive in a snapshot and signed zero remains distinct.
//! Captured rows are decoded after SQLite has canonicalized their stored value.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SqlReal(u64);

impl SqlReal {
    pub(crate) fn as_f64(self) -> f64 {
        f64::from_bits(self.0)
    }
}

impl From<f64> for SqlReal {
    fn from(value: f64) -> Self {
        Self(value.to_bits())
    }
}

impl rusqlite::types::FromSql for SqlReal {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        <f64 as rusqlite::types::FromSql>::column_result(value).map(Self::from)
    }
}

impl rusqlite::ToSql for SqlReal {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(rusqlite::types::ToSqlOutput::Owned(self.as_f64().into()))
    }
}

/// Deferred type validation still needs the original SQLite value for pool
/// telemetry, including REAL values. Its floating branch shares SqlReal's Eq.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SqlValue {
    Null,
    Integer(i64),
    Real(SqlReal),
    Text(String),
    Blob(Vec<u8>),
}

impl SqlValue {
    pub(crate) fn data_type(&self) -> rusqlite::types::Type {
        self.as_value().data_type()
    }
    pub(crate) fn as_value(&self) -> rusqlite::types::Value {
        use rusqlite::types::Value;
        match self {
            Self::Null => Value::Null,
            Self::Integer(value) => Value::Integer(*value),
            Self::Real(value) => Value::Real(value.as_f64()),
            Self::Text(value) => Value::Text(value.clone()),
            Self::Blob(value) => Value::Blob(value.clone()),
        }
    }
}

impl rusqlite::types::FromSql for SqlValue {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        use rusqlite::types::ValueRef;
        Ok(match value {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(value) => Self::Integer(value),
            ValueRef::Real(value) => Self::Real(value.into()),
            ValueRef::Text(value) => Self::Text(
                std::str::from_utf8(value)
                    .map_err(|error| rusqlite::types::FromSqlError::Other(Box::new(error)))?
                    .into(),
            ),
            ValueRef::Blob(value) => Self::Blob(value.into()),
        })
    }
}

#[cfg(test)]
#[path = "sql_real/tests.rs"]
mod tests;
