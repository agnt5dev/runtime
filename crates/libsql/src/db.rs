use std::fmt;

use libsql::{Connection, Value};

#[derive(Debug)]
pub(crate) struct DbError(String);

impl fmt::Display for DbError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for DbError {}

impl From<libsql::Error> for DbError {
    fn from(error: libsql::Error) -> Self {
        Self(error.to_string())
    }
}

pub(crate) trait IntoDbValue {
    fn into_db_value(self) -> Value;
}

macro_rules! integer_value {
    ($($type:ty),+ $(,)?) => {
        $(
            impl IntoDbValue for $type {
                fn into_db_value(self) -> Value {
                    Value::Integer(self as i64)
                }
            }
        )+
    };
}

integer_value!(i8, i16, i32, i64, u8, u16, u32);

macro_rules! integer_reference_value {
    ($($type:ty),+ $(,)?) => {
        $(
            impl IntoDbValue for &$type {
                fn into_db_value(self) -> Value {
                    Value::Integer(*self as i64)
                }
            }
        )+
    };
}

integer_reference_value!(i8, i16, i32, i64, u8, u16, u32);

impl IntoDbValue for bool {
    fn into_db_value(self) -> Value {
        Value::Integer(i64::from(self))
    }
}

impl IntoDbValue for String {
    fn into_db_value(self) -> Value {
        Value::Text(self)
    }
}

impl IntoDbValue for &String {
    fn into_db_value(self) -> Value {
        Value::Text(self.clone())
    }
}

impl IntoDbValue for &str {
    fn into_db_value(self) -> Value {
        Value::Text(self.to_owned())
    }
}

impl IntoDbValue for Vec<u8> {
    fn into_db_value(self) -> Value {
        Value::Blob(self)
    }
}

impl IntoDbValue for &Vec<u8> {
    fn into_db_value(self) -> Value {
        Value::Blob(self.clone())
    }
}

impl IntoDbValue for &[u8] {
    fn into_db_value(self) -> Value {
        Value::Blob(self.to_vec())
    }
}

impl<T> IntoDbValue for Option<T>
where
    T: IntoDbValue,
{
    fn into_db_value(self) -> Value {
        self.map(IntoDbValue::into_db_value).unwrap_or(Value::Null)
    }
}

pub(crate) trait FromDbValue: Sized {
    fn from_db_value(value: Value) -> Result<Self, DbError>;
}

macro_rules! signed_integer {
    ($($type:ty),+ $(,)?) => {
        $(
            impl FromDbValue for $type {
                fn from_db_value(value: Value) -> Result<Self, DbError> {
                    match value {
                        Value::Integer(value) => <$type>::try_from(value).map_err(|_| {
                            DbError(format!("database integer {value} does not fit {}", stringify!($type)))
                        }),
                        other => Err(type_error(stringify!($type), &other)),
                    }
                }
            }
        )+
    };
}

signed_integer!(i16, i32, i64, u32, u64);

impl FromDbValue for bool {
    fn from_db_value(value: Value) -> Result<Self, DbError> {
        match value {
            Value::Integer(0) => Ok(false),
            Value::Integer(1) => Ok(true),
            other => Err(type_error("bool", &other)),
        }
    }
}

impl FromDbValue for String {
    fn from_db_value(value: Value) -> Result<Self, DbError> {
        match value {
            Value::Text(value) => Ok(value),
            other => Err(type_error("String", &other)),
        }
    }
}

impl FromDbValue for Vec<u8> {
    fn from_db_value(value: Value) -> Result<Self, DbError> {
        match value {
            Value::Blob(value) => Ok(value),
            other => Err(type_error("Vec<u8>", &other)),
        }
    }
}

impl<T> FromDbValue for Option<T>
where
    T: FromDbValue,
{
    fn from_db_value(value: Value) -> Result<Self, DbError> {
        match value {
            Value::Null => Ok(None),
            other => T::from_db_value(other).map(Some),
        }
    }
}

fn type_error(expected: &str, actual: &Value) -> DbError {
    DbError(format!("expected {expected}, got {actual:?}"))
}

pub(crate) struct DbRow {
    columns: Vec<(String, Value)>,
}

impl DbRow {
    fn materialize(row: libsql::Row) -> Result<Self, DbError> {
        let mut columns = Vec::with_capacity(row.column_count() as usize);
        for index in 0..row.column_count() {
            let name = row
                .column_name(index)
                .ok_or_else(|| DbError(format!("result column {index} has no name")))?
                .to_string();
            let value = row.get_value(index).map_err(DbError::from)?;
            columns.push((name, value));
        }
        Ok(Self { columns })
    }

    pub(crate) fn try_get<T>(&self, name: &str) -> Result<T, DbError>
    where
        T: FromDbValue,
    {
        let value = self
            .columns
            .iter()
            .find_map(|(column, value)| (column == name).then_some(value))
            .ok_or_else(|| DbError(format!("result column {name:?} is missing")))?;
        T::from_db_value(value.clone())
    }

    pub(crate) fn try_get_index<T>(&self, index: i32) -> Result<T, DbError>
    where
        T: FromDbValue,
    {
        let value = self
            .columns
            .get(index as usize)
            .ok_or_else(|| DbError(format!("result column index {index} is missing")))?;
        T::from_db_value(value.1.clone())
    }
}

pub(crate) struct DbQuery {
    sql: String,
    params: Vec<Value>,
}

pub(crate) fn query(sql: impl Into<String>) -> DbQuery {
    DbQuery {
        sql: sql.into(),
        params: Vec::new(),
    }
}

impl DbQuery {
    pub(crate) fn bind(mut self, value: impl IntoDbValue) -> Self {
        self.params.push(value.into_db_value());
        self
    }

    pub(crate) async fn execute(self, connection: &Connection) -> Result<DbQueryResult, DbError> {
        let rows_affected = connection
            .execute(&self.sql, self.params)
            .await
            .map_err(DbError::from)?;
        Ok(DbQueryResult { rows_affected })
    }

    pub(crate) async fn fetch_all(self, connection: &Connection) -> Result<Vec<DbRow>, DbError> {
        let mut rows = connection
            .query(&self.sql, self.params)
            .await
            .map_err(DbError::from)?;
        let mut result = Vec::new();
        while let Some(row) = rows.next().await.map_err(DbError::from)? {
            result.push(DbRow::materialize(row)?);
        }
        Ok(result)
    }

    pub(crate) async fn fetch_optional(
        self,
        connection: &Connection,
    ) -> Result<Option<DbRow>, DbError> {
        let mut rows = connection
            .query(&self.sql, self.params)
            .await
            .map_err(DbError::from)?;
        rows.next()
            .await
            .map_err(DbError::from)?
            .map(DbRow::materialize)
            .transpose()
    }

    pub(crate) async fn fetch_one(self, connection: &Connection) -> Result<DbRow, DbError> {
        self.fetch_optional(connection)
            .await?
            .ok_or_else(|| DbError("query returned no rows".to_string()))
    }
}

pub(crate) struct DbQueryResult {
    rows_affected: u64,
}

impl DbQueryResult {
    pub(crate) fn rows_affected(&self) -> u64 {
        self.rows_affected
    }
}
