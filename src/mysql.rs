use std::{convert::Infallible, fmt::Write as _};

use crate::{Comparator, Condition, Query, QueryKind};

use async_trait::async_trait;
use datastore::{DataDescriptor, DataQuery, Reader, Store, StoreData, TypeWriter, Write, Writer};
use futures::TryStreamExt;
use sqlx::{mysql::MySqlRow, MySql, Pool, Row};

#[derive(Clone, Debug)]
pub struct MySqlStore {
    pool: Pool<MySql>,
}

#[async_trait]
impl Store for MySqlStore {
    type DataStore = Self;
    type Error = sqlx::Error;

    async fn connect(uri: &str) -> Result<Self, Self::Error> {
        let pool = Pool::connect(uri).await?;

        Ok(Self { pool })
    }

    async fn create<T, D>(&self, descriptor: D) -> Result<(), Self::Error>
    where
        T: StoreData<Self> + Send + Sync + 'static,
        D: DataDescriptor<T, Self> + Send + Sync,
    {
        let table = descriptor.ident();
        let mut writer = MySqlTypeWriter::new(table, QueryKind::Create);
        descriptor.write(&mut writer).unwrap();

        let sql = writer.sql();
        log::debug!("Executing sql CREATE query: \"{}\"", sql);

        sqlx::query(&sql).execute(&self.pool).await?;
        Ok(())
    }

    async fn delete<T, D, Q>(&self, descriptor: D, query: Q) -> Result<(), Self::Error>
    where
        T: StoreData<Self::DataStore> + Send + Sync + 'static,
        D: DataDescriptor<T, Self::DataStore> + Send,
        Q: DataQuery<T, Self::DataStore> + Send,
    {
        let table = descriptor.ident();
        let mut writer = MySqlWriter::new(table, QueryKind::Delete);
        writer.write_conditions = true;
        query.write(&mut writer).unwrap();

        let sql = writer.sql();
        log::debug!("Executing sql DELETE query: \"{}\"", sql);

        sqlx::query(&sql).execute(&self.pool).await?;
        Ok(())
    }

    async fn get<T, D, Q>(&self, descriptor: D, query: Q) -> Result<Vec<T>, Self::Error>
    where
        T: StoreData<Self::DataStore> + Send + Sync + 'static,
        D: DataDescriptor<T, Self::DataStore> + Send,
        Q: DataQuery<T, Self::DataStore> + Send,
    {
        let table = descriptor.ident();

        let mut writer = MySqlWriter::new(table, QueryKind::Select);
        descriptor.write(&mut writer).unwrap();

        writer.write_conditions = true;
        query.write(&mut writer).unwrap();

        let sql = writer.sql();
        log::debug!("Executing sql SELECT query: \"{}\"", sql);

        let mut rows = sqlx::query(&sql).fetch(&self.pool);

        let mut entries = Vec::new();
        while let Some(row) = rows.try_next().await? {
            let mut reader = MySqlReader::new(row);
            let data = T::read(&mut reader).unwrap();

            entries.push(data);
        }

        Ok(entries)
    }

    async fn get_all<T, D>(&self, descriptor: D) -> Result<Vec<T>, Self::Error>
    where
        T: StoreData<Self::DataStore> + Send + Sync + 'static,
        D: DataDescriptor<T, Self::DataStore> + Send + Sync,
    {
        let table = descriptor.ident();
        let mut writer = MySqlTypeWriter::new(table, QueryKind::Select);
        descriptor.write(&mut writer).unwrap();

        let sql = writer.sql();
        log::debug!("Executing sql SELECT query: \"{}\"", sql);

        let mut rows = sqlx::query(&sql).fetch(&self.pool);

        let mut entries = Vec::new();
        while let Some(row) = rows.try_next().await? {
            let mut reader = MySqlReader::new(row);
            let data = T::read(&mut reader)?;

            entries.push(data);
        }

        Ok(entries)
    }

    async fn get_one<T, D, Q>(&self, descriptor: D, query: Q) -> Result<Option<T>, Self::Error>
    where
        T: StoreData<Self::DataStore> + Send + Sync + 'static,
        D: DataDescriptor<T, Self::DataStore> + Send,
        Q: DataQuery<T, Self::DataStore> + Send,
    {
        let table = descriptor.ident();

        let mut writer = MySqlWriter::new(table, QueryKind::Select);
        descriptor.write(&mut writer).unwrap();

        writer.write_conditions = true;
        query.write(&mut writer).unwrap();

        let sql = writer.sql();
        log::debug!("Executing sql SELECT query: \"{}\"", sql);

        let row = match sqlx::query(&sql).fetch_one(&self.pool).await {
            Ok(row) => row,
            Err(sqlx::Error::RowNotFound) => return Ok(None),
            Err(err) => return Err(err.into()),
        };

        let mut reader = MySqlReader::new(row);
        let data = T::read(&mut reader)?;

        Ok(Some(data))
    }

    async fn insert<T, D>(&self, descriptor: D, data: T) -> Result<(), Self::Error>
    where
        T: StoreData<Self::DataStore> + Send + Sync + 'static,
        D: DataDescriptor<T, Self::DataStore> + Send,
    {
        let table = descriptor.ident();

        let mut writer = MySqlWriter::new(table, QueryKind::Insert);
        data.write(&mut writer).unwrap();

        let sql = writer.sql();
        log::debug!("Executing sql INSERT query: \"{}\"", sql);

        sqlx::query(&sql).execute(&self.pool).await?;
        Ok(())
    }
}

#[derive(Debug)]
struct MySqlWriter<'a> {
    query: Query<'a>,
    key: &'static str,
    write_conditions: bool,
}

impl<'a> MySqlWriter<'a> {
    fn new(table: &'a str, kind: QueryKind) -> Self {
        Self {
            query: Query::new(table, kind),
            key: "",
            write_conditions: false,
        }
    }

    fn sql(&self) -> String {
        self.query.to_string()
    }

    fn write<T>(&mut self, val: T) -> Result<(), <Self as Writer<MySqlStore>>::Error>
    where
        T: ToString,
    {
        if self.write_conditions {
            self.query.push_condition(Condition::new(
                self.key.to_owned(),
                val.to_string(),
                Comparator::Eq,
            ));
        } else {
            self.query.push(self.key.to_owned(), val.to_string());
        }
        Ok(())
    }
}

impl<'a> Writer<MySqlStore> for MySqlWriter<'a> {
    type Error = Infallible;

    fn write_bool(&mut self, v: bool) -> Result<(), Self::Error> {
        self.write(match v {
            false => "FALSE",
            true => "TRUE",
        })
    }

    fn write_i8(&mut self, v: i8) -> Result<(), Self::Error> {
        self.write(v)
    }

    fn write_i16(&mut self, v: i16) -> Result<(), Self::Error> {
        self.write(v)
    }

    fn write_i32(&mut self, v: i32) -> Result<(), Self::Error> {
        self.write(v)
    }

    fn write_i64(&mut self, v: i64) -> Result<(), Self::Error> {
        self.write(v)
    }

    fn write_u8(&mut self, v: u8) -> Result<(), Self::Error> {
        self.write(v)
    }

    fn write_u16(&mut self, v: u16) -> Result<(), Self::Error> {
        self.write(v)
    }

    fn write_u32(&mut self, v: u32) -> Result<(), Self::Error> {
        self.write(v)
    }

    fn write_u64(&mut self, v: u64) -> Result<(), Self::Error> {
        self.write(v)
    }

    fn write_f32(&mut self, v: f32) -> Result<(), Self::Error> {
        self.write(v)
    }

    fn write_f64(&mut self, v: f64) -> Result<(), Self::Error> {
        self.write(v)
    }

    fn write_bytes(&mut self, v: &[u8]) -> Result<(), Self::Error> {
        let mut string = String::with_capacity(2 * v.len() + "0x".len());
        string.push_str("0x");
        for byte in v {
            let _ = write!(string, "{:02x}", byte);
        }

        self.write(string)
    }

    fn write_str(&mut self, v: &str) -> Result<(), Self::Error> {
        self.write(format!("'{}'", v.replace('\'', "\'")))
    }

    fn write_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Write<MySqlStore>,
    {
        self.key = key;
        value.write(self)
    }
}

impl<'a> TypeWriter<MySqlStore> for MySqlWriter<'a> {
    type Error = Infallible;

    fn write_bool(&mut self) -> Result<(), Self::Error> {
        self.write("BOOLEAN")
    }

    fn write_i8(&mut self) -> Result<(), Self::Error> {
        self.write("TINYINT")
    }

    fn write_i16(&mut self) -> Result<(), Self::Error> {
        self.write("SMALLINT")
    }

    fn write_i32(&mut self) -> Result<(), Self::Error> {
        self.write("INT")
    }

    fn write_i64(&mut self) -> Result<(), Self::Error> {
        self.write("BIGINT")
    }

    fn write_u8(&mut self) -> Result<(), Self::Error> {
        self.write("TINYINT UNSIGNED")
    }

    fn write_u16(&mut self) -> Result<(), Self::Error> {
        self.write("SMALLINT UNSIGNED")
    }

    fn write_u32(&mut self) -> Result<(), Self::Error> {
        self.write("INT UNSIGNED")
    }

    fn write_u64(&mut self) -> Result<(), Self::Error> {
        self.write("BIGINT UNSIGNED")
    }

    fn write_f32(&mut self) -> Result<(), Self::Error> {
        self.write("FLOAT")
    }

    fn write_f64(&mut self) -> Result<(), Self::Error> {
        self.write("DOUBLE")
    }

    fn write_bytes(&mut self) -> Result<(), Self::Error> {
        self.write("BLOB")
    }

    fn write_str(&mut self) -> Result<(), Self::Error> {
        self.write("TEXT")
    }

    fn write_field<T>(&mut self, key: &'static str) -> Result<(), Self::Error>
    where
        T: ?Sized + Write<MySqlStore>,
    {
        self.key = key;
        T::write_type(self)
    }
}

struct MySqlTypeWriter<'a> {
    query: Query<'a>,
    key: &'static str,
    write_conditions: bool,
}

impl<'a> MySqlTypeWriter<'a> {
    fn new(table: &'a str, kind: QueryKind) -> Self {
        Self {
            query: Query::new(table, kind),
            key: "",
            write_conditions: false,
        }
    }

    fn sql(&self) -> String {
        self.query.to_string()
    }

    fn write<T>(&mut self, value: T) -> Result<(), <Self as TypeWriter<MySqlStore>>::Error>
    where
        T: ToString,
    {
        if !self.write_conditions {
            self.query.push(self.key.to_owned(), value.to_string());
        } else {
            self.query.push_condition(Condition::new(
                self.key.to_owned(),
                value.to_string(),
                Comparator::Eq,
            ));
        }
        Ok(())
    }
}

impl<'a> TypeWriter<MySqlStore> for MySqlTypeWriter<'a> {
    type Error = Infallible;

    fn write_bool(&mut self) -> Result<(), Self::Error> {
        self.write("BOOLEAN")
    }

    fn write_i8(&mut self) -> Result<(), Self::Error> {
        self.write("TINYINT")
    }

    fn write_i16(&mut self) -> Result<(), Self::Error> {
        self.write("SMALLINT")
    }

    fn write_i32(&mut self) -> Result<(), Self::Error> {
        self.write("INT")
    }

    fn write_i64(&mut self) -> Result<(), Self::Error> {
        self.write("BIGINT")
    }

    fn write_u8(&mut self) -> Result<(), Self::Error> {
        self.write("TINYINT UNSIGNED")
    }

    fn write_u16(&mut self) -> Result<(), Self::Error> {
        self.write("SMALLINT UNSIGNED")
    }

    fn write_u32(&mut self) -> Result<(), Self::Error> {
        self.write("INT UNSIGNED")
    }

    fn write_u64(&mut self) -> Result<(), Self::Error> {
        self.write("BIGINT UNSIGNED")
    }

    fn write_f32(&mut self) -> Result<(), Self::Error> {
        self.write("FLOAT")
    }

    fn write_f64(&mut self) -> Result<(), Self::Error> {
        self.write("DOUBLE")
    }

    fn write_bytes(&mut self) -> Result<(), Self::Error> {
        self.write("BLOB")
    }

    fn write_str(&mut self) -> Result<(), Self::Error> {
        self.write("TEXT")
    }

    fn write_field<T>(&mut self, key: &'static str) -> Result<(), Self::Error>
    where
        T: ?Sized + Write<MySqlStore>,
    {
        self.key = key;
        T::write_type(self)
    }
}

struct MySqlReader {
    row: MySqlRow,
    column: Option<&'static str>,
}

impl MySqlReader {
    fn new(row: MySqlRow) -> Self {
        Self { row, column: None }
    }

    fn read<'r, T>(&'r mut self) -> Result<T, <Self as Reader<MySqlStore>>::Error>
    where
        T: sqlx::Decode<'r, MySql> + sqlx::Type<MySql>,
    {
        self.row.try_get(self.column.unwrap())
    }
}

impl Reader<MySqlStore> for MySqlReader {
    type Error = sqlx::Error;

    fn read_bool(&mut self) -> Result<bool, Self::Error> {
        self.read()
    }

    fn read_i8(&mut self) -> Result<i8, Self::Error> {
        self.read()
    }

    fn read_i16(&mut self) -> Result<i16, Self::Error> {
        self.read()
    }

    fn read_i32(&mut self) -> Result<i32, Self::Error> {
        self.read()
    }

    fn read_i64(&mut self) -> Result<i64, Self::Error> {
        self.read()
    }

    fn read_u8(&mut self) -> Result<u8, Self::Error> {
        self.read()
    }

    fn read_u16(&mut self) -> Result<u16, Self::Error> {
        self.read()
    }

    fn read_u32(&mut self) -> Result<u32, Self::Error> {
        self.read()
    }

    fn read_u64(&mut self) -> Result<u64, Self::Error> {
        self.read()
    }

    fn read_f32(&mut self) -> Result<f32, Self::Error> {
        self.read()
    }

    fn read_f64(&mut self) -> Result<f64, Self::Error> {
        self.read()
    }

    fn read_byte_buf(&mut self) -> Result<Vec<u8>, Self::Error> {
        self.read()
    }

    fn read_string(&mut self) -> Result<String, Self::Error> {
        self.read()
    }

    fn read_field<T>(&mut self, key: &'static str) -> Result<T, Self::Error>
    where
        T: Sized + datastore::Read<MySqlStore>,
    {
        self.column = Some(key);
        T::read(self)
    }
}

#[cfg(test)]
mod tests {
    use super::{MySqlStore, MySqlWriter};
    use crate::{mysql::MySqlTypeWriter, QueryKind};

    use datastore::{TypeWriter, Writer};

    macro_rules! write {
        ($writer:expr, $key:expr, $val:expr) => {
            <MySqlWriter as Writer<MySqlStore>>::write_field(&mut $writer, $key, $val).unwrap();
        };
    }

    macro_rules! write_type {
        ($writer:expr, $key:expr, $val:ty) => {
            <MySqlWriter as TypeWriter<MySqlStore>>::write_field::<$val>(&mut $writer, $key)
                .unwrap();
        };
    }

    #[test]
    fn test_writer_create() {
        let mut writer = MySqlTypeWriter::new("test", QueryKind::Create);
        writer.write_field::<i32>("id").unwrap();

        assert_eq!(writer.sql(), "CREATE TABLE IF NOT EXISTS test (id INT)");

        let mut writer = MySqlTypeWriter::new("test", QueryKind::Create);
        writer.write_field::<i32>("id").unwrap();
        writer.write_field::<str>("name").unwrap();

        assert_eq!(
            writer.sql(),
            "CREATE TABLE IF NOT EXISTS test (id INT,name TEXT)"
        );
    }

    #[test]
    fn test_writer_delete() {
        let mut writer = MySqlWriter::new("test", QueryKind::Delete);
        writer.write_conditions = true;
        write!(writer, "id", &3_i32);

        assert_eq!(writer.sql(), "DELETE FROM test WHERE id = 3");

        let mut writer = MySqlWriter::new("test", QueryKind::Delete);
        writer.write_conditions = true;
        write!(writer, "id", &3_i32);
        write!(writer, "name", "hello");

        assert_eq!(
            writer.sql(),
            "DELETE FROM test WHERE id = 3 AND name = 'hello'"
        );
    }

    #[test]
    fn test_writer_insert() {
        let mut writer = MySqlWriter::new("test", QueryKind::Insert);
        write!(writer, "id", &3_i32);

        assert_eq!(writer.sql(), "INSERT INTO test (id) VALUES (3)");

        let mut writer = MySqlWriter::new("test", QueryKind::Insert);
        write!(writer, "id", &3_i32);
        write!(writer, "name", "hello");

        assert_eq!(
            writer.sql(),
            "INSERT INTO test (id,name) VALUES (3,'hello')"
        );
    }

    #[test]
    fn test_writer_select() {
        let mut writer = MySqlWriter::new("test", QueryKind::Select);
        write_type!(writer, "id", i32);

        assert_eq!(writer.sql(), "SELECT id FROM test");

        let mut writer = MySqlWriter::new("test", QueryKind::Select);
        write_type!(writer, "id", i32);
        write_type!(writer, "name", str);

        assert_eq!(writer.sql(), "SELECT id,name FROM test");

        let mut writer = MySqlWriter::new("test", QueryKind::Select);
        write_type!(writer, "id", i32);
        write_type!(writer, "name", str);
        writer.write_conditions = true;
        write!(writer, "id", &3_i32);

        assert_eq!(writer.sql(), "SELECT id,name FROM test WHERE id = 3");
    }
}
