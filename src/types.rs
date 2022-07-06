use datastore::{Read, Reader, TypeWriter, Write, Writer};

use crate::MySqlStore;

impl Write<MySqlStore> for bool {
    fn write<W>(&self, writer: &mut W) -> Result<(), W::Error>
    where
        W: Writer<MySqlStore>,
    {
        writer.write_bool(*self)
    }

    fn write_type<W>(writer: &mut W) -> Result<(), W::Error>
    where
        W: TypeWriter<MySqlStore>,
    {
        writer.write_bool()
    }
}

impl Write<MySqlStore> for i8 {
    fn write<W>(&self, writer: &mut W) -> Result<(), W::Error>
    where
        W: Writer<MySqlStore>,
    {
        writer.write_i8(*self)
    }

    fn write_type<W>(writer: &mut W) -> Result<(), W::Error>
    where
        W: TypeWriter<MySqlStore>,
    {
        writer.write_i8()
    }
}

impl Write<MySqlStore> for i16 {
    fn write<W>(&self, writer: &mut W) -> Result<(), W::Error>
    where
        W: Writer<MySqlStore>,
    {
        writer.write_i16(*self)
    }

    fn write_type<W>(writer: &mut W) -> Result<(), W::Error>
    where
        W: TypeWriter<MySqlStore>,
    {
        writer.write_i16()
    }
}

impl Write<MySqlStore> for i32 {
    fn write<W>(&self, writer: &mut W) -> Result<(), W::Error>
    where
        W: Writer<MySqlStore>,
    {
        writer.write_i32(*self)
    }

    fn write_type<W>(writer: &mut W) -> Result<(), W::Error>
    where
        W: TypeWriter<MySqlStore>,
    {
        writer.write_i32()
    }
}

impl Write<MySqlStore> for i64 {
    fn write<W>(&self, writer: &mut W) -> Result<(), W::Error>
    where
        W: Writer<MySqlStore>,
    {
        writer.write_i64(*self)
    }

    fn write_type<W>(writer: &mut W) -> Result<(), W::Error>
    where
        W: TypeWriter<MySqlStore>,
    {
        writer.write_i64()
    }
}

impl Write<MySqlStore> for u8 {
    fn write<W>(&self, writer: &mut W) -> Result<(), W::Error>
    where
        W: Writer<MySqlStore>,
    {
        writer.write_u8(*self)
    }

    fn write_type<W>(writer: &mut W) -> Result<(), W::Error>
    where
        W: TypeWriter<MySqlStore>,
    {
        writer.write_u8()
    }
}

impl Write<MySqlStore> for u16 {
    fn write<W>(&self, writer: &mut W) -> Result<(), W::Error>
    where
        W: Writer<MySqlStore>,
    {
        writer.write_u16(*self)
    }

    fn write_type<W>(writer: &mut W) -> Result<(), W::Error>
    where
        W: TypeWriter<MySqlStore>,
    {
        writer.write_u16()
    }
}

impl Write<MySqlStore> for u32 {
    fn write<W>(&self, writer: &mut W) -> Result<(), W::Error>
    where
        W: Writer<MySqlStore>,
    {
        writer.write_u32(*self)
    }

    fn write_type<W>(writer: &mut W) -> Result<(), W::Error>
    where
        W: TypeWriter<MySqlStore>,
    {
        writer.write_u32()
    }
}

impl Write<MySqlStore> for u64 {
    fn write<W>(&self, writer: &mut W) -> Result<(), W::Error>
    where
        W: Writer<MySqlStore>,
    {
        writer.write_u64(*self)
    }

    fn write_type<W>(writer: &mut W) -> Result<(), W::Error>
    where
        W: TypeWriter<MySqlStore>,
    {
        writer.write_u64()
    }
}

impl Write<MySqlStore> for f32 {
    fn write<W>(&self, writer: &mut W) -> Result<(), W::Error>
    where
        W: Writer<MySqlStore>,
    {
        writer.write_f32(*self)
    }

    fn write_type<W>(writer: &mut W) -> Result<(), W::Error>
    where
        W: TypeWriter<MySqlStore>,
    {
        writer.write_f32()
    }
}

impl Write<MySqlStore> for f64 {
    fn write<W>(&self, writer: &mut W) -> Result<(), W::Error>
    where
        W: Writer<MySqlStore>,
    {
        writer.write_f64(*self)
    }

    fn write_type<W>(writer: &mut W) -> Result<(), W::Error>
    where
        W: TypeWriter<MySqlStore>,
    {
        writer.write_f64()
    }
}

impl Write<MySqlStore> for [u8] {
    fn write<W>(&self, writer: &mut W) -> Result<(), W::Error>
    where
        W: Writer<MySqlStore>,
    {
        writer.write_bytes(self)
    }

    fn write_type<W>(writer: &mut W) -> Result<(), W::Error>
    where
        W: TypeWriter<MySqlStore>,
    {
        writer.write_bytes()
    }
}

impl Write<MySqlStore> for Vec<u8> {
    fn write<W>(&self, writer: &mut W) -> Result<(), W::Error>
    where
        W: Writer<MySqlStore>,
    {
        writer.write_bytes(&self)
    }

    fn write_type<W>(writer: &mut W) -> Result<(), W::Error>
    where
        W: TypeWriter<MySqlStore>,
    {
        writer.write_bytes()
    }
}

impl Write<MySqlStore> for str {
    fn write<W>(&self, writer: &mut W) -> Result<(), W::Error>
    where
        W: Writer<MySqlStore>,
    {
        writer.write_str(self)
    }

    fn write_type<W>(writer: &mut W) -> Result<(), W::Error>
    where
        W: TypeWriter<MySqlStore>,
    {
        writer.write_str()
    }
}

impl Write<MySqlStore> for String {
    fn write<W>(&self, writer: &mut W) -> Result<(), W::Error>
    where
        W: Writer<MySqlStore>,
    {
        writer.write_str(&self)
    }

    fn write_type<W>(writer: &mut W) -> Result<(), W::Error>
    where
        W: TypeWriter<MySqlStore>,
    {
        writer.write_str()
    }
}

// === impl Read ===

impl Read<MySqlStore> for bool {
    fn read<R>(reader: &mut R) -> Result<Self, R::Error>
    where
        R: Reader<MySqlStore>,
    {
        reader.read_bool()
    }
}

impl Read<MySqlStore> for i8 {
    fn read<R>(reader: &mut R) -> Result<Self, R::Error>
    where
        R: Reader<MySqlStore>,
    {
        reader.read_i8()
    }
}

impl Read<MySqlStore> for i16 {
    fn read<R>(reader: &mut R) -> Result<Self, R::Error>
    where
        R: Reader<MySqlStore>,
    {
        reader.read_i16()
    }
}

impl Read<MySqlStore> for i32 {
    fn read<R>(reader: &mut R) -> Result<Self, R::Error>
    where
        R: Reader<MySqlStore>,
    {
        reader.read_i32()
    }
}

impl Read<MySqlStore> for i64 {
    fn read<R>(reader: &mut R) -> Result<Self, R::Error>
    where
        R: Reader<MySqlStore>,
    {
        reader.read_i64()
    }
}

impl Read<MySqlStore> for u8 {
    fn read<R>(reader: &mut R) -> Result<Self, R::Error>
    where
        R: Reader<MySqlStore>,
    {
        reader.read_u8()
    }
}

impl Read<MySqlStore> for u16 {
    fn read<R>(reader: &mut R) -> Result<Self, R::Error>
    where
        R: Reader<MySqlStore>,
    {
        reader.read_u16()
    }
}

impl Read<MySqlStore> for u32 {
    fn read<R>(reader: &mut R) -> Result<Self, R::Error>
    where
        R: Reader<MySqlStore>,
    {
        reader.read_u32()
    }
}

impl Read<MySqlStore> for u64 {
    fn read<R>(reader: &mut R) -> Result<Self, R::Error>
    where
        R: Reader<MySqlStore>,
    {
        reader.read_u64()
    }
}

impl Read<MySqlStore> for f32 {
    fn read<R>(reader: &mut R) -> Result<Self, R::Error>
    where
        R: Reader<MySqlStore>,
    {
        reader.read_f32()
    }
}

impl Read<MySqlStore> for f64 {
    fn read<R>(reader: &mut R) -> Result<Self, R::Error>
    where
        R: Reader<MySqlStore>,
    {
        reader.read_f64()
    }
}

impl Read<MySqlStore> for Vec<u8> {
    fn read<R>(reader: &mut R) -> Result<Self, R::Error>
    where
        R: Reader<MySqlStore>,
    {
        reader.read_byte_buf()
    }
}

impl Read<MySqlStore> for String {
    fn read<R>(reader: &mut R) -> Result<Self, R::Error>
    where
        R: Reader<MySqlStore>,
    {
        reader.read_string()
    }
}
