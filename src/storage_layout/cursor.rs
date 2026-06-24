use pgwire::error::PgWireResult;

use super::tuple_codec::read_bytes_segment;
use crate::error::user_error;

pub(super) struct Cursor<'a> {
    bytes: &'a [u8],
    pub(super) pos: usize,
}

impl<'a> Cursor<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    pub(super) fn is_done(&self) -> bool {
        self.pos == self.bytes.len()
    }

    pub(super) fn u8(&mut self) -> PgWireResult<u8> {
        let value = *self
            .bytes
            .get(self.pos)
            .ok_or_else(|| user_error("XX000", "truncated u8"))?;
        self.pos += 1;
        Ok(value)
    }

    pub(super) fn u32(&mut self) -> PgWireResult<u32> {
        if self.bytes.len() < self.pos + 4 {
            return Err(user_error("XX000", "truncated u32"));
        }
        let value = u32::from_be_bytes(self.bytes[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(value)
    }

    pub(super) fn u64(&mut self) -> PgWireResult<u64> {
        if self.bytes.len() < self.pos + 8 {
            return Err(user_error("XX000", "truncated u64"));
        }
        let value = u64::from_be_bytes(self.bytes[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(value)
    }

    pub(super) fn i16(&mut self) -> PgWireResult<i16> {
        if self.bytes.len() < self.pos + 2 {
            return Err(user_error("XX000", "truncated i16"));
        }
        let value = i16::from_be_bytes(self.bytes[self.pos..self.pos + 2].try_into().unwrap());
        self.pos += 2;
        Ok(value)
    }

    pub(super) fn i32(&mut self) -> PgWireResult<i32> {
        if self.bytes.len() < self.pos + 4 {
            return Err(user_error("XX000", "truncated i32"));
        }
        let value = i32::from_be_bytes(self.bytes[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(value)
    }

    pub(super) fn i64(&mut self) -> PgWireResult<i64> {
        if self.bytes.len() < self.pos + 8 {
            return Err(user_error("XX000", "truncated i64"));
        }
        let value = i64::from_be_bytes(self.bytes[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(value)
    }

    pub(super) fn f32(&mut self) -> PgWireResult<f32> {
        if self.bytes.len() < self.pos + 4 {
            return Err(user_error("XX000", "truncated f32"));
        }
        let value = f32::from_be_bytes(self.bytes[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(value)
    }

    pub(super) fn f64(&mut self) -> PgWireResult<f64> {
        if self.bytes.len() < self.pos + 8 {
            return Err(user_error("XX000", "truncated f64"));
        }
        let value = f64::from_be_bytes(self.bytes[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(value)
    }

    pub(super) fn bytes_segment(&mut self) -> PgWireResult<Vec<u8>> {
        let (bytes, consumed) = read_bytes_segment(&self.bytes[self.pos..])?;
        self.pos += consumed;
        Ok(bytes)
    }

    pub(super) fn skip(&mut self, len: usize) -> PgWireResult<()> {
        if self.bytes.len() < self.pos + len {
            return Err(user_error("XX000", "truncated tuple value"));
        }
        self.pos += len;
        Ok(())
    }

    pub(super) fn skip_bytes_segment(&mut self) -> PgWireResult<()> {
        let (_, consumed) = read_bytes_segment(&self.bytes[self.pos..])?;
        self.pos += consumed;
        Ok(())
    }
}
