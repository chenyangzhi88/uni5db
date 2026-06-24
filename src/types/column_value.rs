use std::cmp::Ordering;

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnValue {
    Null,
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Numeric(String),
    Text(String),
    Boolean(bool),
    Date(String),
    Timestamp(String),
    TimestampTz(String),
    Uuid(String),
    Bytea(Vec<u8>),
    Json(String),
    Jsonb(String),
    Array(Vec<ColumnValue>),
}

impl ColumnValue {
    pub fn to_text(&self) -> Option<String> {
        match self {
            ColumnValue::Null => None,
            ColumnValue::Int16(v) => Some(v.to_string()),
            ColumnValue::Int32(v) => Some(v.to_string()),
            ColumnValue::Int64(v) => Some(v.to_string()),
            ColumnValue::Float32(v) => Some(v.to_string()),
            ColumnValue::Float64(v) => Some(v.to_string()),
            ColumnValue::Numeric(v) => Some(v.clone()),
            ColumnValue::Text(v) => Some(v.clone()),
            ColumnValue::Boolean(v) => Some(v.to_string()),
            ColumnValue::Date(v) => Some(v.clone()),
            ColumnValue::Timestamp(v) => Some(v.clone()),
            ColumnValue::TimestampTz(v) => Some(v.clone()),
            ColumnValue::Uuid(v) => Some(v.clone()),
            ColumnValue::Bytea(v) => Some(format!("\\x{}", hex_encode(v))),
            ColumnValue::Json(v) => Some(v.clone()),
            ColumnValue::Jsonb(v) => Some(v.clone()),
            ColumnValue::Array(values) => Some(format!(
                "{{{}}}",
                values
                    .iter()
                    .map(|value| value
                        .to_text()
                        .map(escape_array_text)
                        .unwrap_or_else(|| "NULL".to_string()))
                    .collect::<Vec<_>>()
                    .join(",")
            )),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, ColumnValue::Null)
    }
}

impl PartialOrd for ColumnValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (ColumnValue::Int16(a), ColumnValue::Int16(b)) => a.partial_cmp(b),
            (ColumnValue::Int16(a), ColumnValue::Int32(b)) => (*a as i32).partial_cmp(b),
            (ColumnValue::Int32(a), ColumnValue::Int16(b)) => a.partial_cmp(&(*b as i32)),
            (ColumnValue::Int16(a), ColumnValue::Int64(b)) => (*a as i64).partial_cmp(b),
            (ColumnValue::Int64(a), ColumnValue::Int16(b)) => a.partial_cmp(&(*b as i64)),
            (ColumnValue::Int32(a), ColumnValue::Int32(b)) => a.partial_cmp(b),
            (ColumnValue::Int64(a), ColumnValue::Int64(b)) => a.partial_cmp(b),
            (ColumnValue::Int32(a), ColumnValue::Int64(b)) => (*a as i64).partial_cmp(b),
            (ColumnValue::Int64(a), ColumnValue::Int32(b)) => a.partial_cmp(&(*b as i64)),
            (ColumnValue::Float32(a), ColumnValue::Float32(b)) => a.partial_cmp(b),
            (ColumnValue::Float64(a), ColumnValue::Float64(b)) => a.partial_cmp(b),
            (ColumnValue::Float32(a), ColumnValue::Float64(b)) => (*a as f64).partial_cmp(b),
            (ColumnValue::Float64(a), ColumnValue::Float32(b)) => a.partial_cmp(&(*b as f64)),
            (ColumnValue::Int16(a), ColumnValue::Float32(b)) => (*a as f32).partial_cmp(b),
            (ColumnValue::Float32(a), ColumnValue::Int16(b)) => a.partial_cmp(&(*b as f32)),
            (ColumnValue::Int16(a), ColumnValue::Float64(b)) => (*a as f64).partial_cmp(b),
            (ColumnValue::Float64(a), ColumnValue::Int16(b)) => a.partial_cmp(&(*b as f64)),
            (ColumnValue::Int32(a), ColumnValue::Float32(b)) => (*a as f32).partial_cmp(b),
            (ColumnValue::Float32(a), ColumnValue::Int32(b)) => a.partial_cmp(&(*b as f32)),
            (ColumnValue::Int64(a), ColumnValue::Float64(b)) => (*a as f64).partial_cmp(b),
            (ColumnValue::Float64(a), ColumnValue::Int64(b)) => a.partial_cmp(&(*b as f64)),
            (ColumnValue::Numeric(a), ColumnValue::Numeric(b)) => numeric_cmp(a, b),
            (ColumnValue::Text(a), ColumnValue::Text(b)) => a.partial_cmp(b),
            (ColumnValue::Boolean(a), ColumnValue::Boolean(b)) => a.partial_cmp(b),
            (ColumnValue::Date(a), ColumnValue::Date(b))
            | (ColumnValue::Timestamp(a), ColumnValue::Timestamp(b))
            | (ColumnValue::TimestampTz(a), ColumnValue::TimestampTz(b))
            | (ColumnValue::Uuid(a), ColumnValue::Uuid(b)) => a.partial_cmp(b),
            (ColumnValue::Bytea(a), ColumnValue::Bytea(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

fn numeric_cmp(a: &str, b: &str) -> Option<Ordering> {
    match (NormalizedDecimal::parse(a), NormalizedDecimal::parse(b)) {
        (Some(a), Some(b)) => Some(a.cmp(&b)),
        _ => a.partial_cmp(b),
    }
}

#[derive(Debug, Eq, PartialEq)]
struct NormalizedDecimal {
    sign: i8,
    digits: Vec<u8>,
    scale: i64,
}

impl NormalizedDecimal {
    fn parse(raw: &str) -> Option<Self> {
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }

        let (sign, rest) = match raw.as_bytes().first()? {
            b'-' => (-1, &raw[1..]),
            b'+' => (1, &raw[1..]),
            _ => (1, raw),
        };
        let (mantissa, exponent) = match rest.find(['e', 'E']) {
            Some(idx) => {
                let exponent = rest[idx + 1..].parse::<i64>().ok()?;
                (&rest[..idx], exponent)
            }
            None => (rest, 0),
        };

        let mut digits = Vec::new();
        let mut frac_len = 0_i64;
        let mut saw_dot = false;
        let mut saw_digit = false;
        for byte in mantissa.bytes() {
            match byte {
                b'0'..=b'9' => {
                    saw_digit = true;
                    digits.push(byte);
                    if saw_dot {
                        frac_len = frac_len.checked_add(1)?;
                    }
                }
                b'.' if !saw_dot => saw_dot = true,
                _ => return None,
            }
        }
        if !saw_digit {
            return None;
        }

        let mut scale = frac_len.checked_sub(exponent)?;
        let first_non_zero = digits.iter().position(|digit| *digit != b'0');
        let Some(first_non_zero) = first_non_zero else {
            return Some(Self {
                sign: 1,
                digits: Vec::new(),
                scale: 0,
            });
        };
        digits.drain(..first_non_zero);
        while digits.last() == Some(&b'0') {
            digits.pop();
            scale = scale.checked_sub(1)?;
        }

        Some(Self {
            sign,
            digits,
            scale,
        })
    }

    fn cmp_abs(&self, other: &Self) -> Ordering {
        let self_exp = self.digits.len() as i64 - self.scale;
        let other_exp = other.digits.len() as i64 - other.scale;
        match self_exp.cmp(&other_exp) {
            Ordering::Equal => {}
            ordering => return ordering,
        }

        let max_len = self.digits.len().max(other.digits.len());
        for idx in 0..max_len {
            let left = self.digits.get(idx).copied().unwrap_or(b'0');
            let right = other.digits.get(idx).copied().unwrap_or(b'0');
            match left.cmp(&right) {
                Ordering::Equal => {}
                ordering => return ordering,
            }
        }
        Ordering::Equal
    }
}

impl Ord for NormalizedDecimal {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.digits.is_empty(), other.digits.is_empty()) {
            (true, true) => return Ordering::Equal,
            (true, false) => return 0.cmp(&(other.sign as i32)),
            (false, true) => return (self.sign as i32).cmp(&0),
            (false, false) => {}
        }
        match self.sign.cmp(&other.sign) {
            Ordering::Equal if self.sign < 0 => other.cmp_abs(self),
            Ordering::Equal => self.cmp_abs(other),
            ordering => ordering,
        }
    }
}

impl PartialOrd for NormalizedDecimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn escape_array_text(value: String) -> String {
    if value.is_empty()
        || value.contains(',')
        || value.contains('{')
        || value.contains('}')
        || value.contains('"')
        || value.contains('\\')
        || value.chars().any(char::is_whitespace)
    {
        format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        value
    }
}
