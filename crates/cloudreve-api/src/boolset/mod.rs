use base64::{engine::general_purpose::STANDARD, Engine};

/// A compact boolean set stored as a bit array
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Boolset {
    data: Vec<u8>,
}

impl Boolset {
    /// Create a new empty Boolset
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    /// Create a Boolset from raw bytes
    pub fn from_raw(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// Create a Boolset from a base64-encoded string
    pub fn from_base64(encoded: &str) -> Result<Self, base64::DecodeError> {
        let data = STANDARD.decode(encoded)?;
        Ok(Self { data })
    }

    /// Create a Boolset from an optional base64 string, falling back to raw bytes or empty
    /// This mimics the TypeScript constructor behavior
    pub fn from_data(base64_str: Option<&str>, raw: Option<Vec<u8>>) -> Self {
        if let Some(encoded) = base64_str {
            Self::from_base64(encoded).unwrap_or_else(|e| {
                eprintln!("Failed to decode boolset: {}", e);
                Self::new()
            })
        } else if let Some(data) = raw {
            Self::from_raw(data)
        } else {
            Self::new()
        }
    }

    /// Check if a bit at the given index is enabled
    pub fn enabled(&self, index: usize) -> bool {
        if index >= self.data.len() * 8 {
            return false;
        }
        (self.data[index / 8] & (1 << (index % 8))) != 0
    }

    /// Perform bitwise AND with another Boolset, returning a new Boolset
    pub fn and(&self, other: &Boolset) -> Boolset {
        let length = self.data.len().max(other.data.len());
        let mut result = vec![0u8; length];

        for i in 0..length {
            let a = self.data.get(i).copied().unwrap_or(0);
            let b = other.data.get(i).copied().unwrap_or(0);
            result[i] = a & b;
        }

        Boolset { data: result }
    }

    /// Perform bitwise OR with another Boolset, returning a new Boolset
    pub fn or(&self, other: &Boolset) -> Boolset {
        let length = self.data.len().max(other.data.len());
        let mut result = vec![0u8; length];

        for i in 0..length {
            let a = self.data.get(i).copied().unwrap_or(0);
            let b = other.data.get(i).copied().unwrap_or(0);
            result[i] = a | b;
        }

        Boolset { data: result }
    }

    /// Set or clear a bit at the given index
    /// Returns a mutable reference to self for method chaining
    pub fn set(&mut self, index: usize, enabled: bool) -> &mut Self {
        let byte_index = index / 8;
        let bit_index = index % 8;

        // Expand array if necessary
        if byte_index >= self.data.len() {
            self.data.resize(byte_index + 1, 0);
        }

        if enabled {
            self.data[byte_index] |= 1 << bit_index;
        } else {
            self.data[byte_index] &= !(1 << bit_index);
        }

        self
    }

    /// Set multiple bits at once from a slice of (index, enabled) tuples
    pub fn sets(&mut self, values: &[(usize, bool)]) -> &mut Self {
        for &(index, enabled) in values {
            self.set(index, enabled);
        }
        self
    }

    /// Convert to base64-encoded string
    pub fn to_base64(&self) -> String {
        STANDARD.encode(&self.data)
    }

    /// Get the underlying byte data
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

impl Default for Boolset {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_and_default_are_empty() {
        assert_eq!(Boolset::new().as_bytes(), &[] as &[u8]);
        assert_eq!(Boolset::default(), Boolset::new());
    }

    #[test]
    fn from_raw_preserves_bytes() {
        let bs = Boolset::from_raw(vec![1, 2, 3]);
        assert_eq!(bs.as_bytes(), &[1, 2, 3]);
    }

    #[test]
    fn base64_roundtrip() {
        // byte 0b0000_0101 -> bits 0 and 2 set
        let bs = Boolset::from_raw(vec![0b0000_0101]);
        let encoded = bs.to_base64();
        let decoded = Boolset::from_base64(&encoded).unwrap();
        assert_eq!(bs, decoded);
        assert_eq!(Boolset::from_base64("BQ==").unwrap().as_bytes(), &[5]);
    }

    #[test]
    fn from_base64_rejects_invalid_input() {
        assert!(Boolset::from_base64("not valid base64!!!").is_err());
    }

    #[test]
    fn from_data_prefers_base64_then_raw_then_empty() {
        assert_eq!(
            Boolset::from_data(Some("BQ=="), Some(vec![9])).as_bytes(),
            &[5]
        );
        assert_eq!(
            Boolset::from_data(None, Some(vec![9, 8])).as_bytes(),
            &[9, 8]
        );
        assert_eq!(Boolset::from_data(None, None).as_bytes(), &[] as &[u8]);
    }

    #[test]
    fn from_data_falls_back_to_empty_on_bad_base64() {
        assert_eq!(
            Boolset::from_data(Some("###"), Some(vec![9])).as_bytes(),
            &[] as &[u8]
        );
    }

    #[test]
    fn enabled_reads_individual_bits() {
        let bs = Boolset::from_raw(vec![0b0000_0101]);
        assert!(bs.enabled(0));
        assert!(!bs.enabled(1));
        assert!(bs.enabled(2));
    }

    #[test]
    fn enabled_out_of_range_is_false() {
        let bs = Boolset::from_raw(vec![0xFF]);
        assert!(bs.enabled(7));
        assert!(!bs.enabled(8));
        assert!(!Boolset::new().enabled(0));
    }

    #[test]
    fn set_expands_storage_and_toggles_bits() {
        let mut bs = Boolset::new();
        bs.set(9, true);
        assert_eq!(bs.as_bytes().len(), 2);
        assert!(bs.enabled(9));

        bs.set(9, false);
        assert!(!bs.enabled(9));
        // Storage is not shrunk when clearing.
        assert_eq!(bs.as_bytes().len(), 2);
    }

    #[test]
    fn sets_applies_multiple_values() {
        let mut bs = Boolset::new();
        bs.sets(&[(0, true), (3, true), (3, false), (16, true)]);
        assert!(bs.enabled(0));
        assert!(!bs.enabled(3));
        assert!(bs.enabled(16));
    }

    #[test]
    fn and_or_handle_differing_lengths() {
        let a = Boolset::from_raw(vec![0b1100, 0b1010]);
        let b = Boolset::from_raw(vec![0b1010]);

        assert_eq!(a.and(&b).as_bytes(), &[0b1000, 0b0000]);
        assert_eq!(a.or(&b).as_bytes(), &[0b1110, 0b1010]);
        // Operation is commutative with respect to length handling.
        assert_eq!(b.and(&a).as_bytes(), a.and(&b).as_bytes());
        assert_eq!(b.or(&a).as_bytes(), a.or(&b).as_bytes());
    }
}
