/// [u8,2] -> u16
pub fn two_u8_to_u16(slice: &[u8]) -> u16 {
    assert_eq!(slice.len(), 2, "slice size not 2");
    ((slice[0] as u16) << 8) | slice[1] as u16
}

pub const SIZEOF_USIZE: usize = 4;

pub const SIZEOF_U16: usize = std::mem::size_of::<u16>();
