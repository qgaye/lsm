/// [u8,2] -> u16
pub fn two_u8_to_u16(slice: &[u8]) -> u16 {
    assert_eq!(slice.len(), 2, "slice size not 2");
    ((slice[0] as u16) << 8) | slice[1] as u16
}