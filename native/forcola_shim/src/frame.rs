//! Wire-protocol framing: 4-byte big-endian length prefix + 1-byte tag +
//! payload, read from and written to plain byte streams.

use std::io::{self, Read, Write};

/// Inbound tag: BEAM -> shim.
pub const TAG_SPAWN: u8 = 0x01;
pub const TAG_STDIN: u8 = 0x02;
pub const TAG_EOF: u8 = 0x03;
pub const TAG_KILL: u8 = 0x04;
/// Grants the stdout pump N more bytes of read budget under backpressure.
/// Payload is an 8-byte big-endian byte count. Only sent when the BEAM
/// opted into backpressure via `window_bytes`.
pub const TAG_CREDIT: u8 = 0x05;

/// Outbound tag: shim -> BEAM.
pub const TAG_STDOUT: u8 = 0x11;
pub const TAG_STDERR: u8 = 0x12;
pub const TAG_EXIT: u8 = 0x13;
pub const TAG_ERROR: u8 = 0x14;

/// Upper bound on a single frame's length (tag byte + payload).
///
/// In practice frames never come close: the BEAM writes with `{:packet, 4}`
/// discipline, SPAWN/KILL/EOF payloads are small, and STDIN chunks are
/// bounded by what one `Port.command` carries. The cap exists so a corrupt
/// or malicious length prefix cannot trigger a multi-gigabyte allocation
/// in `read_frame`; it is a robustness bound, not a protocol limit anyone
/// should ever hit.
pub const MAX_FRAME_LEN: usize = 64 * 1024 * 1024;

/// A single framed message: a tag byte plus its payload.
#[derive(Debug, Clone)]
pub struct Frame {
    pub tag: u8,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn new(tag: u8, payload: Vec<u8>) -> Self {
        Frame { tag, payload }
    }
}

/// Reads one length-prefixed, tagged frame from `r`.
///
/// Returns `Ok(None)` on clean EOF at a frame boundary (no bytes read at
/// all for the length prefix). Any EOF in the middle of a frame is an
/// error, since that indicates a truncated stream rather than a clean
/// shutdown.
pub fn read_frame<R: Read>(r: &mut R) -> io::Result<Option<Frame>> {
    let mut len_buf = [0u8; 4];
    if !read_exact_or_eof(r, &mut len_buf)? {
        return Ok(None);
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    if len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame length must include at least the tag byte",
        ));
    }

    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame length {len} exceeds the {MAX_FRAME_LEN}-byte cap"),
        ));
    }

    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;

    let tag = body[0];
    let payload = body[1..].to_vec();
    Ok(Some(Frame::new(tag, payload)))
}

/// Like `read_exact`, but returns `Ok(false)` instead of erroring when
/// zero bytes are available at the very start of the read (clean EOF at a
/// frame boundary).
fn read_exact_or_eof<R: Read>(r: &mut R, buf: &mut [u8]) -> io::Result<bool> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => {
                if filled == 0 {
                    return Ok(false);
                }
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "stream ended mid-frame",
                ));
            }
            Ok(n) => filled += n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

/// Writes one length-prefixed, tagged frame to `w` and flushes it.
///
/// Frames going to the BEAM must be flushed promptly: the BEAM reads with
/// `{:packet, 4}` framing and blocks waiting for complete frames.
pub fn write_frame<W: Write>(w: &mut W, tag: u8, payload: &[u8]) -> io::Result<()> {
    let len = (payload.len() + 1) as u32;
    w.write_all(&len.to_be_bytes())?;
    w.write_all(&[tag])?;
    w.write_all(payload)?;
    w.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn round_trip_frame() {
        let mut buf = Vec::new();
        write_frame(&mut buf, TAG_STDOUT, b"hello").unwrap();

        let mut cursor = Cursor::new(buf);
        let frame = read_frame(&mut cursor).unwrap().unwrap();
        assert_eq!(frame.tag, TAG_STDOUT);
        assert_eq!(frame.payload, b"hello");
    }

    #[test]
    fn empty_payload_round_trip() {
        let mut buf = Vec::new();
        write_frame(&mut buf, TAG_EOF, b"").unwrap();

        let mut cursor = Cursor::new(buf);
        let frame = read_frame(&mut cursor).unwrap().unwrap();
        assert_eq!(frame.tag, TAG_EOF);
        assert!(frame.payload.is_empty());
    }

    #[test]
    fn clean_eof_at_boundary_returns_none() {
        let mut cursor = Cursor::new(Vec::<u8>::new());
        let frame = read_frame(&mut cursor).unwrap();
        assert!(frame.is_none());
    }

    #[test]
    fn truncated_mid_frame_is_an_error() {
        // Length prefix claims 10 bytes but only 2 follow.
        let mut buf = vec![0u8, 0, 0, 10];
        buf.extend_from_slice(&[TAG_STDOUT, b'h']);
        let mut cursor = Cursor::new(buf);
        let err = read_frame(&mut cursor).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn zero_length_frame_is_rejected() {
        // A frame must contain at least the tag byte.
        let buf = vec![0u8, 0, 0, 0];
        let mut cursor = Cursor::new(buf);
        let err = read_frame(&mut cursor).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn oversized_length_prefix_is_rejected_before_allocating() {
        // A corrupt prefix one byte over the cap must be rejected as
        // InvalidData, not attempted as an allocation-then-EOF.
        let len = (MAX_FRAME_LEN + 1) as u32;
        let mut buf = len.to_be_bytes().to_vec();
        buf.push(TAG_STDOUT);
        let mut cursor = Cursor::new(buf);
        let err = read_frame(&mut cursor).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("cap"), "unexpected error: {err}");
    }

    #[test]
    fn length_at_the_cap_is_not_rejected_by_the_cap() {
        // Exactly MAX_FRAME_LEN passes the cap check and proceeds to the
        // body read, which then fails as a truncation (UnexpectedEof, not
        // InvalidData) because no body follows.
        let len = MAX_FRAME_LEN as u32;
        let buf = len.to_be_bytes().to_vec();
        let mut cursor = Cursor::new(buf);
        let err = read_frame(&mut cursor).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn truncation_mid_length_prefix_is_an_error() {
        // EOF after 1..3 bytes of the 4-byte length prefix is a truncated
        // stream, not the clean-EOF-at-a-boundary case.
        for prefix_len in 1..4 {
            let buf = vec![0u8; prefix_len];
            let mut cursor = Cursor::new(buf);
            let err = read_frame(&mut cursor).unwrap_err();
            assert_eq!(
                err.kind(),
                io::ErrorKind::UnexpectedEof,
                "prefix of {prefix_len} bytes"
            );
        }
    }

    #[test]
    fn multiple_frames_in_sequence() {
        let mut buf = Vec::new();
        write_frame(&mut buf, TAG_STDOUT, b"one").unwrap();
        write_frame(&mut buf, TAG_STDERR, b"two").unwrap();

        let mut cursor = Cursor::new(buf);
        let first = read_frame(&mut cursor).unwrap().unwrap();
        let second = read_frame(&mut cursor).unwrap().unwrap();
        assert_eq!((first.tag, first.payload), (TAG_STDOUT, b"one".to_vec()));
        assert_eq!((second.tag, second.payload), (TAG_STDERR, b"two".to_vec()));
        assert!(read_frame(&mut cursor).unwrap().is_none());
    }
}
