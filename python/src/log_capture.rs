//! Per-step capture of log output via a tee writer on the fmt layer.
//!
//! The fmt layer's writer tees to both stderr and a thread-local buffer.
//! When capture is active (`start()` called), formatted output is copied
//! into the buffer. `take()` returns the buffered output and stops capture.

use std::cell::RefCell;
use std::io;
use tracing_subscriber::fmt::MakeWriter;

thread_local! {
    static BUFFER: RefCell<Option<Vec<u8>>> = const { RefCell::new(None) };
}

/// Start capturing formatted log output on the current thread.
pub fn start() {
    BUFFER.with(|b| {
        *b.borrow_mut() = Some(Vec::new());
    });
}

/// Stop capturing and return all captured output as a string.
pub fn take() -> String {
    BUFFER.with(|b| {
        b.borrow_mut()
            .take()
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .unwrap_or_default()
    })
}

pub struct TeeWriter;

impl<'a> MakeWriter<'a> for TeeWriter {
    type Writer = TeeIo;

    fn make_writer(&'a self) -> Self::Writer {
        TeeIo
    }
}

pub struct TeeIo;

impl io::Write for TeeIo {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        io::stderr().write_all(buf)?;
        BUFFER.with(|b| {
            if let Some(ref mut vec) = *b.borrow_mut() {
                vec.extend_from_slice(buf);
            }
        });
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        io::stderr().flush()
    }
}
