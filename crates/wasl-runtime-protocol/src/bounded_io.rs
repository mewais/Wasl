use std::io;

use tokio::io::{AsyncBufRead, AsyncBufReadExt};

pub(crate) const MAX_STDOUT_LINE_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const EVENT_CHANNEL_CAPACITY: usize = 4096;

pub(crate) enum BoundedLineRead {
    Line(String),
    Eof,
    TooLong,
}

pub(crate) async fn discard_until_newline<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> io::Result<()> {
    loop {
        let chunk = reader.fill_buf().await?;
        if chunk.is_empty() {
            return Ok(());
        }
        if let Some(pos) = chunk.iter().position(|b| *b == b'\n') {
            reader.consume(pos + 1);
            return Ok(());
        }
        let len = chunk.len();
        reader.consume(len);
    }
}

/// Consume and discard exactly one line (bounded the same way as
/// [`read_bounded_line`]) WITHOUT ever allocating its content into a `String`.
/// Used for drains whose content is thrown away regardless (stderr, §4.4) —
/// building a `String` per line there is pure overhead for content nobody
/// reads. Returns `Ok(true)` if a line (or an EOF-terminated partial line) was
/// consumed, `Ok(false)` on a clean EOF with nothing pending.
pub(crate) async fn discard_one_bounded_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max_bytes: usize,
) -> io::Result<bool> {
    let mut seen = 0usize;
    loop {
        let chunk = reader.fill_buf().await?;
        if chunk.is_empty() {
            return Ok(seen > 0);
        }
        if let Some(pos) = chunk.iter().position(|b| *b == b'\n') {
            reader.consume(pos + 1);
            return Ok(true);
        }
        let len = chunk.len();
        seen = seen.saturating_add(len);
        reader.consume(len);
        if seen > max_bytes {
            discard_until_newline(reader).await?;
            return Ok(true);
        }
    }
}

pub(crate) async fn read_bounded_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max_bytes: usize,
) -> io::Result<BoundedLineRead> {
    let mut line = Vec::new();
    loop {
        let chunk = reader.fill_buf().await?;
        if chunk.is_empty() {
            if line.is_empty() {
                return Ok(BoundedLineRead::Eof);
            }
            return Ok(BoundedLineRead::Line(
                String::from_utf8_lossy(&line).to_string(),
            ));
        }
        if let Some(pos) = chunk.iter().position(|b| *b == b'\n') {
            let take = pos + 1;
            if line.len().saturating_add(take) > max_bytes {
                reader.consume(take);
                return Ok(BoundedLineRead::TooLong);
            }
            line.extend_from_slice(&chunk[..take]);
            reader.consume(take);
            if line.last() == Some(&b'\n') {
                line.pop();
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
            }
            return Ok(BoundedLineRead::Line(
                String::from_utf8_lossy(&line).to_string(),
            ));
        }

        if line.len().saturating_add(chunk.len()) > max_bytes {
            let len = chunk.len();
            reader.consume(len);
            discard_until_newline(reader).await?;
            return Ok(BoundedLineRead::TooLong);
        }

        line.extend_from_slice(chunk);
        let len = chunk.len();
        reader.consume(len);
    }
}
