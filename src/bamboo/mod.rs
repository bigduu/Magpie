//! The Bamboo-facing layer: HTTP client, `/v2/stream` WS client, and the
//! wire types both use. See `ARCHITECTURE.md` for the full API mapping.

pub mod client;
pub mod stream;
pub mod types;

pub use client::{BambooClient, ClientError};
pub use stream::{BambooStream, StreamError, StreamEvent};
