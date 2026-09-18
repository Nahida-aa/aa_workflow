//! Middleware 层：对齐 TanStack `src/middleware/`。

mod create_middleware;
pub use create_middleware::{CtxProducer, CtxWrapper, Middleware};
