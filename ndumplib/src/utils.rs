use rusqlite::{CachedStatement, Connection, Transaction};

pub(crate) trait CanPrepare {
    fn prepare_cached_common(&self, sql: &str) -> rusqlite::Result<CachedStatement>;
}

impl CanPrepare for Connection {
    #[inline(always)]
    fn prepare_cached_common(&self, sql: &str) -> rusqlite::Result<CachedStatement> {
        self.prepare_cached(sql)
    }
}

impl<'a> CanPrepare for Transaction<'a> {
    #[inline(always)]
    fn prepare_cached_common(&self, sql: &str) -> rusqlite::Result<CachedStatement> {
        self.prepare_cached(sql)
    }
}
