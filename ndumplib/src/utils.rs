use std::collections::{HashMap, HashSet};

use rusqlite::{CachedStatement, Connection, Transaction};

use crate::{Result, ResultUtils};

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

pub(crate) fn get_database_tables(connection: &impl CanPrepare) -> Result<HashSet<String>> {
    let mut statement = connection
        .prepare_cached_common("SELECT * FROM sqlite_master WHERE type = 'table'")
        .ndl("Failed to retrieve created tables from catalog DB")?;
    let mut tables = HashSet::new();
    let mut rows = statement
        .query(())
        .ndl("Failed to retrieve created tables from catalog DB")?;
    while let Some(row) = rows
        .next()
        .ndl("Failed to retrieve created tables from catalog DB")?
    {
        tables.insert(
            row.get("name")
                .ndl("Failed to retrieve created tables from catalog DB")?,
        );
    }
    Ok(tables)
}

pub(crate) fn get_database_indexes(
    connection: &impl CanPrepare,
) -> Result<HashMap<String, String>> {
    let mut statement = connection
        .prepare_cached_common("SELECT * FROM sqlite_master WHERE type = 'index'")
        .ndl("Failed to retrieve created tables from catalog DB")?;
    let mut indexes: HashMap<String, String> = HashMap::new();
    let mut rows = statement
        .query(())
        .ndl("Failed to retrieve created tables from catalog DB")?;
    while let Some(row) = rows
        .next()
        .ndl("Failed to retrieve created tables from catalog DB")?
    {
        indexes.insert(
            row.get("name")
                .ndl("Failed to retrieve created tables from catalog DB")?,
            row.get("tbl_name")
                .ndl("Failed to retrieve created tables from catalog DB")?,
        );
    }
    Ok(indexes)
}
