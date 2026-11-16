//! MySQL-protocol access to the Doris FE (Broker Load jobs, metadata, seq bootstrap).

use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Pool, SslOpts};

use crate::error::{Error, Result};
use crate::loader::quote_ident;
use crate::schema::{SeqTarget, TableDef};

#[derive(Clone, Debug)]
pub struct MysqlConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    /// Require TLS (managed services such as VeloDB Cloud).
    pub tls: bool,
}

impl MysqlConfig {
    pub fn pool(&self) -> Pool {
        crate::tls::ensure_crypto_provider();
        let mut b = OptsBuilder::default()
            .ip_or_hostname(self.host.clone())
            .tcp_port(self.port)
            .user(Some(self.user.clone()))
            .pass(Some(self.password.clone()))
            .prefer_socket(false);
        if self.tls {
            b = b.ssl_opts(Some(SslOpts::default()));
        }
        Pool::new(Opts::from(b))
    }
}

pub(crate) fn sql_err(e: mysql_async::Error) -> Error {
    Error::Sql(e.to_string())
}

/// Largest sequence value already in the Doris table, for [`crate::Engine::attach`]'s
/// `min_next_seq` (pass the result + 1).
pub async fn max_seq(pool: &Pool, table: &TableDef) -> Result<u64> {
    let mut conn = pool.get_conn().await.map_err(sql_err)?;
    // Delete-marked rows are hidden by default but their sequence values still count: starting
    // below a tombstone's seq would make Doris ignore new writes of that key.
    conn.query_drop("SET show_hidden_columns = true").await.map_err(sql_err)?;
    let column = match &table.seq {
        SeqTarget::Column(c) => quote_ident(c),
        SeqTarget::Hidden => quote_ident("__DORIS_SEQUENCE_COL__"),
    };
    let sql = format!(
        "SELECT CAST(COALESCE(MAX({column}), 0) AS BIGINT) FROM {}.{}",
        quote_ident(&table.db),
        quote_ident(&table.name)
    );
    let v: Option<i64> = conn.query_first(sql).await.map_err(sql_err)?;
    Ok(v.unwrap_or(0).max(0) as u64)
}

/// Reads a Doris table's definition (`SHOW CREATE TABLE`) and checks it can be buffered:
/// Unique Key model, merge-on-write, and a sequence column. The mapped sequence column is owned by
/// doris-ingress and left out of the client schema.
pub async fn describe_table(pool: &Pool, db: &str, table: &str, config: crate::config::TableConfig) -> Result<TableDef> {
    let mut conn = pool.get_conn().await.map_err(sql_err)?;
    let sql = format!("SHOW CREATE TABLE {}.{}", quote_ident(db), quote_ident(table));
    let row: Option<(String, String)> = conn.query_first(sql).await.map_err(sql_err)?;
    let (_, ddl) = row.ok_or_else(|| Error::UnknownTable(format!("{db}.{table}")))?;
    table_def_from_ddl(db, table, &ddl, config)
}

/// Parses `SHOW CREATE TABLE` output into a [`TableDef`].
pub fn table_def_from_ddl(db: &str, table: &str, ddl: &str, config: crate::config::TableConfig) -> Result<TableDef> {
    use arrow::datatypes::{Field, Schema};
    let bad = |m: String| Error::Schema(format!("{db}.{table}: {m}"));

    let props = parse_properties(ddl);
    if props.get("enable_unique_key_merge_on_write").map(String::as_str) != Some("true") {
        return Err(bad("not a merge-on-write table (enable_unique_key_merge_on_write must be true)".into()));
    }
    let keys = section_list(ddl, "UNIQUE KEY(").ok_or_else(|| bad("not a Unique Key table".into()))?;
    let seq = if let Some(col) = props.get("function_column.sequence_col") {
        SeqTarget::Column(col.clone())
    } else if props.contains_key("function_column.sequence_type") {
        SeqTarget::Hidden
    } else {
        return Err(bad("no sequence column (set function_column.sequence_col or sequence_type)".into()));
    };

    let mut fields = Vec::new();
    for line in ddl.lines() {
        let line = line.trim();
        if line.starts_with(") ENGINE") || line.starts_with(")ENGINE") {
            break;
        }
        let Some(rest) = line.strip_prefix('`') else { continue };
        let Some((name, rest)) = rest.split_once('`') else { continue };
        let rest = rest.trim();
        let ty = rest.split_whitespace().next().unwrap_or_default();
        let nullable = !rest.contains("NOT NULL");
        if let SeqTarget::Column(c) = &seq
            && c == name
        {
            continue;
        }
        let dt = doris_type_to_arrow(ty).ok_or_else(|| bad(format!("column {name}: unsupported type {ty}")))?;
        let key = keys.iter().any(|k| k == name);
        fields.push(Field::new(name, dt, nullable && !key));
    }
    if fields.is_empty() {
        return Err(bad("no columns found in SHOW CREATE TABLE output".into()));
    }
    Ok(TableDef {
        db: db.into(),
        name: table.into(),
        schema: std::sync::Arc::new(Schema::new(fields)),
        key_columns: keys,
        seq,
        config,
    })
}

/// Maps a Doris column type (as printed by `SHOW CREATE TABLE`) to an Arrow type.
pub fn doris_type_to_arrow(ty: &str) -> Option<arrow::datatypes::DataType> {
    use arrow::datatypes::{DataType, TimeUnit};
    let t = ty.to_ascii_lowercase();
    let base = t.split('(').next().unwrap_or(&t);
    let args: Vec<u8> = t
        .split_once('(')
        .and_then(|(_, a)| a.strip_suffix(')'))
        .map(|a| a.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_default();
    Some(match base {
        "boolean" => DataType::Boolean,
        "tinyint" => DataType::Int8,
        "smallint" => DataType::Int16,
        "int" => DataType::Int32,
        "bigint" => DataType::Int64,
        "largeint" => DataType::Decimal128(38, 0),
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "decimal" | "decimalv3" => {
            let p = *args.first().unwrap_or(&38);
            let s = *args.get(1).unwrap_or(&0) as i8;
            if p > 38 { DataType::Decimal256(p, s) } else { DataType::Decimal128(p, s) }
        }
        "date" | "datev2" => DataType::Date32,
        "datetime" | "datetimev2" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "char" | "varchar" | "text" | "string" | "json" | "jsonb" | "variant" => DataType::Utf8,
        _ => return None,
    })
}

fn parse_properties(ddl: &str) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    let Some(start) = ddl.find("PROPERTIES (") else { return out };
    for line in ddl[start..].lines().skip(1) {
        let line = line.trim().trim_end_matches(',');
        let mut parts = line.splitn(2, " = ");
        if let (Some(k), Some(v)) = (parts.next(), parts.next()) {
            out.insert(k.trim_matches('"').to_string(), v.trim_matches('"').to_string());
        }
    }
    out
}

/// Comma-separated backticked names after `prefix`, up to the closing parenthesis.
fn section_list(ddl: &str, prefix: &str) -> Option<Vec<String>> {
    let start = ddl.find(prefix)? + prefix.len();
    let end = ddl[start..].find(')')? + start;
    Some(ddl[start..end].split(',').map(|s| s.trim().trim_matches('`').to_string()).filter(|s| !s.is_empty()).collect())
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, TimeUnit};

    use super::*;

    const DDL: &str = "CREATE TABLE `describe_probe` (\n  `id` bigint NOT NULL,\n  `region` varchar(16) NOT NULL,\n  `amount` decimal(18,4) NULL,\n  `ts` datetime NULL,\n  `d` date NULL,\n  `flag` boolean NULL,\n  `n` int NULL,\n  `f` double NULL,\n  `s` text NULL,\n  `seq` bigint NULL\n) ENGINE=OLAP\nUNIQUE KEY(`id`, `region`)\nDISTRIBUTED BY HASH(`id`) BUCKETS 1\nPROPERTIES (\n\"enable_unique_key_merge_on_write\" = \"true\",\n\"light_schema_change\" = \"true\",\n\"function_column.sequence_col\" = \"seq\",\n\"disable_auto_compaction\" = \"false\"\n);";

    #[test]
    fn parses_velodb_ddl() {
        let def = table_def_from_ddl("db", "describe_probe", DDL, Default::default()).unwrap();
        assert_eq!(def.key_columns, vec!["id", "region"]);
        assert_eq!(def.seq, SeqTarget::Column("seq".into()));
        let f = |n: &str| def.schema.field_with_name(n).unwrap().clone();
        assert!(def.schema.field_with_name("seq").is_err(), "sequence column is owned by doris-ingress");
        assert_eq!(f("id").data_type(), &DataType::Int64);
        assert!(!f("region").is_nullable());
        assert_eq!(f("amount").data_type(), &DataType::Decimal128(18, 4));
        assert_eq!(f("ts").data_type(), &DataType::Timestamp(TimeUnit::Microsecond, None));
        assert_eq!(f("d").data_type(), &DataType::Date32);
        assert_eq!(f("s").data_type(), &DataType::Utf8);
        assert!(f("n").is_nullable());
    }

    #[test]
    fn rejects_tables_that_cannot_be_buffered() {
        let mor = DDL.replace("\"enable_unique_key_merge_on_write\" = \"true\"", "\"enable_unique_key_merge_on_write\" = \"false\"");
        assert!(table_def_from_ddl("db", "t", &mor, Default::default()).is_err());
        let dup = DDL.replace("UNIQUE KEY", "DUPLICATE KEY");
        assert!(table_def_from_ddl("db", "t", &dup, Default::default()).is_err());
        let noseq = DDL.replace("\"function_column.sequence_col\" = \"seq\",\n", "");
        assert!(table_def_from_ddl("db", "t", &noseq, Default::default()).is_err());
        let hidden = DDL.replace("\"function_column.sequence_col\" = \"seq\"", "\"function_column.sequence_type\" = \"bigint\"");
        let def = table_def_from_ddl("db", "t", &hidden, Default::default()).unwrap();
        assert_eq!(def.seq, SeqTarget::Hidden);
        assert!(def.schema.field_with_name("seq").is_ok(), "a plain column when the sequence is hidden");
    }
}
