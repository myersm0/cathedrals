use anyhow::Result;
use rusqlite::{params, Connection};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct Claim {
	pub id: i64,
	pub document_id: i64,
	pub entry_id: Option<i64>,
	pub author: Option<String>,
	pub content: String,
	pub created_at: String,
	pub model: String,
	pub prompt_hash: String,
}

pub fn insert_claim(
	connection: &Connection,
	document_id: i64,
	entry_id: Option<i64>,
	author: Option<&str>,
	content: &str,
	model: &str,
	prompt_hash: &str,
) -> Result<i64> {
	let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
	connection.execute(
		"INSERT INTO claims (document_id, entry_id, author, content, created_at, model, prompt_hash)
		 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
		params![document_id, entry_id, author, content, now, model, prompt_hash],
	)?;
	Ok(connection.last_insert_rowid())
}

pub fn get_claims_for_document(connection: &Connection, document_id: i64) -> Result<Vec<Claim>> {
	let mut stmt = connection.prepare(
		"SELECT id, document_id, entry_id, author, content, created_at, model, prompt_hash
		 FROM claims WHERE document_id = ?1 ORDER BY id"
	)?;
	let claims = stmt
		.query_map(params![document_id], |row| {
			Ok(Claim {
				id: row.get(0)?,
				document_id: row.get(1)?,
				entry_id: row.get(2)?,
				author: row.get(3)?,
				content: row.get(4)?,
				created_at: row.get(5)?,
				model: row.get(6)?,
				prompt_hash: row.get(7)?,
			})
		})?
		.collect::<std::result::Result<Vec<_>, _>>()?;
	Ok(claims)
}

pub fn delete_claims_for_document(connection: &Connection, document_id: i64) -> Result<usize> {
	let count = connection.execute(
		"DELETE FROM claims WHERE document_id = ?1",
		params![document_id],
	)?;
	Ok(count)
}

pub fn claim_count(connection: &Connection) -> Result<i64> {
	let count: i64 = connection.query_row(
		"SELECT COUNT(*) FROM claims", [], |row| row.get(0),
	)?;
	Ok(count)
}

pub fn documents_with_claims_count(connection: &Connection) -> Result<i64> {
	let count: i64 = connection.query_row(
		"SELECT COUNT(DISTINCT document_id) FROM claims", [], |row| row.get(0),
	)?;
	Ok(count)
}

pub fn get_documents_needing_extraction(
	connection: &Connection,
	model: &str,
	prompt_hash: &str,
) -> Result<Vec<i64>> {
	let mut stmt = connection.prepare(
		"SELECT d.id FROM documents d
		 WHERE NOT EXISTS (
			SELECT 1 FROM claims c
			WHERE c.document_id = d.id AND c.model = ?1 AND c.prompt_hash = ?2
		 )"
	)?;
	let ids = stmt
		.query_map(params![model, prompt_hash], |r| r.get(0))?
		.filter_map(|r| r.ok())
		.collect();
	Ok(ids)
}
