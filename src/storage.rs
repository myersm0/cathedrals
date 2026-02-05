use anyhow::Result;
use rusqlite::{params, Connection};

use crate::types::*;

pub fn initialize(connection: &Connection) -> Result<()> {
	connection.execute_batch(
		"
		CREATE TABLE IF NOT EXISTS documents (
			id INTEGER PRIMARY KEY,
			source_title TEXT NOT NULL,
			merge_strategy TEXT NOT NULL CHECK (merge_strategy IN ('none', 'positional', 'timestamped')),
			origin_path TEXT
		);

		CREATE TABLE IF NOT EXISTS entries (
			id INTEGER PRIMARY KEY,
			document_id INTEGER NOT NULL REFERENCES documents(id),
			body TEXT NOT NULL,
			author TEXT,
			timestamp TEXT,
			source_title TEXT NOT NULL,
			clip_date TEXT NOT NULL,
			file_path TEXT NOT NULL,
			position INTEGER NOT NULL,
			heading_level INTEGER,
			heading_title TEXT,
			is_quote INTEGER NOT NULL DEFAULT 0,
			minhash BLOB NOT NULL
		);

		CREATE TABLE IF NOT EXISTS chunks (
			id INTEGER PRIMARY KEY,
			entry_id INTEGER NOT NULL REFERENCES entries(id) ON DELETE CASCADE,
			chunk_index INTEGER NOT NULL,
			start_char INTEGER NOT NULL,
			end_char INTEGER NOT NULL,
			body TEXT NOT NULL
		);

		CREATE INDEX IF NOT EXISTS chunks_entry_id ON chunks(entry_id);

		CREATE TABLE IF NOT EXISTS media (
			id INTEGER PRIMARY KEY,
			file_path TEXT NOT NULL,
			media_type TEXT NOT NULL CHECK (media_type IN ('screenshot', 'audio', 'transcript_segment')),
			timestamp TEXT NOT NULL,
			duration_seconds REAL,
			document_id INTEGER REFERENCES documents(id)
		);

		CREATE TABLE IF NOT EXISTS timeline_links (
			media_id INTEGER NOT NULL REFERENCES media(id),
			entry_id INTEGER NOT NULL REFERENCES entries(id),
			PRIMARY KEY (media_id, entry_id)
		);

		CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
			body,
			content=chunks,
			content_rowid=id
		);

		CREATE TRIGGER IF NOT EXISTS chunks_fts_insert AFTER INSERT ON chunks BEGIN
			INSERT INTO chunks_fts(rowid, body)
			VALUES (new.id, new.body);
		END;

		CREATE TRIGGER IF NOT EXISTS chunks_fts_delete AFTER DELETE ON chunks BEGIN
			INSERT INTO chunks_fts(chunks_fts, rowid, body)
			VALUES ('delete', old.id, old.body);
		END;

		CREATE TRIGGER IF NOT EXISTS chunks_fts_update AFTER UPDATE ON chunks BEGIN
			INSERT INTO chunks_fts(chunks_fts, rowid, body)
			VALUES ('delete', old.id, old.body);
			INSERT INTO chunks_fts(rowid, body)
			VALUES (new.id, new.body);
		END;
		",
	)?;
	Ok(())
}

fn merge_strategy_to_str(strategy: MergeStrategy) -> &'static str {
	match strategy {
		MergeStrategy::None => "none",
		MergeStrategy::Positional => "positional",
		MergeStrategy::Timestamped => "timestamped",
	}
}

pub fn insert_document(
	connection: &Connection,
	source_title: &str,
	merge_strategy: MergeStrategy,
	origin_path: Option<&str>,
) -> Result<DocumentId> {
	connection.execute(
		"INSERT INTO documents (source_title, merge_strategy, origin_path)
		 VALUES (?1, ?2, ?3)",
		params![
			source_title,
			merge_strategy_to_str(merge_strategy),
			origin_path,
		],
	)?;
	Ok(DocumentId(connection.last_insert_rowid()))
}

pub fn insert_entry(
	connection: &Connection,
	document_id: DocumentId,
	entry: &SegmentedEntry,
	position: u32,
	source_title: &str,
	clip_date: &str,
	file_path: &str,
	minhash: &MinHashSignature,
) -> Result<EntryId> {
	let minhash_bytes: Vec<u8> = minhash
		.iter()
		.flat_map(|v| v.to_le_bytes())
		.collect();
	connection.execute(
		"INSERT INTO entries (
			document_id, body, author, timestamp, source_title,
			clip_date, file_path, position, heading_level, heading_title,
			is_quote, minhash
		) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
		params![
			document_id.0,
			entry.body,
			entry.author,
			entry.timestamp,
			source_title,
			clip_date,
			file_path,
			position,
			entry.heading_level.map(|l| l as i32),
			entry.heading_title,
			entry.is_quote as i32,
			minhash_bytes,
		],
	)?;
	Ok(EntryId(connection.last_insert_rowid()))
}

pub fn insert_chunks(
	connection: &Connection,
	entry_id: EntryId,
	chunks: &[crate::chunking::Chunk],
) -> Result<()> {
	for chunk in chunks {
		connection.execute(
			"INSERT INTO chunks (entry_id, chunk_index, start_char, end_char, body)
			 VALUES (?1, ?2, ?3, ?4, ?5)",
			params![
				entry_id.0,
				chunk.chunk_index,
				chunk.start_char,
				chunk.end_char,
				chunk.body,
			],
		)?;
	}
	Ok(())
}

pub struct ChunkSearchResult {
	pub chunk_id: i64,
	pub entry_id: i64,
	pub document_id: i64,
	pub chunk_body: String,
	pub chunk_index: u32,
	pub entry_position: u32,
	pub author: Option<String>,
	pub source_title: String,
	pub clip_date: String,
	pub heading_title: Option<String>,
	pub rank: f64,
}

pub struct GroupedSearchResult {
	pub document_id: i64,
	pub source_title: String,
	pub clip_date: String,
	pub chunks: Vec<ChunkHit>,
}

pub struct ChunkHit {
	pub entry_id: i64,
	pub entry_position: u32,
	pub chunk_index: u32,
	pub chunk_body: String,
	pub author: Option<String>,
	pub heading_title: Option<String>,
	pub rank: f64,
}

pub fn search(connection: &Connection, query: &str) -> Result<Vec<GroupedSearchResult>> {
	let mut statement = connection.prepare(
		"SELECT c.id, c.entry_id, e.document_id, c.body, c.chunk_index, e.position,
		        e.author, e.source_title, e.clip_date, e.heading_title, f.rank
		 FROM chunks_fts f
		 JOIN chunks c ON c.id = f.rowid
		 JOIN entries e ON e.id = c.entry_id
		 WHERE chunks_fts MATCH ?1
		 ORDER BY f.rank
		 LIMIT 50",
	)?;
	let rows: Vec<ChunkSearchResult> = statement
		.query_map(params![query], |row| {
			Ok(ChunkSearchResult {
				chunk_id: row.get(0)?,
				entry_id: row.get(1)?,
				document_id: row.get(2)?,
				chunk_body: row.get(3)?,
				chunk_index: row.get(4)?,
				entry_position: row.get(5)?,
				author: row.get(6)?,
				source_title: row.get(7)?,
				clip_date: row.get(8)?,
				heading_title: row.get(9)?,
				rank: row.get(10)?,
			})
		})?
		.collect::<std::result::Result<Vec<_>, _>>()?;

	let mut grouped: Vec<GroupedSearchResult> = Vec::new();
	for row in rows {
		let doc = grouped.iter_mut().find(|d| d.document_id == row.document_id);
		let hit = ChunkHit {
			entry_id: row.entry_id,
			entry_position: row.entry_position,
			chunk_index: row.chunk_index,
			chunk_body: row.chunk_body,
			author: row.author,
			heading_title: row.heading_title,
			rank: row.rank,
		};
		match doc {
			Some(doc) => doc.chunks.push(hit),
			None => grouped.push(GroupedSearchResult {
				document_id: row.document_id,
				source_title: row.source_title,
				clip_date: row.clip_date,
				chunks: vec![hit],
			}),
		}
	}

	for doc in &mut grouped {
		doc.chunks.sort_by_key(|c| (c.entry_position, c.chunk_index));
	}

	Ok(grouped)
}

pub fn document_count(connection: &Connection) -> Result<i64> {
	Ok(connection.query_row("SELECT COUNT(*) FROM documents", [], |row| row.get(0))?)
}

pub fn entry_count(connection: &Connection) -> Result<i64> {
	Ok(connection.query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))?)
}

pub fn chunk_count(connection: &Connection) -> Result<i64> {
	Ok(connection.query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))?)
}

pub struct DumpEntry {
	pub body: String,
	pub author: Option<String>,
	pub heading_title: Option<String>,
	pub position: u32,
}

pub struct DumpDocument {
	pub document_id: i64,
	pub source_title: String,
	pub merge_strategy: String,
	pub entries: Vec<DumpEntry>,
}

pub fn dump_document(connection: &Connection, title_filter: Option<&str>) -> Result<Vec<DumpDocument>> {
	let (where_clause, filter_param) = match title_filter {
		Some(filter) => ("WHERE d.source_title LIKE ?1", format!("%{}%", filter)),
		None => ("", String::new()),
	};
	let query = format!(
		"SELECT d.id, d.source_title, d.merge_strategy,
		        e.body, e.author, e.heading_title, e.position
		 FROM documents d
		 JOIN entries e ON e.document_id = d.id
		 {} ORDER BY d.id, e.position",
		where_clause
	);
	let mut statement = connection.prepare(&query)?;
	let rows: Vec<(i64, String, String, String, Option<String>, Option<String>, u32)> = if title_filter.is_some() {
		statement.query_map([&filter_param], |row| {
			Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?))
		})?.collect::<std::result::Result<Vec<_>, _>>()?
	} else {
		statement.query_map([], |row| {
			Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?))
		})?.collect::<std::result::Result<Vec<_>, _>>()?
	};

	let mut documents: Vec<DumpDocument> = Vec::new();
	for (doc_id, source_title, merge_strategy, body, author, heading_title, position) in rows {
		let doc = documents.iter_mut().find(|d| d.document_id == doc_id);
		let entry = DumpEntry { body, author, heading_title, position };
		match doc {
			Some(doc) => doc.entries.push(entry),
			None => documents.push(DumpDocument {
				document_id: doc_id,
				source_title,
				merge_strategy,
				entries: vec![entry],
			}),
		}
	}
	Ok(documents)
}
