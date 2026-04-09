use anyhow::Result;
use rusqlite::Connection;

use crate::config::ExtractConfig;
use crate::llm::LlmBackend;
use crate::storage;
use crate::util;

pub struct ExtractOptions {
	pub force: bool,
	pub limit: Option<usize>,
	pub status: bool,
}

pub fn run_status(connection: &Connection) -> Result<()> {
	let total_docs: i64 = storage::document_count(connection)?;
	let docs_with_claims = storage::documents_with_claims_count(connection)?;
	let total_claims = storage::claim_count(connection)?;
	println!("Extraction status:");
	println!("  total documents:    {}", total_docs);
	println!("  with claims:        {}", docs_with_claims);
	println!("  missing:            {}", total_docs - docs_with_claims);
	println!("  total claims:       {}", total_claims);
	Ok(())
}

pub fn run(
	connection: &Connection,
	backend: &dyn LlmBackend,
	config: &ExtractConfig,
	options: &ExtractOptions,
) -> Result<()> {
	let doc_ids: Vec<i64> = if options.force {
		let mut stmt = connection.prepare("SELECT id FROM documents")?;
		let ids = stmt.query_map([], |r| r.get(0))?.filter_map(|r| r.ok()).collect();
		ids
	} else {
		storage::get_documents_needing_extraction(connection)?
	};

	if doc_ids.is_empty() {
		println!("no documents need extraction");
		return Ok(());
	}

	let doc_ids: Vec<i64> = match options.limit {
		Some(lim) => doc_ids.into_iter().take(lim).collect(),
		None => doc_ids,
	};

	println!("extracting claims from {} documents...", doc_ids.len());
	println!("  model: {}", config.model);

	let mut total_claims = 0usize;
	for (i, doc_id) in doc_ids.iter().enumerate() {
		let (source_title, doctype_name): (String, Option<String>) = connection.query_row(
			"SELECT source_title, doctype_name FROM documents WHERE id = ?1",
			[doc_id],
			|row| Ok((row.get(0)?, row.get(1)?)),
		)?;

		eprint!(
			"\r  [{}/{}] {}...",
			i + 1,
			doc_ids.len(),
			util::truncate_str(&source_title, 40),
		);

		if options.force {
			storage::delete_claims_for_document(connection, *doc_id)?;
		}

		let count = extract_document(
			connection, backend, config, *doc_id, doctype_name.as_deref(),
		)?;
		total_claims += count;
	}
	eprintln!();
	println!("done — {} claims extracted", total_claims);
	Ok(())
}

fn extract_document(
	connection: &Connection,
	backend: &dyn LlmBackend,
	config: &ExtractConfig,
	document_id: i64,
	doctype_name: Option<&str>,
) -> Result<usize> {
	let full_text = storage::get_document_full_text(connection, document_id)?;
	let prompt = build_prompt(config, doctype_name, &full_text);
	let response = backend.chat(&prompt, &config.model)?;
	let claims = parse_claims(&response);

	let author = resolve_author(connection, document_id);

	for (kind, content) in &claims {
		storage::insert_claim(
			connection,
			document_id,
			None,
			author.as_deref(),
			content,
			kind,
			&config.model,
		)?;
	}

	Ok(claims.len())
}

fn build_prompt(config: &ExtractConfig, doctype_name: Option<&str>, document_text: &str) -> String {
	let framing = config.get_framing(doctype_name);
	let rules = config.get_rules();
	match framing {
		Some(f) => format!("{}{}\n{}", f, rules, document_text),
		None => format!("{}\n{}", rules, document_text),
	}
}

fn parse_claims(response: &str) -> Vec<(String, String)> {
	let mut claims = Vec::new();
	for line in response.lines() {
		let trimmed = line.trim();
		if trimmed.is_empty() {
			continue;
		}
		if let Some(parsed) = parse_claim_line(trimmed) {
			claims.push(parsed);
		}
	}
	claims
}

fn parse_claim_line(line: &str) -> Option<(String, String)> {
	if !line.starts_with('[') {
		return None;
	}
	let bracket_end = line.find(']')?;
	let kind = line[1..bracket_end].trim().to_lowercase();
	let content = line[bracket_end + 1..].trim().to_string();
	if content.is_empty() {
		return None;
	}
	let valid_kinds = [
		"observation", "decision", "result", "recommendation",
		"hypothesis", "question", "plan", "limitation", "method",
	];
	if valid_kinds.contains(&kind.as_str()) {
		Some((kind, content))
	} else {
		Some(("observation".to_string(), content))
	}
}

fn resolve_author(connection: &Connection, document_id: i64) -> Option<String> {
	let mut stmt = connection.prepare(
		"SELECT DISTINCT author FROM entries WHERE document_id = ?1 AND author IS NOT NULL"
	).ok()?;
	let authors: Vec<String> = stmt
		.query_map([document_id], |r| r.get(0))
		.ok()?
		.filter_map(|r| r.ok())
		.collect();
	if authors.len() == 1 {
		Some(authors.into_iter().next().unwrap())
	} else {
		None
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_claim_line_basic() {
		let (kind, content) = parse_claim_line("[result] The F1 score was 0.91.").unwrap();
		assert_eq!(kind, "result");
		assert_eq!(content, "The F1 score was 0.91.");
	}

	#[test]
	fn parse_claim_line_unknown_kind_falls_back() {
		let (kind, content) = parse_claim_line("[finding] Some claim text.").unwrap();
		assert_eq!(kind, "observation");
		assert_eq!(content, "Some claim text.");
	}

	#[test]
	fn parse_claim_line_empty_content_rejected() {
		assert!(parse_claim_line("[result]").is_none());
		assert!(parse_claim_line("[result]  ").is_none());
	}

	#[test]
	fn parse_claim_line_no_bracket_rejected() {
		assert!(parse_claim_line("Just a regular line.").is_none());
	}

	#[test]
	fn parse_claims_filters_junk() {
		let response = "\
[observation] Claim one.
Some preamble the model shouldn't have emitted.
[method] Claim two.

[result] Claim three.";
		let claims = parse_claims(response);
		assert_eq!(claims.len(), 3);
		assert_eq!(claims[0].0, "observation");
		assert_eq!(claims[1].0, "method");
		assert_eq!(claims[2].0, "result");
	}
}
