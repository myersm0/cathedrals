use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LengthTier {
	Short,
	Medium,
	Long,
}

impl LengthTier {
	pub fn from_len(content_len: usize, short_threshold: usize, medium_threshold: usize) -> Self {
		if content_len < short_threshold {
			LengthTier::Short
		} else if content_len < medium_threshold {
			LengthTier::Medium
		} else {
			LengthTier::Long
		}
	}

	pub fn key(&self) -> &'static str {
		match self {
			LengthTier::Short => "short",
			LengthTier::Medium => "medium",
			LengthTier::Long => "long",
		}
	}
}

pub fn detailed_summary_prompt(document_text: &str, instructions: &str) -> String {
	format!("{}\n{}", instructions, document_text)
}

pub fn brief_summary_prompt(detailed_summary: &str, instructions: &str) -> String {
	format!("{}\n{}", instructions, detailed_summary)
}

pub fn claim_extraction_prompt(
	document_text: &str,
	rules: &str,
	framing: Option<&str>,
) -> String {
	match framing {
		Some(f) => format!("{}{}\n{}", f, rules, document_text),
		None => format!("{}\n{}", rules, document_text),
	}
}

pub fn compute_prompt_hash(rules: &str) -> String {
	let mut hasher = DefaultHasher::new();
	rules.hash(&mut hasher);
	format!("{:016x}", hasher.finish())
}

pub fn default_detailed_prompt(tier: LengthTier) -> &'static str {
	match tier {
		LengthTier::Short => DEFAULT_DETAILED_SHORT,
		LengthTier::Medium => DEFAULT_DETAILED_MEDIUM,
		LengthTier::Long => DEFAULT_DETAILED_LONG,
	}
}

pub fn default_brief_prompt() -> &'static str {
	DEFAULT_BRIEF
}

pub fn default_extract_rules() -> &'static str {
	DEFAULT_EXTRACT_RULES
}

const DEFAULT_DETAILED_SHORT: &str = r#"Summarize briefly. 1-2 sentences max. No filler.

Document:
"#;

const DEFAULT_DETAILED_MEDIUM: &str = r#"Summarize this document. Cover the main points directly.
Be proportional — a few sentences to a paragraph.
No hedging, no meta-commentary.

Document:
"#;

const DEFAULT_DETAILED_LONG: &str = r#"Summarize this document by section:
1. What is the main topic/claim?
2. What evidence or points are made in the body?
3. What conclusions or outcomes are reached?

Be thorough but not verbose. No filler.

Document:
"#;

const DEFAULT_BRIEF: &str = r#"Compress to 1-2 sentences. Output ONLY the summary, then stop.
Never include:
- "---" or separators
- Explanations of your summary
- "Feel free to ask" or similar
- Who/what/why breakdowns

Summary to compress:
"#;

const DEFAULT_EXTRACT_RULES: &str = include_str!("prompts/extract_default.txt");
