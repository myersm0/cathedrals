use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct GenerateRequest {
	model: String,
	prompt: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	system: Option<String>,
	stream: bool,
	#[serde(skip_serializing_if = "Option::is_none")]
	format: Option<String>,
}

#[derive(Deserialize)]
struct GenerateResponse {
	response: String,
}

#[derive(Serialize)]
struct EmbeddingRequest {
	model: String,
	prompt: String,
}

#[derive(Deserialize)]
struct EmbeddingResponse {
	embedding: Vec<f32>,
}

pub struct OllamaClient {
	pub base_url: String,
	pub model: String,
	client: reqwest::blocking::Client,
}

impl OllamaClient {
	pub fn new(base_url: &str, model: &str) -> Self {
		OllamaClient {
			base_url: base_url.to_string(),
			model: model.to_string(),
			client: reqwest::blocking::Client::builder()
				.timeout(std::time::Duration::from_secs(600))
				.build()
				.expect("failed to build http client"),
		}
	}

	pub fn generate(
		&self,
		prompt: &str,
		model: &str,
		system: Option<&str>,
		format: Option<&str>,
	) -> Result<String> {
		let request = GenerateRequest {
			model: model.to_string(),
			prompt: prompt.to_string(),
			system: system.map(|s| s.to_string()),
			stream: false,
			format: format.map(|f| f.to_string()),
		};

		let response: GenerateResponse = self
			.client
			.post(format!("{}/api/generate", self.base_url))
			.json(&request)
			.send()?
			.json()?;

		Ok(response.response)
	}

	pub fn chat(&self, prompt: &str, model: &str) -> Result<String> {
		self.generate(prompt, model, None, None)
	}

	pub fn embed(&self, text: &str, model: &str) -> Result<Vec<f32>> {
		let request = EmbeddingRequest {
			model: model.to_string(),
			prompt: text.to_string(),
		};

		let response: EmbeddingResponse = self
			.client
			.post(format!("{}/api/embeddings", self.base_url))
			.json(&request)
			.send()?
			.json()?;

		Ok(response.embedding)
	}
}
