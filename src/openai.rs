use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::llm::LlmBackend;

#[derive(Serialize)]
struct ChatMessage {
	role: String,
	content: String,
}

#[derive(Serialize)]
struct ChatRequest {
	model: String,
	messages: Vec<ChatMessage>,
	#[serde(skip_serializing_if = "Option::is_none")]
	response_format: Option<ResponseFormat>,
}

#[derive(Serialize)]
struct ResponseFormat {
	r#type: String,
}

#[derive(Deserialize)]
struct ChatResponse {
	choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct ChatChoice {
	message: ChatMessageResponse,
}

#[derive(Deserialize)]
struct ChatMessageResponse {
	content: String,
}

#[derive(Serialize)]
struct EmbeddingRequest {
	model: String,
	input: String,
}

#[derive(Deserialize)]
struct EmbeddingResponse {
	data: Vec<EmbeddingData>,
}

#[derive(Deserialize)]
struct EmbeddingData {
	embedding: Vec<f32>,
}

pub struct OpenAiClient {
	base_url: String,
	api_key: String,
	client: reqwest::blocking::Client,
}

impl OpenAiClient {
	pub fn new(base_url: &str, api_key: &str) -> Self {
		OpenAiClient {
			base_url: base_url.to_string(),
			api_key: api_key.to_string(),
			client: reqwest::blocking::Client::builder()
				.timeout(std::time::Duration::from_secs(600))
				.build()
				.expect("failed to build http client"),
		}
	}

	pub fn from_env() -> Result<Self> {
		let api_key = std::env::var("OPENAI_API_KEY")
			.context("OPENAI_API_KEY environment variable not set")?;
		let base_url = std::env::var("OPENAI_API_BASE")
			.unwrap_or_else(|_| "https://api.openai.com/v1".to_string());
		Ok(Self::new(&base_url, &api_key))
	}
}

impl LlmBackend for OpenAiClient {
	fn generate(
		&self,
		prompt: &str,
		model: &str,
		system: Option<&str>,
		format: Option<&str>,
	) -> Result<String> {
		let mut messages = Vec::new();
		if let Some(system_prompt) = system {
			messages.push(ChatMessage {
				role: "system".to_string(),
				content: system_prompt.to_string(),
			});
		}
		messages.push(ChatMessage {
			role: "user".to_string(),
			content: prompt.to_string(),
		});

		let response_format = match format {
			Some("json") => Some(ResponseFormat {
				r#type: "json_object".to_string(),
			}),
			_ => None,
		};

		let request = ChatRequest {
			model: model.to_string(),
			messages,
			response_format,
		};

		let response: ChatResponse = self
			.client
			.post(format!("{}/chat/completions", self.base_url))
			.bearer_auth(&self.api_key)
			.json(&request)
			.send()?
			.error_for_status()
			.context("OpenAI API request failed")?
			.json()?;

		response
			.choices
			.into_iter()
			.next()
			.map(|c| c.message.content)
			.context("no response from OpenAI API")
	}

	fn embed(&self, text: &str, model: &str) -> Result<Vec<f32>> {
		let request = EmbeddingRequest {
			model: model.to_string(),
			input: text.to_string(),
		};

		let response: EmbeddingResponse = self
			.client
			.post(format!("{}/embeddings", self.base_url))
			.bearer_auth(&self.api_key)
			.json(&request)
			.send()?
			.error_for_status()
			.context("OpenAI embeddings request failed")?
			.json()?;

		response
			.data
			.into_iter()
			.next()
			.map(|d| d.embedding)
			.context("no embedding in OpenAI response")
	}
}
