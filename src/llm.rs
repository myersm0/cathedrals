use anyhow::Result;

pub trait LlmBackend: Send + Sync {
	fn generate(
		&self,
		prompt: &str,
		model: &str,
		system: Option<&str>,
		format: Option<&str>,
	) -> Result<String>;

	fn chat(&self, prompt: &str, model: &str) -> Result<String> {
		self.generate(prompt, model, None, None)
	}

	fn embed(&self, text: &str, model: &str) -> Result<Vec<f32>>;
}
