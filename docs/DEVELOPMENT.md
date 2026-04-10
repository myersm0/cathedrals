# what-was-said Development Guide

Personal knowledge base for clipped documents with full-text and semantic search.

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                          main.rs                              │
│                     (CLI via clap, dispatch)                  │
└──────────────────────────────────────────────────────────────┘
       │              │              │              │           │
       ▼              ▼              ▼              ▼           ▼
┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐
│  ingest   │  │ storage/  │  │  derive   │  │   tui/    │  │   serve   │
│(parse +   │  │ (sqlite)  │  │  (LLM)    │  │(ratatui)  │  │  (axum)   │
│orchestrate)│  └───────────┘  └───────────┘  └───────────┘  └───────────┘
└───────────┘        │         ┌───────────┐
       │             │         │  extract  │
       ▼             │         │  (LLM)    │
┌───────────┐        │         └───────────┘
│ chunking  │◄───────┘
└───────────┘                  ┌───────────┐
       ▲                       │   util    │
       │                       └───────────┘
┌───────────┐      ┌───────────┐      ┌───────────┐
│   llm     │◄─────│  ollama   │      │  openai   │
│  (trait)  │◄─────│(impl)     │      │  (impl)   │
└───────────┘      └───────────┘      └───────────┘
```

`LlmBackend` is used by ingest, derive, extract, tui, and serve.

## Data Model

### Hierarchy

```
Document (1) ──► Entry (n) ──► Chunk (n)
    │               │              │
    │               │              └── vec_chunks (1:1, via sqlite-vec)
    │               │
    │               └── author, timestamp, heading
    │
    ├── source_title, doctype, merge_strategy, tags, derived_content, document_minhash
    │
    └── claims (n) ──► vec_claims (1:1, via sqlite-vec)
```

### Key Tables

**documents**: Top-level container. One per source (e.g., one Slack channel conversation, one email thread).
- `source_title`: Browser window title or filename
- `doctype`: Matched type (slack, email, markdown, etc.)
- `merge_strategy`: none | positional (for conversations that grow over time)
- `document_minhash`: MinHash signature over full document body, used for near-duplicate detection at ingest

**entries**: Logical segments within a document (messages, paragraphs, sections). Cascades on document deletion.
- `position`: Order within document
- `author`, `timestamp`: For conversations
- `heading_title`, `heading_level`: For structured docs
- `minhash`: Per-entry MinHash signature

**chunks**: Text fragments for search indexing. Entries are split into chunks of ~300 words. Cascades on entry deletion.
- `chunk_index`: Position within entry
- `body`: The text

**chunks_fts**: FTS5 virtual table for full-text search.

**vec_chunks**: sqlite-vec `vec0` virtual table for semantic search. Stores embeddings with cosine distance metric. Created lazily on first `what-was-said embed` with the dimension detected from the embedding model.

**document_tags**: Many-to-many relationship for tagging.

**derived_content**: LLM-generated summaries (brief + detailed) with quality tracking, model provenance, and source hashing for staleness detection.

**claims**: LLM-extracted atomic propositions from documents. Each claim is a self-contained sentence with provenance back to its source document and (optionally) entry. Stores the extraction model and a hash of the prompt text used, enabling incremental re-extraction when the model or prompt changes.
- `content`: The claim text
- `author`: Inherited from the entry when a single author is known
- `model`: Which LLM produced the extraction
- `prompt_hash`: Hash of the prompt text, for staleness detection

**vec_claims**: sqlite-vec `vec0` virtual table for semantic search over claims. Separate from `vec_chunks` because claims and chunks are different semantic objects with different query purposes and re-embedding lifecycles. Created lazily on first `what-was-said embed` after claims exist.

## Module Responsibilities

### main.rs
CLI parsing via clap derive API and command dispatch. Registers the sqlite-vec extension via `sqlite3_auto_extension`. Contains `open_db()` for connection setup and `create_backend()` factory that returns `Box<dyn LlmBackend>` based on `BackendConfig`.

The `--config` flag specifies the config directory (default: `~/.config/what-was-said/`). `BackendConfig` is loaded from `backend.toml` within this directory, with CLI flags (`--backend`, `--ollama`, `--model`, `--embed-model`) as optional overrides. All CLI backend/model flags are `Option<String>` — no hardcoded defaults in clap.

Subcommands: ingest, search, similar, get, dump, stats, embed, derive, extract, serve, browse (default).

Global flags: `--db`, `--config`, `--backend`, `--ollama`, `--model`, `--embed-model`, `--theme`, `--json`.

### llm.rs
`LlmBackend` trait defining the interface for LLM providers:
- `generate(prompt, model, system, format)`: Full generation with optional system prompt and format (e.g., JSON mode)
- `chat(prompt, model)`: Default implementation calling `generate` without system prompt or format
- `embed(text, model)`: Text embedding, returns float vector

The trait requires `Send + Sync` for use in the async serve module.

### ollama.rs
`OllamaClient` implementing `LlmBackend` via the Ollama HTTP API. Constructor takes just `base_url`; the model is passed per-call.

### openai.rs
`OpenAiClient` implementing `LlmBackend` via the OpenAI chat completions and embeddings APIs. Supports two authentication strategies:

- **API key** (`auth = "api_key"`): Reads `OPENAI_API_KEY` from environment. Compatible with standard OpenAI, Azure OpenAI, and other OpenAI-compatible endpoints.
- **OAuth2 client credentials** (`auth = "oauth"`): Reads `OAUTH2_CLIENT_ID` and `OAUTH2_CLIENT_SECRET` from environment. Fetches bearer tokens from a configured token URL with a configured scope. Tokens are cached in a `Mutex<Option<CachedToken>>` (for `Send + Sync` compatibility) and refreshed automatically 60 seconds before expiry.

The primary constructor is `from_config(&OpenAiConfig)`, taking its configuration from `backend.toml`. A convenience `from_env()` constructor remains for the plain API key case.

Translates the `format: Some("json")` parameter to OpenAI's `response_format: {"type": "json_object"}`.

### serve.rs
Axum-based localhost JSON API server. Holds an `AppState` with `Mutex<Connection>` and `Box<dyn LlmBackend>`.

Endpoints:
- `GET /search?q=...&sort=score|date` — FTS5 search, results grouped by document
- `GET /similar?q=...&limit=N` — semantic search (embeds query via backend, then KNN)
- `GET /get/:id` — full document with entries and chunks
- `GET /entries/:doc_id` — entries for a document
- `GET /claims/doc/:doc_id` — claims extracted from a document
- `GET /claims/similar?q=...&limit=N` — semantic search over claims via `vec_claims`
- `GET /stats` — document/entry/chunk/embedding/claim counts
- `GET /derive/status` — derivation progress

Creates a tokio runtime internally so the rest of the binary stays synchronous.

### derive.rs
LLM summary generation. Calls `backend.chat()` with prompts from derive.toml.

Key functions:
- `run()`: Iterates documents needing derivation, generates detailed then brief summaries
- `run_status()`: Reports derivation progress
- `derive_detailed()`: Generates detailed summary, returns body + content length
- `derive_brief()`: Generates brief summary via LLM, or copies detailed directly for short documents (under `short_threshold`)

### extract.rs
LLM claim extraction. Calls `backend.chat()` with an adaptive prompt that handles all document types. Parallel to `derive.rs` structurally.

Key functions:
- `run()`: Selects documents needing extraction (missing claims or stale model/prompt), deletes existing claims, re-extracts, stores results
- `run_status()`: Reports extraction progress (documents with/without claims, total claim count)
- `extract_document()`: Builds prompt from config (rules + optional source-format framing), calls LLM, parses response, inserts claims
- `parse_claims()`: Lenient line parser. Strips bullet markers (`- `, `* `), numbered prefixes (`1. `), bracket labels (`[kind]`), and skips blank lines and preamble. Returns clean claim strings.
- `compute_prompt_hash()`: Hashes the rules text for staleness detection
- `resolve_author()`: If all entries in a document share a single author, inherits it for claims

Staleness detection: each claim stores the `model` and `prompt_hash` that produced it. `get_documents_needing_extraction(model, prompt_hash)` returns documents whose claims don't match the current config, enabling automatic incremental re-extraction when the model or prompt changes.

### config.rs
Loads and parses configuration files from the config directory. Handles doctype detection.

**Doctype matching** (in order):
1. `source_pattern` regex against source title
2. `extension` match against file extension
3. Content sniffing (markdown headers, copilot email format)

Key types:
- `Doctype`: Parsed config entry
- `DoctypeMatch`: Result of detection, includes parser/preprocessor/merge_strategy
- `TagConfig`: Tag hierarchy, default exclusions, color assignments
- `DeriveConfig`: Model selection, prompt tiers, thresholds
- `ExtractConfig`: Model selection, source-format framing paths, rules path, prompt hash computation
- `BackendConfig`: Backend selection, model defaults, OpenAI auth configuration
- `BackendKind`: Enum (`Ollama`, `OpenAi`)
- `OpenAiAuth`: Enum (`ApiKey`, `OAuth`)
- `OpenAiConfig`: Base URL, auth strategy, OAuth token URL and scope

`BackendConfig::load(config_dir)` reads `backend.toml` from the config directory, falling back to defaults (Ollama on localhost) if the file doesn't exist. `DeriveConfig::load(config_dir)` reads `derive.toml` similarly. `ExtractConfig::load(config_dir)` reads `extract.toml` similarly, defaulting to `gemma3:27b` and the compiled-in adaptive prompt.

### ingest.rs
Text parsing, segmentation, and ingestion orchestration. All public functions accept `&dyn LlmBackend` plus an explicit `model: &str` parameter.

**Parsers**:
- `Whole`: Entire file as single entry
- `Markdown`: Split on headings
- `CopilotEmail`: Parse Copilot-formatted email threads
- `Ollama`: LLM-based segmentation (mostly deprecated)
- `Whisper`: Declared but not yet implemented

**External preprocessors**: Python scripts that return JSON. Called via:
```rust
run_preprocessor(script_path, file_path) -> SegmentationResult
```

**Orchestration**:
- `ingest_file()`: Main ingestion logic — reads file, detects doctype, parses, handles merge, detects near-duplicates, stores results
- `ingest_directory()`: Iterates directory, calls ingest_file per file
- `find_overlap()`: Detects overlapping entries for positional merge
- `segment()`: Free function that calls `backend.generate()` with segmentation prompt

**Near-duplicate detection**: After parsing but before inserting, computes a document-level MinHash signature and compares against existing documents within a ±180 day window. If Jaccard similarity ≥ 0.7 (`dup_jaccard_threshold`), the older document is tagged `superseded`. Near-misses (≥ 0.4) are logged to stderr for diagnostic tuning.

### storage/
All SQLite operations. Uses rusqlite directly (no ORM). Integrates sqlite-vec for vector search. Split into submodules, all re-exported from `storage/mod.rs`. Sets `PRAGMA foreign_keys = ON` and `PRAGMA journal_mode=WAL` on initialization for referential integrity and concurrent read access.

Key output types (`GroupedSearchResult`, `ChunkHit`, `SimilarChunk`, `SimilarClaim`, `Claim`, `DumpDocument`, `DumpEntry`, `DocumentContent`, `EntryContent`, `ChunkContent`, `DeriveStatus`) derive `Serialize` for JSON output.

**mod.rs**: Schema initialization (`initialize()`), foreign keys pragma, WAL mode pragma, `migrate()` for schema upgrades, re-exports, tests.

`migrate()` runs on every startup and handles: rebuilding the entries table to add `ON DELETE CASCADE` if missing, adding the `document_minhash` column to documents if missing, migrating the claims table if it has the old `kind` column or lacks `prompt_hash` (drops and recreates the table; re-extract to repopulate), and cleaning up any orphaned entries/chunks with an FTS rebuild.

**documents.rs**: Document/entry/chunk CRUD, list/get/dump, counts, merge helpers.
- `insert_document/entry/chunks`: Write path
- `get_document()`: Read full document with entries and chunks
- `list_documents()`: Browse-mode listing with brief summaries and tags
- `find_documents_by_merge_key()`: Finds candidates for positional merge
- `find_dup_candidates()`: Finds documents within a time window that have MinHash signatures, for near-duplicate comparison

**search.rs**: FTS5 search with grouping, author/date filters pushed into SQL.
- `search()` / `search_filtered()`: FTS5 MATCH with snippet generation
- Result grouping: chunks grouped by document via `GroupedSearchResult`, deduplicated by snippet similarity

**embed.rs**: sqlite-vec integration for both chunks and claims.
- `ensure_vec_table()`: Creates `vec_chunks` vec0 virtual table with detected embedding dimension
- `insert_embedding()`: Writes chunk embedding via zerocopy
- `find_similar_chunks_filtered()`: KNN search via sqlite-vec `MATCH` with cosine distance
- `ensure_vec_claims_table()`: Creates `vec_claims` vec0 virtual table, separate from `vec_chunks`
- `insert_claim_embedding()`: Writes claim embedding
- `find_similar_claims()`: KNN search over claims, joining back to `claims` and `documents` for metadata
- `get_claims_without_embeddings()`: Claims needing embedding
- `count_claims_with/without_embeddings()`: Progress tracking

**tags.rs**: Tag add/remove/list/get operations.

**derive.rs**: Derived content CRUD, derive status, source hash computation for staleness detection.

**claims.rs**: Claim CRUD, extraction status, staleness-aware document selection.
- `insert_claim()`: Write path (document_id, entry_id, author, content, model, prompt_hash)
- `get_claims_for_document()`: All claims for a document, ordered by id
- `delete_claims_for_document()`: Bulk delete before re-extraction
- `get_documents_needing_extraction(model, prompt_hash)`: Returns document ids where no claims match both the current model and prompt hash — handles missing claims, model changes, and prompt changes in a single query

### util.rs
Shared string utilities.

- `strip_source_suffix()`: Removes browser names, URLs from source titles. Used for both merge key matching and TUI group navigation.
- `extract_group_key()`: Derives a grouping key from a source title by stripping suffixes and filtering out generic names. Used for multi-document navigation in the TUI read view.
- `normalize_to_ascii()`: Converts curly quotes, em-dashes, ellipsis to ASCII equivalents. Collapses whitespace (including non-breaking spaces).
- `truncate_str()`: Char-boundary-safe string truncation.
- `strip_fts_markers()`: Removes FTS5 snippet highlight control characters (`\x01`, `\x02`, `\x03`) for clean JSON output.

### chunking.rs
Splits entry text into chunks for indexing.

Strategy: Sliding window of ~300 words with 1/3 stride. Snaps boundaries to sentence ends and paragraph breaks within a max distance of 500 chars; falls back to word boundaries when no suitable boundary is nearby. Entries under 400 words are kept as a single chunk. Remaining words below the split threshold are absorbed into the final chunk to avoid tiny trailing fragments.

Sentence boundaries are detected by terminal punctuation (`.!?` followed by whitespace) and paragraph breaks (`\n\n`, with optional intermediate whitespace). This ensures documents without terminal punctuation (lists, notes, bullet points) still get chunked at natural paragraph boundaries rather than producing oversized single chunks.

### tui/
Ratatui-based terminal UI. Split into submodules with a shared `App` struct in `mod.rs`. Each mode has a key handler and draw function; the event loop dispatches based on `app.mode`.

**mod.rs**: App struct (all state), enums (Mode, SearchMode, SearchField, SummaryType), shared methods (load_documents, filtered_documents, load_document_for_reading, navigate_group, etc.), `run()`/`run_app()` event loop, `draw()` dispatcher, `entry_chunk_count()`/`document_chunk_count()` helpers. `SearchConfig` holds a `&dyn LlmBackend` reference and `embed_model` string.

**theme.rs**: Theme system. `Theme` struct with 19 semantic color slots (background, text hierarchy, borders, highlights, etc.), hex (`#RRGGBB`) and named color parsing, TOML loading. Five built-in themes compiled via `include_str!` (dracula, gruvbox, nord, solarized, light). Resolution order: builtin name → absolute path → config dir themes folder → fallback to dracula.

**browse.rs**: Browse mode — document list with sorting, filtering, tag color markers, brief summary preview. Centered vertical scrolling.

**read.rs**: Read mode — view document content, navigate chunks, yank to clipboard, group navigation. Skips empty-body entries during chunk navigation.

**search.rs**: Search mode — FTS5 or semantic search (semantic by default) with author/date filters, backtick mode toggle, search execution. Uses `search_config.backend.embed()` for semantic queries.

**tags.rs**: TagEdit and TagFilter modes — add/remove tags, filter document list by tag.

**summary.rs**: SummaryView mode — popup for viewing/toggling brief/detailed summaries, copy, mark bad.

**render.rs**: Shared rendering — status bar with key binding highlighting, markdown line/inline parsing, table alignment, snippet parsing with match highlighting, centered rect utility.

### types.rs
Shared type definitions: `DocumentId`, `EntryId`, `MediaId`, `SegmentedEntry`, `SegmentationResult`, `MergeStrategy`, `MediaItem`, `MediaType`, `MinHashSignature`.

### minhash.rs
MinHash signatures for near-duplicate detection. Tokenizes text into 3-word shingles (sliding windows of 3 consecutive words) for order-sensitive fingerprinting. Texts shorter than 3 words fall back to individual token hashing.

Key functions:
- `minhash()`: Compute a MinHash signature for arbitrary text
- `minhash_document()`: Compute a document-level signature by concatenating all entry bodies
- `minhash_with_context()`: Compute with optional surrounding entry text for contextual hashing
- `jaccard()`: Estimate Jaccard similarity between two signatures
- `is_short_entry()`: Check if text is below 50 chars (used for short-entry handling)

### markdown.rs
Markdown-specific parsing (heading extraction, section splitting). Tracks fenced code block state (```` ``` ```` and `~~~` delimiters) so that `#` comment lines inside code blocks are not misinterpreted as headings.

### whisper.rs
VTT transcript parsing for Whisper output. Parses segments and converts to `MediaItem` list. Not yet wired into the ingestion pipeline.

## Configuration

### config.toml

```toml
[[doctype]]
name = "slack"
source_pattern = "(Channel|DM).*Slack"
parser = "whole"
merge_strategy = "positional"
preprocessor = "~/.config/what-was-said/parsers/slack_parser.py"
skip = false
merge_consecutive_same_author = true
cleanup_patterns = ["^\\s*:\\w+:\\s*$"]
```

**Fields**:
- `name`: Identifier for this doctype
- `source_pattern`: Regex matched against source title
- `extension`: Alternative match by file extension
- `parser`: whole | markdown | whisper | copilot_email | ollama
- `merge_strategy`: none | positional
- `preprocessor`: Path to external parser script (~ expanded)
- `skip`: If true, files matching this doctype are skipped
- `cleanup_patterns`: Regexes for lines to remove
- `merge_consecutive_same_author`: Combine adjacent same-author entries
- `prompt`: Custom prompt for ollama parser

### backend.toml

```toml
backend = "openai"
ollama_url = "http://localhost:11434"
model = "gpt-4.1-mini"
embed_model = "text-embedding-3-small"

[openai]
base_url = "https://api.openai.com/v1"
auth = "oauth"
oauth_token_url = "https://login.microsoftonline.com/.../oauth2/v2.0/token"
oauth_scope = "api://.../.default"
```

**Fields**:
- `backend`: `ollama` (default) or `openai`
- `ollama_url`: Ollama endpoint (default: `http://localhost:11434`)
- `model`: Default model for generation (overridden by `--model` CLI flag)
- `embed_model`: Default model for embeddings (overridden by `--embed-model` CLI flag)

**`[openai]` section**:
- `base_url`: API endpoint (default: `https://api.openai.com/v1`)
- `auth`: `api_key` (default) or `oauth`
- `oauth_token_url`: OAuth2 token endpoint (required when `auth = "oauth"`)
- `oauth_scope`: OAuth2 scope (required when `auth = "oauth"`)

Environment variables:
- `OPENAI_API_KEY`: Required when `auth = "api_key"`
- `OAUTH2_CLIENT_ID`, `OAUTH2_CLIENT_SECRET`: Required when `auth = "oauth"`

If `backend.toml` doesn't exist, defaults to Ollama on localhost with no file needed.

### tags.toml

```toml
[defaults]
exclude = ["junk", "archived"]

[includes]
project-x = ["x-frontend", "x-backend", "x-infra"]

[colors]
research = "cyan"
project = "green"
reference = "blue"
```

- `[defaults].exclude`: Tags filtered out by default (override with `--include-all`)
- `[includes]`: Parent tags that match documents tagged with any child
- `[colors]`: Tag color for browse view markers (named colors or hex `#RRGGBB`)

### derive.toml

```toml
detailed_model = "qwen2.5:32b"
brief_model = "qwen2.5:32b"
prompt_version = "v1"
short_threshold = 1200
medium_threshold = 3500

[prompts]
default = "~/.config/what-was-said/prompts/detailed.txt"
brief = "~/.config/what-was-said/prompts/brief.txt"
```

Prompt tier is selected by document content length: short (<1200 chars) gets a terse 1-2 sentence prompt, medium (<3500) gets a proportional summary, long gets structured section-by-section analysis. For short documents, the brief summary is copied directly from the detailed output without an additional LLM call.

### extract.toml

```toml
model = "gemma3:27b"

[framings]
email = "~/.config/what-was-said/prompts/extract_framing_conversation.txt"
slack = "~/.config/what-was-said/prompts/extract_framing_conversation.txt"
whisper = "~/.config/what-was-said/prompts/extract_framing_voice.txt"
```

**Fields**:
- `model`: LLM for claim extraction (default: `gemma3:27b`). Independent of the global `model` in `backend.toml`.
- `rules`: Optional path to a custom extraction rules prompt. If absent, the compiled-in adaptive prompt is used.
- `framings`: Optional per-doctype source-format framings. These are structural hints (e.g. "attribute to speaker"), not domain-specific instructions — the adaptive prompt handles domain adaptation. Keyed by doctype name from `config.toml`.

If `extract.toml` doesn't exist, defaults to `gemma3:27b` with the compiled-in prompt and no framings.

## External Preprocessors

Python scripts that parse format-specific content.

**Contract**:
- Input: File path as CLI argument
- Output: JSON to stdout

```json
{
  "entries": [
    {
      "body": "Message text",
      "author": "Jane Smith",
      "timestamp": "2024-01-15T10:30:00",
      "heading_title": null,
      "heading_level": null
    }
  ]
}
```

Only `body` is required. Timestamps should be ISO 8601, normalized to UTC.

**Invocation**: `python3 script.py /path/to/file.txt`

See `examples/parsers/` for working email and Slack preprocessor scripts, and `examples/inbox/` for sample input documents.

## Ingestion Flow

```
1. Read file
2. Extract "# source: ..." header line → source_title
3. Match doctype (config.detect_with_content)
4. If skip=true, return early
5. If preprocessor defined, call it; else use built-in parser
6. Apply cleanup_patterns
7. If merge_consecutive_same_author, combine entries
8. Normalize source_title to ASCII, compute merge key via strip_source_suffix
9. If merge_strategy=positional:
   a. Find existing docs with same merge_key
   b. Check each for overlapping entries (≥150 chars consecutive match)
   c. If overlap found, append new entries to existing doc
   d. Else fall through to step 10
10. Compute document-level MinHash signature
11. Query existing docs within ±180 day window for near-duplicates
12. If Jaccard similarity ≥ 0.7, mark older doc as superseded
13. Insert document/entries/chunks
14. Index in FTS5 (via trigger)
```

## Search Flow

### FTS5
```
1. Convert query to prefix search ("foo bar" → "foo* bar*")
2. Build SQL with author/date filters in WHERE clause
3. Execute FTS5 MATCH query with snippet()
4. Group results by document_id
5. Deduplicate similar snippets within document
6. Sort by best rank (or date)
```

### Semantic
```
1. Embed query text via LlmBackend (default: qwen3-embedding:8b via Ollama)
2. Run KNN query against vec_chunks using sqlite-vec MATCH
3. Join results back to chunks/entries for metadata
4. Filter by author/date if specified
5. Group by document_id
6. Convert to GroupedSearchResult format
```

## Embeddings

Chunk embeddings are stored in `vec_chunks`, claim embeddings in `vec_claims` — both sqlite-vec `vec0` virtual tables with cosine distance metric. Tables are created lazily on the first `what-was-said embed` run, with the embedding dimension detected from the model's response.

**Generate**: `what-was-said embed [--limit N] [--embed-model MODEL]`

The embed command processes chunks first, then claims, with separate progress reporting for each.

**Default model**: qwen3-embedding:8b via Ollama

**Search**: KNN via sqlite-vec's `WHERE embedding MATCH ? AND k = ?` syntax. Sublinear in collection size. Chunk search and claim search are independent queries against their respective tables.

## Key Design Decisions

1. **Chunk-level search, document-level display**: Search indexes chunks for precision, but results are grouped by document for context.

2. **Positional merge**: Conversation documents (Slack, email) grow over time. Overlap detection allows appending new messages without duplicating.

3. **External preprocessors**: Format-specific parsing is delegated to Python scripts. Easier to iterate on parsing logic without recompiling.

4. **No ORM**: Direct rusqlite for simplicity and control. Schema is simple enough that an ORM adds more complexity than it removes.

5. **sqlite-vec for embeddings**: KNN search via vec0 virtual tables replaces brute-force cosine similarity. Scales to large collections without loading all embeddings into memory.

6. **Short-doc brief optimization**: Documents under the short threshold get their brief summary copied from the detailed output, saving an LLM round-trip on content that's already 1-2 sentences.

7. **LLM backend abstraction**: `LlmBackend` trait with `OllamaClient` and `OpenAiClient` implementations. Backend selection and model defaults are configured in `backend.toml`, with CLI flags as overrides. `OpenAiClient` supports both static API keys and OAuth2 client credentials with automatic token refresh, enabling use on machines with institutional OpenAI endpoints.

8. **WAL mode**: SQLite journal_mode=WAL set on initialization. Enables concurrent readers during serve mode without blocking on writes.

9. **JSON API via serve**: Localhost axum server holding the DB connection open, exposing storage functions as JSON endpoints. Designed as the agent-facing interface, avoiding per-call process spawn overhead.

10. **Foreign key enforcement**: `PRAGMA foreign_keys = ON` set on every connection. All parent-child relationships use `ON DELETE CASCADE`. A migration system in `initialize()` detects old schemas and rebuilds tables as needed, preventing orphaned rows.

11. **Near-duplicate detection via MinHash**: Documents are fingerprinted at ingest using 3-word shingle MinHash signatures. New documents are compared against existing docs within a configurable time window (default ±180 days). Near-duplicates are ingested normally but the older version is tagged `superseded`, preserving both versions while flagging staleness.

12. **Themeable TUI via semantic color slots**: All TUI colors are driven by a `Theme` struct with named semantic slots (background, text, text_muted, highlight_bg, heading, code, etc.) rather than hardcoded terminal colors. Themes are TOML files with hex color values. Five built-in themes are compiled into the binary; custom themes can be loaded from the config directory or an absolute path. Tag colors remain user-configured independently of the theme.

13. **Paragraph-aware chunking**: Chunk boundary detection recognizes both sentence-ending punctuation (`.!?`) and paragraph breaks (`\n\n`) as snap points. This prevents documents without terminal punctuation (bullet lists, notes, code-heavy content) from producing oversized single chunks.

14. **Config-directory convention**: All configuration files (`config.toml`, `backend.toml`, `tags.toml`, `derive.toml`, `extract.toml`, themes) live in a single config directory (default `~/.config/what-was-said/`), overridable with `--config`. This allows multiple configurations (e.g. personal vs. work) by pointing at different directories.

15. **Claim extraction with adaptive prompt**: A single prompt handles all document types. The LLM adapts its extraction to whatever is substantive in the text rather than following per-domain rules. Source-format framings (email attribution, voice memo context) are retained as structural hints only. No stored classification — testing showed models are weak at it, and the claim text itself is the representation.

16. **Incremental re-extraction via model + prompt_hash**: Each claim stores the model and a hash of the prompt rules that produced it. `what-was-said extract` compares against the current config and re-extracts only stale documents. This means changing the configured model or editing the prompt file triggers automatic re-extraction without `--force`.

17. **Separate vec_claims table**: Claims and chunks are different semantic objects — claims are atomic propositions, chunks are text fragments. They have different query purposes (what was said vs. where is it) and different re-embedding lifecycles (re-extract claims without re-chunking). Mixing them in one vector table would couple these concerns.

## Adding a New CLI Command

1. Add a variant to the `Command` enum in main.rs with clap attributes
2. Implement logic in the appropriate module (or a new one)
3. Add a match arm in `main()`
4. Add the module to `lib.rs` if new

## Adding a New LLM Backend

1. Create a new module (e.g., `anthropic.rs`)
2. Implement `LlmBackend` for your client struct
3. Add a variant to `BackendKind` in config.rs and handle it in `BackendConfig` deserialization
4. Add a case to `create_backend()` in main.rs
5. Add the module to `lib.rs`

## Common Maintenance Tasks

**Reset database**: Delete `~/.local/share/what-was-said/what-was-said.db`

**Re-embed everything**: `DROP TABLE vec_chunks; DROP TABLE IF EXISTS vec_claims;` in sqlite3, then `what-was-said embed`

**Re-extract all claims**: `what-was-said extract --force` then `what-was-said embed`

**Debug ingestion**: Run with file directly, check stderr output. Near-dup matches and near-misses are logged to stderr.

**Profile search**: Add timing around `storage::search()` or `find_similar_chunks()`

**Run tests**: `cargo test` (all tests use in-memory SQLite, no network or filesystem dependencies)
