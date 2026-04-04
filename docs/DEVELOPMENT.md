# Cathedrals Development Guide

Personal knowledge base for clipped documents with full-text and semantic search.

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                          main.rs                              │
│                    (CLI, orchestration)                       │
└──────────────────────────────────────────────────────────────┘
       │              │              │              │
       ▼              ▼              ▼              ▼
┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐
│  ingest   │  │  storage  │  │   derive  │  │    tui    │
│ (parsing) │  │ (sqlite)  │  │  (LLM)    │  │ (ratatui) │
└───────────┘  └───────────┘  └───────────┘  └───────────┘
       │              │
       ▼              │
┌───────────┐         │         ┌───────────┐
│ chunking  │◄────────┘         │   util    │
└───────────┘                   └───────────┘
```

## Data Model

### Hierarchy

```
Document (1) ──► Entry (n) ──► Chunk (n)
    │               │              │
    │               │              └── vec_chunks (1:1, via sqlite-vec)
    │               │
    │               └── author, timestamp, heading
    │
    └── source_title, doctype, merge_strategy, tags, derived_content
```

### Key Tables

**documents**: Top-level container. One per source (e.g., one Slack channel conversation, one email thread).
- `source_title`: Browser window title or filename
- `doctype`: Matched type (slack, email, markdown, etc.)
- `merge_strategy`: none | positional (for conversations that grow over time)

**entries**: Logical segments within a document (messages, paragraphs, sections).
- `position`: Order within document
- `author`, `timestamp`: For conversations
- `heading_title`, `heading_level`: For structured docs

**chunks**: Text fragments for search indexing. Entries are split into chunks of ~300 words.
- `chunk_index`: Position within entry
- `body`: The text

**chunks_fts**: FTS5 virtual table for full-text search.

**vec_chunks**: sqlite-vec `vec0` virtual table for semantic search. Stores embeddings with cosine distance metric. Created lazily on first `cathedrals embed` with the dimension detected from the embedding model.

**document_tags**: Many-to-many relationship for tagging.

**derived_content**: LLM-generated summaries (brief + detailed) with quality tracking, model provenance, and source hashing for staleness detection.

## Module Responsibilities

### main.rs
CLI parsing and command dispatch. Registers the sqlite-vec extension via `sqlite3_auto_extension`. Orchestrates ingestion flow:
1. Read file, extract `# source:` header
2. Match doctype via config
3. Call appropriate parser/preprocessor
4. Handle merge logic for positional documents
5. Store results

Key functions:
- `ingest_file()`: Main ingestion logic
- `find_overlap()`: Detects overlapping entries for merge
- `open_db()`: Opens connection with sqlite-vec registered

### derive.rs
LLM summary generation. Extracted from main.rs for clarity.

Key functions:
- `run()`: Iterates documents needing derivation, generates detailed then brief summaries
- `run_status()`: Reports derivation progress
- `derive_detailed()`: Generates detailed summary, returns body + content length
- `derive_brief()`: Generates brief summary via LLM, or copies detailed directly for short documents (under `short_threshold`)

### config.rs
Loads and parses `config.toml`, `tags.toml`, and `derive.toml`. Handles doctype detection.

**Doctype matching** (in order):
1. `source_pattern` regex against source title
2. `extension` match against file extension
3. Content sniffing (markdown headers, copilot email format)

Key types:
- `Doctype`: Parsed config entry
- `DoctypeMatch`: Result of detection, includes parser/preprocessor/merge_strategy
- `TagConfig`: Tag hierarchy, default exclusions, color assignments
- `DeriveConfig`: Model selection, prompt tiers, thresholds

### ingest.rs
Text parsing and segmentation.

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

**OllamaClient**: HTTP client for Ollama API (chat completions and embeddings).

### storage.rs
All SQLite operations. Uses rusqlite directly (no ORM). Integrates sqlite-vec for vector search.

Key functions:
- `initialize()`: Creates tables, FTS5, indexes
- `insert_document/entry/chunks`: Write path
- `get_document()`: Read full document with entries and chunks
- `search()` / `search_filtered()`: FTS5 search with grouping, author/date filters pushed into SQL
- `ensure_vec_table()`: Creates vec0 virtual table with detected embedding dimension
- `insert_embedding()`: Writes embedding to vec_chunks via zerocopy
- `find_similar_chunks_filtered()`: KNN search via sqlite-vec `MATCH` with cosine distance

**Search result grouping**: Both FTS5 and semantic search return `GroupedSearchResult` — chunks grouped by document, sorted by best score.

### util.rs
Shared string utilities.

- `strip_source_suffix()`: Removes browser names, URLs from source titles. Used for both merge key matching and TUI group navigation.
- `normalize_to_ascii()`: Converts curly quotes, em-dashes, ellipsis to ASCII equivalents.
- `truncate_str()`: Char-boundary-safe string truncation.

### chunking.rs
Splits entry text into chunks for indexing.

Strategy: Sliding window of ~300 words with 1/3 stride. Snaps boundaries to sentence ends. Falls back to word boundaries for very long sentences. Entries under 400 words are kept as a single chunk.

### tui.rs
Ratatui-based terminal UI.

**Modes**:
- `Browse`: Document list with sorting, filtering, tag color markers, brief summary preview
- `Read`: View document content, navigate chunks
- `Search`: FTS5 or semantic search with author/date filters
- `TagEdit`: Add/remove tags on current document
- `TagFilter`: Filter document list by tag
- `SummaryView`: Popup for viewing/toggling brief/detailed summaries

**Search modes** (toggled with F2):
- `Fts5`: Keyword search via SQLite FTS5
- `Semantic`: KNN vector similarity via sqlite-vec

### types.rs
Shared type definitions: `DocumentId`, `EntryId`, `MediaId`, `SegmentedEntry`, `SegmentationResult`, `MergeStrategy`, `MediaItem`, `MediaType`, `MinHashSignature`.

### minhash.rs
MinHash signatures for near-duplicate detection. Used during ingestion to compute per-entry hashes stored in the entries table.

### markdown.rs
Markdown-specific parsing (heading extraction, section splitting).

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
preprocessor = "~/.config/cathedrals/parsers/slack_parser.py"
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
- `[colors]`: Tag color for browse view markers

### derive.toml

```toml
detailed_model = "qwen2.5:32b"
brief_model = "qwen2.5:32b"
prompt_version = "v1"
short_threshold = 1200
medium_threshold = 3500

[prompts]
default = "~/.config/cathedrals/prompts/detailed.txt"
brief = "~/.config/cathedrals/prompts/brief.txt"
```

Prompt tier is selected by document content length: short (<1200 chars) gets a terse 1-2 sentence prompt, medium (<3500) gets a proportional summary, long gets structured section-by-section analysis. For short documents, the brief summary is copied directly from the detailed output without an additional LLM call.

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
   d. Else create new document
10. Insert document/entries/chunks
11. Index in FTS5 (via trigger)
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
1. Embed query text via Ollama (qwen3-embedding:8b)
2. Run KNN query against vec_chunks using sqlite-vec MATCH
3. Join results back to chunks/entries for metadata
4. Filter by author/date if specified
5. Group by document_id
6. Convert to GroupedSearchResult format
```

## Embeddings

Stored in `vec_chunks`, a sqlite-vec `vec0` virtual table with cosine distance metric. The table is created lazily on the first `cathedrals embed` run, with the embedding dimension detected from the model's response.

**Generate**: `cathedrals embed [--limit N] [--embed-model MODEL]`

**Default model**: qwen3-embedding:8b via Ollama

**Search**: KNN via sqlite-vec's `WHERE embedding MATCH ? AND k = ?` syntax. Sublinear in collection size.

## Key Design Decisions

1. **Chunk-level search, document-level display**: Search indexes chunks for precision, but results are grouped by document for context.

2. **Positional merge**: Conversation documents (Slack, email) grow over time. Overlap detection allows appending new messages without duplicating.

3. **External preprocessors**: Format-specific parsing is delegated to Python scripts. Easier to iterate on parsing logic without recompiling.

4. **No ORM**: Direct rusqlite for simplicity and control. Schema is simple enough that an ORM adds more complexity than it removes.

5. **sqlite-vec for embeddings**: KNN search via vec0 virtual tables replaces brute-force cosine similarity. Scales to large collections without loading all embeddings into memory.

6. **Short-doc brief optimization**: Documents under the short threshold get their brief summary copied from the detailed output, saving an LLM round-trip on content that's already 1-2 sentences.

## Adding a New Parser

1. Define doctype in config.toml with `parser = "whole"` and `preprocessor = "path/to/script.py"`

2. Write Python script that:
   - Reads file path from `sys.argv[1]`
   - Parses content into entries
   - Outputs JSON to stdout

3. Test: `cathedrals ingest path/to/test/file.txt`

## Adding a New CLI Command

1. Add case to `match positional.first()` in main.rs
2. Implement logic (often calling storage functions, or in a new module)
3. Update `print_usage()`

## Common Maintenance Tasks

**Reset database**: Delete `~/.local/share/cathedrals/cathedrals.db`

**Re-embed everything**: `DROP TABLE vec_chunks;` in sqlite3, then `cathedrals embed`

**Debug ingestion**: Run with file directly, check stderr output

**Profile search**: Add timing around `storage::search()` or `find_similar_chunks()`

**Run tests**: `cargo test` (all tests use in-memory SQLite, no network or filesystem dependencies)
