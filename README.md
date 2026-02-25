# PDF Indexer

Extract tables from PDF documents once, query them forever. Built for data science workflows where you have a stack of PDFs full of tabular data and need to work with it programmatically.

## Why This Exists

You just imported 15 government budget PDFs into your project. They contain hundreds of financial tables spread across 1,200 pages. Every time you (or an AI assistant) need a number, someone has to open the PDF, find the right page, locate the table, and read the values.

PDF Indexer fixes this. It reads every PDF once, extracts all the tables, classifies the data types (currency, percentages, fiscal years), and stores everything in a local SQLite database. After that, you never open the PDFs again.

## Getting Started

### Step 1: Install

Clone this repo and install in editable mode:

```bash
git clone https://github.com/casruta/PDF-Indexer-.git
cd PDF-Indexer-
pip install -e ".[mcp]"
```

Requires **Python 3.12+**. The `[mcp]` extra installs the Claude Code integration. If you only want the CLI, plain `pip install -e .` works.

### Step 2: Put your PDFs in a folder

Organize your PDFs however you like. The indexer will scan the entire directory tree:

```
my-project/
  data/
    pdfs/
      2020-Budget.pdf
      2021-Budget.pdf
      2022-Budget.pdf
      Annual-Report-2023.pdf
      ...
```

### Step 3: Index

Point the indexer at your PDF folder:

```bash
pdf-index data/pdfs
```

You'll see output like:

```
Scanning: C:\Users\you\my-project\data\pdfs
Found 15 PDF file(s)
  Indexing: 2020-Budget.pdf
    24 pages, 13 tables
  Indexing: 2021-Budget.pdf
    24 pages, 14 tables
  ...

Done in 498.2s
  Indexed: 15 | Skipped: 0
  Database: 15 docs, 1187 pages, 858 tables, 68282 cells
```

**This only happens once.** Re-running the same command takes <1 second because the indexer hashes each file and skips anything unchanged:

```
Done in 0.1s
  Indexed: 0 | Skipped: 15
```

### Step 4: Explore your data

**List what was indexed:**

```bash
pdf-index data/pdfs --list
```

```
Document                                       Pages  Tables   Cells
----------------------------------------------------------------------
2020-Budget.pdf                                   24      13    1311
2021-Budget.pdf                                   24      14    1533
2022-Budget.pdf                                   24      16    1751
...
```

**Search for tables by keyword:**

```bash
pdf-index data/pdfs --search "Education"
```

This returns the actual table data — headers, rows, cell values — along with the document name and page number so you can cite it.

### Step 5: Connect to Claude Code (recommended)

This is where it gets powerful. Generate the MCP config:

```bash
pdf-index data/pdfs --generate-mcp-config
```

Save the output to `.mcp.json` in your project root. Now when you open Claude Code in that project, it has 8 tools to query your PDF data directly:

| Tool | What to use it for |
|---|---|
| `search_tables("revenue")` | Find all tables containing "revenue" across every PDF |
| `search_tables("", data_type="currency", min_value=1000000)` | Find tables with dollar amounts over $1M |
| `get_table(42)` | Get the full contents of table #42 (from search results) |
| `get_document_summary("2022-Budget")` | How many pages, tables, and cells are in a specific PDF |
| `get_page_content("2022-Budget", 5)` | See all text and tables from page 5 |
| `list_documents()` | Overview of everything that's been indexed |
| `query_data("SELECT ...")` | Run custom SQL against the extracted data |
| `add_observation("spending grew 5%")` | Save a research note for later |

**Claude never reads the PDF files.** It queries the pre-extracted database, which means:
- No wasted tokens re-reading documents
- No hallucinated data — every value comes from the actual table cells
- Instant responses (milliseconds, not minutes)
- Every answer includes a citation (table ID, page number, document name)

### Step 6: Start analyzing

Ask Claude questions about your data:

> "What was the education spending in each year?"

Claude calls `search_tables("education", data_type="currency")` and gets back the exact tables with the real numbers.

> "Compare health vs education spending from 2018 to 2024"

Claude calls `query_data()` with a SQL query that joins across documents and returns the comparison.

> "Show me everything on page 12 of the 2022 budget"

Claude calls `get_page_content("2022", 12)` and gets the raw text plus any tables on that page.

## Session Tracking

When doing extended research, you can track your work:

```bash
pdf-index data/pdfs --session-start
pdf-index data/pdfs --observe "Education spending increased 12% from 2020-2024"
pdf-index data/pdfs --observe "Health is consistently the largest budget item"
pdf-index data/pdfs --observations       # view your notes
pdf-index data/pdfs --session-end
```

Claude Code can also record observations via the `add_observation` tool, and retrieve them later with `get_session_notes`. Observations persist across conversations.

## How It Works Under the Hood

1. **Scan** — Walks the directory for `.pdf` files
2. **Hash** — SHA-256 of each file. If the hash matches what's stored, skip it entirely
3. **Extract** — pdfplumber detects tables automatically. Falls back to word-coordinate column detection for complex layouts
4. **Classify** — Each cell is typed: `currency` ($1,234), `percent` (12.5%), `fiscal_year` (2020-21), `number`, or `text`. Currency and numeric values are parsed to floats for filtering
5. **Store** — Everything goes into `.pdfindex/index.db` (SQLite). Documents, pages, tables, and individual cells with their typed values
6. **Query** — CLI search, 8 MCP tools, or direct SQL against the database

## Database Schema

The SQLite database has these tables you can query with `query_data`:

- **documents** — `id`, `file_path`, `title`, `author`, `page_count`, `content_hash`
- **pages** — `id`, `document_id`, `page_number`, `raw_text`, `word_count`
- **tables** — `id`, `page_id`, `table_index`, `row_count`, `col_count`, `headers_json`, `table_type`
- **table_cells** — `id`, `table_id`, `row_idx`, `col_idx`, `value`, `data_type`, `numeric_value`

Example SQL:

```sql
SELECT d.title, SUM(t.row_count) as total_rows, COUNT(t.id) as tables
FROM documents d
JOIN pages p ON p.document_id = d.id
JOIN tables t ON t.page_id = p.id
GROUP BY d.id
ORDER BY tables DESC
```

## CLI Reference

```
pdf-index <path>                        Index all PDFs in directory
pdf-index <path> --list                 List indexed documents
pdf-index <path> --search "term"        Search tables by keyword
pdf-index <path> --serve-mcp            Start MCP server
pdf-index <path> --generate-mcp-config  Print .mcp.json config
pdf-index <path> --session-start        Start a research session
pdf-index <path> --session-end          End the active session
pdf-index <path> --observe "note"       Add a research observation
pdf-index <path> --observations         Show session observations
pdf-index <path> -v                     Verbose output
pdf-index <path> --exclude "*.draft"    Exclude files by pattern
pdf-index <path> --min-rows 3           Minimum rows for table detection
pdf-index <path> --min-cols 2           Minimum columns for table detection
```

## Windows Notes

Run Python with `-X utf8` to avoid encoding issues:

```bash
python -X utf8 -m pdf_indexer /path/to/pdfs
```

The MCP config generated by `--generate-mcp-config` already includes this flag.

## Requirements

- Python 3.12+
- pdfplumber (installed automatically)
- mcp >= 1.0.0 (optional, for Claude Code integration)
