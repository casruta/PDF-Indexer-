# PDF Indexer — Claude Code Instructions

## PDF Data Access Rules

When the `pdf-indexer` MCP server is available, follow these rules strictly:

1. **NEVER read PDF files directly.** Always use the pdf-indexer MCP tools to access PDF data. Do not use the Read tool on any `.pdf` file.

2. **For any data question, call `search_tables` first** to find the relevant table. This returns exact extracted data — no guessing, no hallucination.

3. **Only call `get_page_content` if the user explicitly asks for surrounding context** beyond what the table provides. Tables are the primary data source.

4. **Never guess or interpolate values.** Only report what the table cells contain. If a value is not in the index, say so.

5. **Always cite the source** when presenting financial data: include the table ID, page number, and document name.

6. **Use `query_data` for aggregations.** When the user asks for comparisons across years or documents, write a SQL SELECT query rather than manually reading multiple tables.

7. **Use `add_observation` to record findings.** When you discover something notable in the data, record it as an observation for future reference.

## Workflow

```
User asks a question
  → search_tables(query, filters)
  → Present the table data with citation
  → If user wants more context → get_page_content(doc, page)
  → If user wants cross-document comparison → query_data(SQL)
  → Record notable findings → add_observation(content)
```

## Git Commits
Never include a "Co-Authored-By" trailer in commit messages.
