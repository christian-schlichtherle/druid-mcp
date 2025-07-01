# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an MCP (Model Context Protocol) server that provides read-only access to one or more Apache Druid clusters. The server uses FastMCP framework to expose comprehensive Druid functionality through standardized MCP tools, resources, and prompts.

## Development Commands

### Install Dependencies
```bash
uv install
```

### Run MCP Server
```bash
# Development mode with auto-reload
mcp dev main.py

# Production mode
mcp run main.py

# Install for Claude Desktop
mcp install main.py
```

## Architecture

### Core Structure
- **main.py**: Complete MCP server implementation (1000+ lines)
  - FastMCP server with httpx async client
  - Custom DruidError exception handling
  - Schema caching with 5-minute TTL
  - Date utilities for default time intervals

### MCP Components

#### Resources
- `druid://{cluster}/v2/datasources`: List datasources in a cluster
- `druid://{cluster}/v2/datasources/{name}`: Datasource schema (dimensions, metrics)
- `druid://{cluster}/coordinator/v1/datasources/{name}`: Coordinator datasource info
- `druid://{cluster}/coordinator/v1/metadata/datasources/{name}/segments`: Segment info
- `druid://{cluster}/overlord/v1/tasks`: Task list
- `druid://{cluster}/overlord/v1/supervisors`: Supervisor list
- `druid://{cluster}/status/health`: Health status

#### Tools (16 total)

**IMPORTANT: Cluster Parameter Change**
All tools (except `list_clusters`) now require an explicit `cluster` parameter as the first argument. This enables efficient multi-cluster operations without state management.

Example:
```python
# Explicit cluster specification:
execute_sql_query("fret-prod", "SELECT COUNT(*) FROM wikipedia")
```

**Query Execution:**
- `execute_sql_query(cluster, query, context)`: SQL queries with context
- `execute_native_query(cluster, query)`: Native JSON queries (timeseries, topN, groupBy, etc.)

**Exploration:**
- `list_datasources(cluster, include_details)`, `get_datasource_schema(cluster, datasource)`: Datasource management
- `list_supervisors(cluster, include_state)`, `get_supervisor_status(cluster, supervisor_id)`: Ingestion monitoring
- `list_tasks(cluster, ...)`, `get_task_status(cluster, task_id)`: Task management
- `list_segments(cluster, datasource, full)`, `get_segments_info(cluster, datasource)`: Segment analysis
- `get_cluster_status(cluster)`, `list_services(cluster, service_type)`: Cluster health
- `list_lookups(cluster, tier)`, `get_lookup(cluster, lookup_id, tier)`, `get_lookup_status(cluster, ...)`: Lookup management

**Multi-Cluster:**
- `list_clusters()`: Cluster management (no cluster parameter needed)

#### Prompts (5 total)
All prompts include smart defaults (midnight same day last month → now):
- `analyze_time_range`: Data analysis within time periods
- `explore_datasource`: Datasource structure exploration
- `monitor_ingestion`: Ingestion health monitoring
- `compare_periods`: Period-over-period analysis
- `data_quality_check`: Comprehensive data quality checks

### Key Implementation Details

**Error Handling:**
- Custom `DruidError` class parsing Druid API error responses
- Consistent try/catch patterns across all HTTP calls

**Time Handling:**
- `get_default_time_interval()` function with edge case handling
- Uses `python-dateutil` for robust date arithmetic
- Handles month boundaries, leap years, year transitions

**Caching:**
- Schema cache with 5-minute TTL to reduce API calls
- Resource endpoints provide instant cached access
- Cache keys are cluster-aware (e.g., "cluster:path")

**Multi-Cluster Architecture:**
- Separate HTTP clients per cluster for proper connection management
- Explicit cluster parameter on all tools (except cluster management tools)
- Resource URIs include cluster identifier for explicit targeting
- Enables parallel operations across multiple clusters without state changes

## Environment Configuration

- `DRUID_CLUSTER_URLS`: Whitespace-separated cluster definitions as key=value pairs (default: "localhost=http://localhost:8088")

## Dependencies

- Python 3.11+
- httpx: Async HTTP client for Druid API communication
- mcp[cli]: Model Context Protocol SDK with CLI tools
- python-dateutil: Robust date arithmetic for time intervals

## Implementation Patterns

When extending functionality:
1. Use `@mcp.tool()` decorator with comprehensive docstrings
2. **Always include `cluster: str` as the first parameter** (except for `list_clusters` tool)
3. Implement consistent error handling with DruidError
4. Follow async/await patterns for all HTTP calls
5. Add optional parameters with sensible defaults
6. Maintain read-only access constraints
7. Pass cluster parameter explicitly to `_make_request()`

## Testing and Quality Assurance

No formal test suite exists. Manual testing approach:
1. Start MCP server: `mcp dev main.py`
2. Test with Claude Desktop or other MCP client
3. Verify tool responses and resource access
4. Test multi-cluster operations if configured

## Code Writing Guidelines

- When editing code, do not comment on the obvious  
- When committing code, do not includes references to yourself, Claude
- Never mention yourself in git commit messages

# important-instruction-reminders

Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.