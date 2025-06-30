# Druid MCP

This repository provides an MCP server for read-only access to one or more Apache Druid clusters.
This is particularly useful for ad-hoc data analysis and comparison across Druid clusters.
For example, if you have multiple environments, each with their own Druid cluster, but ingesting data from the same
origin, then you could use a simple prompt like this to run a complex cross-cluster comparison:

> Check and compare the datasource for SumUp across all Druid clusters.

The AI agent should then list the Druid clusters, list their datasources, explore their schemas, segments, tasks, and 
their data distribution all by itself, and ultimately present you a nice summary.

## Overview

The Druid MCP server enables AI applications to interact with Apache Druid through the Model Context Protocol (MCP). It provides a standardized interface for querying and exploring Druid datasources, making it easy to integrate Druid data into AI workflows.

## Features

- **Multi-Cluster Support**: Connect to and query multiple Druid clusters simultaneously
- **Query Execution**: SQL and native JSON queries
- **Datasource Management**: List, explore, and inspect datasource schemas
- **Ingestion Monitoring**: Supervisor and task status tracking
- **Cluster Operations**: Service health monitoring and segment analysis
- **Lookup Management**: Query and inspect lookup tables
- **Smart Analysis Prompts**: Pre-built prompts for common data analysis tasks
- **Schema Caching**: Efficient caching with 5-minute TTL for better performance in resources only

## Prerequisites

- Python 3.11+
- Access to an Apache Druid cluster
- MCP-compatible client (e.g., Claude Desktop)

## Installation

Install dependencies using uv:

```bash
uv install
```

Or with pip:

```bash
pip install -e .
```

## Configuration

### Multi-Cluster Support

Configure multiple Druid clusters with whitespace-separated key=value pairs:

```bash
# Define available clusters (optional, defaults to "localhost=http://localhost:8088")
export DRUID_CLUSTERS="localhost=http://localhost:8088 dev=https://druid.dev.example.com prod=https://druid.prod.example.com"

# Set default cluster (optional, defaults to "localhost")
export DRUID_DEFAULT_CLUSTER=dev
```

The cluster names can be any valid string and are used in:
- Resource URIs (e.g., `druid://dev/v2/datasources`)
- Tool parameters (e.g., `execute_sql_query("dev", "SELECT ...")`)
- Cluster management commands

## Usage

### Development Mode

Run with auto-reload for development:

```bash
mcp dev main.py
```

### Production Mode

Run the server in production:

```bash
mcp run main.py
```

### Claude Desktop Integration

Install the server for use with Claude Desktop:

```bash
mcp install main.py
```

## Available Tools

The MCP server exposes 18 tools organized by functionality:

**Important**: All tools (except cluster management tools) require an explicit `cluster` parameter as the first argument. This enables efficient multi-cluster operations without state management.

### Query Execution
- `execute_sql_query(cluster, query, context)`: Execute SQL queries
- `execute_native_query(cluster, query)`: Execute native JSON queries (timeseries, topN, groupBy, etc.)

### Datasource Operations
- `list_datasources(cluster, include_details)`: List all available datasources
- `get_datasource_schema(cluster, datasource)`: Get dimensions and metrics for a datasource

### Ingestion Monitoring
- `list_supervisors(cluster, include_state)`: List all supervisors with optional state information
- `get_supervisor_status(cluster, supervisor_id)`: Get detailed supervisor status and health
- `list_tasks(cluster, ...)`: List tasks with filtering options
- `get_task_status(cluster, task_id)`: Get status of specific tasks

### Cluster Management
- `get_cluster_status(cluster)`: Overall cluster health status
- `list_services(cluster, service_type)`: List active services by type
- `list_segments(cluster, datasource, full)`: List segments for a datasource
- `get_segments_info(cluster, datasource)`: Get aggregated segment statistics

### Lookup Management
- `list_lookups(cluster, tier)`: List lookups by tier
- `get_lookup(cluster, lookup_id, tier)`: Get specific lookup configuration
- `get_lookup_status(cluster, lookup_id, tier)`: Get lookup loading status across nodes

### Multi-Cluster Management
- `get_cluster()`: Get the current active cluster
- `set_cluster(cluster)`: Switch to a different cluster
- `list_clusters()`: List all configured clusters with their base URLs

## Analysis Prompts

5 pre-built prompts with smart defaults (last month to now):

- `analyze_time_range`: Analyze data within time periods
- `explore_datasource`: Explore datasource structure and content
- `monitor_ingestion`: Monitor ingestion health and progress
- `compare_periods`: Compare metrics between time periods
- `data_quality_check`: Perform comprehensive data quality checks

## Examples

### Basic Usage

```python
# List datasources from production cluster
list_datasources("prod")

# Execute SQL query on development cluster
execute_sql_query("dev", "SELECT COUNT(*) FROM wikipedia WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY")

# Get datasource schema from staging
get_datasource_schema("stage", "wikipedia")
```

### Multi-Cluster Operations

```python
# Compare data across environments
dev_count = execute_sql_query("dev", "SELECT COUNT(*) FROM orders")
prod_count = execute_sql_query("prod", "SELECT COUNT(*) FROM orders")

# Check cluster health across all environments
for cluster in ["dev", "stage", "prod"]:
    status = get_cluster_status(cluster)
    print(f"{cluster}: {status}")

# Monitor ingestion across clusters
list_supervisors("dev", include_state=True)
list_supervisors("prod", include_state=True)
```

### Advanced Queries

```python
# Native query with explicit cluster
execute_native_query("prod", {
    "queryType": "timeseries",
    "dataSource": "wikipedia",
    "intervals": ["2024-01-01/2024-01-02"],
    "granularity": "hour",
    "aggregations": [
        {"type": "count", "name": "edits"},
        {"type": "longSum", "name": "added", "fieldName": "added"}
    ]
})

# Complex filtering with multiple parameters
list_tasks("prod", 
    datasource="wikipedia",
    state="running",
    max_tasks=10
)
```

## Resources

Druid API endpoints are exposed as MCP resources with caching and multi-cluster support:

### Resource URI Format
`druid://{cluster}/{path}`

### Available Resources
- `druid://localhost/v2/datasources` - List all datasources
- `druid://localhost/v2/datasources/{name}` - Get datasource schema
- `druid://localhost/coordinator/v1/datasources/{name}` - Coordinator info
- `druid://localhost/coordinator/v1/metadata/datasources/{name}/segments` - Segments
- `druid://localhost/overlord/v1/tasks` - List tasks
- `druid://localhost/overlord/v1/supervisors` - List supervisors
- `druid://localhost/status/health` - Health check

Replace `localhost` with any configured cluster name to access different environments.

## Migration Guide

### From Stateful to Explicit Cluster Parameters

The Druid MCP server has transitioned from implicit cluster state to explicit cluster parameters for better clarity and multi-cluster support.

#### Old Pattern (Stateful)
```python
# Switch cluster state
set_cluster("prod")

# All subsequent operations use the "prod" cluster
datasources = list_datasources()
schema = get_datasource_schema("wikipedia")
status = get_cluster_status()
```

#### New Pattern (Explicit)
```python
# Each operation explicitly specifies the cluster
datasources = list_datasources("prod")
schema = get_datasource_schema("prod", "wikipedia")
status = get_cluster_status("prod")

# Multi-cluster operations are now trivial
dev_data = list_datasources("dev")
prod_data = list_datasources("prod")
```

### Benefits
- **No hidden state**: Always clear which cluster is being queried
- **Parallel operations**: Query multiple clusters simultaneously
- **Better error messages**: Invalid clusters are caught immediately
- **Self-documenting code**: Cluster intent is explicit in every call

## Security

This MCP server provides **read-only** access to your Druid cluster. No write operations are supported or allowed.

## License

MIT
