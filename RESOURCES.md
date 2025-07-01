# Druid MCP Resources Documentation

This document describes all available MCP resources provided by the Druid MCP server for accessing Apache Druid cluster data.

## Resource URI Pattern

All resources follow the pattern: `druid://{cluster}/{path}`

Where:
- `{cluster}` is the cluster ID (e.g., "localhost", "prod", "staging")
- `{path}` is the Druid API endpoint path (without `/druid` prefix)

## Available Resources

### Core Data Sources

#### List All Datasources
- **URI**: `druid://{cluster}/v2/datasources`
- **Description**: Returns a JSON array of all datasource names in the cluster
- **Example**: `druid://localhost/v2/datasources`
- **Response**: `["typeform", "ploom_expert", "art_questionnaire", "fret_demo", "sumup"]`

#### Datasource Schema
- **URI**: `druid://{cluster}/v2/datasources/{datasource}`
- **Description**: Returns the schema for a specific datasource including dimensions and metrics
- **Example**: `druid://localhost/v2/datasources/sumup`
- **Response**: JSON object with dimensions, metrics, and aggregators

### Coordinator Resources

#### Datasource Metadata
- **URI**: `druid://{cluster}/coordinator/v1/datasources/{datasource}`
- **Description**: Returns coordinator-level datasource information and metadata
- **Example**: `druid://localhost/coordinator/v1/datasources/sumup`

#### Segment Information
- **URI**: `druid://{cluster}/coordinator/v1/metadata/datasources/{datasource}/segments`
- **Description**: Returns detailed segment information for a datasource
- **Example**: `druid://localhost/coordinator/v1/metadata/datasources/sumup/segments`
- **Response**: Array of segment objects with interval, size, and metadata

#### All Datasources (Coordinator)
- **URI**: `druid://{cluster}/coordinator/v1/datasources`
- **Description**: Returns all datasources from coordinator perspective
- **Example**: `druid://localhost/coordinator/v1/datasources`

#### Server List
- **URI**: `druid://{cluster}/coordinator/v1/servers`
- **Description**: Returns list of all servers in the cluster
- **Example**: `druid://localhost/coordinator/v1/servers`

#### Lookup Configuration
- **URI**: `druid://{cluster}/coordinator/v1/lookups/config/all`
- **Description**: Returns all lookup configurations across all tiers
- **Example**: `druid://localhost/coordinator/v1/lookups/config/all`

- **URI**: `druid://{cluster}/coordinator/v1/lookups/config/{tier}`
- **Description**: Returns lookup configurations for a specific tier
- **Example**: `druid://localhost/coordinator/v1/lookups/config/__default`

- **URI**: `druid://{cluster}/coordinator/v1/lookups/config/{tier}/{lookup_id}`
- **Description**: Returns configuration for a specific lookup
- **Example**: `druid://localhost/coordinator/v1/lookups/config/__default/my_lookup`

#### Lookup Status
- **URI**: `druid://{cluster}/coordinator/v1/lookups/status`
- **Description**: Returns lookup loading status across all cluster nodes
- **Example**: `druid://localhost/coordinator/v1/lookups/status`

### Overlord Resources

#### Task List
- **URI**: `druid://{cluster}/indexer/v1/tasks`
- **Description**: Returns list of all tasks (can be filtered via query parameters)
- **Example**: `druid://localhost/indexer/v1/tasks`

#### Task Status
- **URI**: `druid://{cluster}/indexer/v1/task/{task_id}/status`
- **Description**: Returns detailed status for a specific task
- **Example**: `druid://localhost/indexer/v1/task/index_sumup_2024-01-01_2024-01-02_abc123/status`

#### Supervisor List
- **URI**: `druid://{cluster}/indexer/v1/supervisor`
- **Description**: Returns list of all supervisors
- **Example**: `druid://localhost/indexer/v1/supervisor`

#### Supervisor Status
- **URI**: `druid://{cluster}/indexer/v1/supervisor/{supervisor_id}/status`
- **Description**: Returns detailed status for a specific supervisor
- **Example**: `druid://localhost/indexer/v1/supervisor/sumup_kafka_supervisor/status`

### Health and Status Resources

#### Cluster Health
- **URI**: `druid://{cluster}/status/health`
- **Description**: Returns overall cluster health status
- **Example**: `druid://localhost/status/health`

#### Coordinator Leader
- **URI**: `druid://{cluster}/coordinator/v1/leader`
- **Description**: Returns the current coordinator leader
- **Example**: `druid://localhost/coordinator/v1/leader`

#### Overlord Leader
- **URI**: `druid://{cluster}/indexer/v1/leader`
- **Description**: Returns the current overlord leader
- **Example**: `druid://localhost/indexer/v1/leader`

## Resource Features

### Caching
- All resources are cached for 5 minutes to improve performance
- Cache keys include cluster ID to support multi-cluster setups
- Cache is automatically invalidated after TTL expires

### Multi-Cluster Support
- Resources support multiple clusters via the `{cluster}` parameter
- Cluster must be whitelisted in `DRUID_CLUSTER_URLS` environment variable
- Each cluster has its own HTTP client for proper connection management

### Error Handling
- Resources return proper HTTP status codes
- Druid API errors are parsed and returned as structured error responses
- Network errors are handled gracefully with appropriate error messages

## Usage Examples

### Using MCP Resource Tool
```python
# List datasources in localhost cluster
resource = await ReadMcpResourceTool(
    server="druid", 
    uri="druid://localhost/v2/datasources"
)

# Get schema for specific datasource
schema = await ReadMcpResourceTool(
    server="druid",
    uri="druid://localhost/v2/datasources/sumup"
)

# Check supervisor status
status = await ReadMcpResourceTool(
    server="druid",
    uri="druid://localhost/indexer/v1/supervisor/sumup_kafka_supervisor/status"
)
```

### Direct Resource Access
Resources can be accessed directly via the MCP protocol using any MCP-compatible client. The URI format remains consistent across all access methods.

## Environment Configuration

Resources respect the following environment variables:

- `DRUID_CLUSTER_URLS`: Whitespace-separated cluster definitions (e.g., "localhost=http://localhost:8088 prod=https://druid.prod.com:8088")
- `DRUID_ROUTER_URL`: Legacy single-cluster URL (deprecated)

## Implementation Details

Resources are implemented using FastMCP's `@mcp.resource()` decorator with dynamic path matching up to 5 levels deep. This allows flexible access to any Druid API endpoint while maintaining type safety and validation.

The resource handlers automatically:
1. Validate cluster whitelist
2. Build appropriate HTTP requests
3. Handle authentication and headers
4. Cache responses for performance
5. Parse and return structured data