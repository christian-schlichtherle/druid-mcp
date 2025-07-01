import asyncio
import calendar
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from os import getenv
from typing import Any

import httpx
from dateutil.relativedelta import relativedelta
from mcp.server.fastmcp import FastMCP



def check_truthy(value: Any, message: str) -> Any:
    """Check if the value is truthy."""
    if not value:
        raise ValueError(message)
    return value


def parse_key_value_pairs(pairs: str) -> dict[str, str]:
    """Parse space-separated key=value pairs."""
    return {
        k: v for k, v in (e.split("=", 1) for e in pairs.split()) if k and v
    }


DRUID_CLUSTERS = check_truthy(parse_key_value_pairs(getenv("DRUID_CLUSTERS", "localhost=http://localhost:8088")),
                              "DRUID_CLUSTERS must not be empty")


def get_default_time_interval():
    """Get default time interval: midnight same day last month to now
    
    Returns:
        tuple: (start_time_iso, end_time_iso) as ISO format strings
    """
    now = datetime.now()

    # Go back one month, same day
    try:
        start_date = now - relativedelta(months=1)
        # Set to midnight
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    except ValueError:
        # Handle edge case where day doesn't exist in previous month
        # (e.g., March 31 -> February 28/29)
        start_date = (now.replace(day=1) - relativedelta(months=1))
        # Get last day of that month and set to midnight
        _, last_day = calendar.monthrange(start_date.year, start_date.month)
        start_date = start_date.replace(day=last_day, hour=0, minute=0, second=0, microsecond=0)

    # Format as ISO strings
    start_time = start_date.isoformat()
    end_time = now.isoformat()

    return start_time, end_time


def _enhance_schema_items(items: list, type_mapping: dict[str, str], default_type: str) -> list[dict[str, Any]]:
    """Enhance schema items (dimensions or metrics) with type information
    
    Args:
        items: List of schema items (can be strings or dicts)
        type_mapping: Mapping of column names to data types
        default_type: Default type to use if not found in mapping
        
    Returns:
        List of enhanced items with type information
    """
    return [
        {
            "name": item,
            "type": type_mapping.get(item, default_type)
        } if isinstance(item, str) else {
            **item,
            "type": type_mapping.get(item.get("name", item.get("outputName", "")), default_type)
        }
        for item in items
    ]


@dataclass
class AppContext:
    """Application context containing shared resources"""
    # Store clients per cluster
    clients: dict[str, httpx.AsyncClient]


class DruidError(Exception):
    def __init__(self, error_data: dict):
        self.error = error_data.get("error", "Unknown error")
        self.error_code = error_data.get("errorCode", "UNKNOWN")
        self.error_class = error_data.get("errorClass", "Unknown")
        self.error_message = error_data.get("errorMessage", "")
        self.context = error_data.get("context", {})
        message = self.error_message or self.error
        super().__init__(f"Druid error [{self.error_code}]: {message}")


async def _make_request(
        cluster: str,
        method: str,
        endpoint: str,
        json_data: dict | None = None,
        params: dict | None = None
) -> Any:
    """Make HTTP request to Druid with consistent error handling
    
    Args:
        cluster: Target cluster name (required)
        method: HTTP method (GET or POST)
        endpoint: API endpoint path
        json_data: JSON payload for POST requests
        params: Query parameters
        
    Returns:
        JSON response data
        
    Raises:
        DruidError: If Druid returns an error response
        ValueError: If cluster is not in whitelist
        RuntimeError: If the client is not initialized
    """
    ctx = mcp.get_context()
    if ctx is None:
        raise RuntimeError("HTTP client not initialized. Server may not be started properly.")

    app_context = ctx.request_context.lifespan_context
    assert isinstance(app_context, AppContext)

    # Validate cluster
    if cluster not in DRUID_CLUSTERS:
        available = ", ".join(DRUID_CLUSTERS.keys())
        raise ValueError(f"Invalid cluster '{cluster}'. Available clusters: {available}")

    # Get the client for the cluster
    if cluster not in app_context.clients:
        raise RuntimeError(f"No client available for cluster '{cluster}'")

    client = app_context.clients[cluster]

    try:
        if method.upper() == "POST":
            response = await client.post(endpoint, json=json_data, params=params)
        else:
            response = await client.get(endpoint, params=params)

        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        if e.response.content:
            raise DruidError(e.response.json())
        raise


async def _check_leader_service(cluster: str, endpoint: str) -> dict[str, Any]:
    """Check if a leader service is available and get leader info
    
    Args:
        endpoint: API endpoint to check leader status
        cluster: Target cluster name
        
    Returns:
        Dictionary with availability and leader information
    """
    ctx = mcp.get_context()
    if ctx is None:
        return {"available": False, "leader": None}

    app_context = ctx.request_context.lifespan_context
    assert isinstance(app_context, AppContext)

    # Validate cluster
    if cluster not in DRUID_CLUSTERS:
        raise ValueError(f"Invalid cluster '{cluster}'")

    # Get the client for the target cluster
    if cluster not in app_context.clients:
        return {"available": False, "leader": None}

    client = app_context.clients[cluster]

    try:
        response = await client.get(endpoint)
        return {
            "available": response.status_code == 200,
            "leader": response.text.strip('"') if response.status_code == 200 else None
        }
    except (httpx.RequestError, httpx.HTTPStatusError):
        return {"available": False, "leader": None}




@asynccontextmanager
async def lifespan(_server: FastMCP) -> AsyncIterator[AppContext]:
    """Manage httpx clients lifecycle for the MCP server"""
    clients = {
        cluster_name: httpx.AsyncClient(
            base_url=cluster_url,
            headers={"Content-Type": "application/json"},
            timeout=30.0,
        )
        for cluster_name, cluster_url in DRUID_CLUSTERS.items()
    }

    try:
        yield AppContext(clients=clients)
    finally:
        await asyncio.gather(*[client.aclose() for client in clients.values()], return_exceptions=True)


mcp = FastMCP("druid", lifespan=lifespan)


@mcp.tool()
async def list_clusters() -> list[str]:
    """List all available Druid clusters

    Returns:
        Sorted list of cluster names
    """
    return sorted(DRUID_CLUSTERS.keys())


@mcp.tool()
async def execute_sql_query(cluster: str, query: str, context: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Execute SQL query against Druid
    
    Args:
        cluster: Target cluster name (e.g., 'fret-dev', 'fret-prod')
        query: SQL query string
        context: Optional query context parameters
    
    Returns:
        Query results as a list of objects
        
    Example:
        execute_sql_query("fret-prod", "SELECT COUNT(*) FROM datasource")
    """
    payload: dict[str, Any] = {
        "query": query
    }
    if context:
        payload["context"] = context

    return await _make_request(cluster, "POST", "/druid/v2/sql", json_data=payload)


@mcp.tool()
async def execute_native_query(cluster: str, query: dict[str, Any]) -> list[dict[str, Any]]:
    """Execute native JSON query against Druid
    
    Args:
        cluster: Target cluster name (e.g., 'fret-dev', 'fret-prod')
        query: Native query as JSON object (must include queryType and dataSource)
    
    Returns:
        Query results as list of objects
        
    Example:
        execute_native_query("fret-prod", {
            "queryType": "timeseries",
            "dataSource": "wikipedia",
            "intervals": ["2015-09-12/2015-09-13"],
            "granularity": "hour",
            "aggregations": [{"type": "count", "name": "count"}]
        })
    """
    if "queryType" not in query:
        raise ValueError("Query must include 'queryType' field")
    if "dataSource" not in query:
        raise ValueError("Query must include 'dataSource' field")

    return await _make_request(cluster, "POST", "/druid/v2", json_data=query)


@mcp.tool()
async def list_datasources(cluster: str, include_details: bool = False) -> list[Any]:
    """List all available datasources in the cluster
    
    Args:
        cluster: Target cluster name (e.g., 'fret-dev', 'fret-prod')
        include_details: If True, includes full datasource metadata
    
    Returns:
        List of datasource names or full datasource objects
        
    Example:
        list_datasources("fret-prod")
        list_datasources("fret-prod", include_details=True)
    """
    params = {"full": "true"} if include_details else {}
    return await _make_request(cluster, "GET", "/druid/coordinator/v1/datasources", params=params)


@mcp.tool()
async def get_datasource_schema(cluster: str, datasource: str) -> dict[str, Any]:
    """Get dimensions and metrics schema for a datasource with column types
    
    Args:
        cluster: Target cluster name (e.g., 'fret-dev', 'fret-prod')
        datasource: Name of the datasource
    
    Returns:
        Dictionary containing dimensions, metrics, and aggregators with type information
        
    Example:
        get_datasource_schema("fret-prod", "wikipedia")
    """
    # Get basic schema
    schema = await _make_request(cluster, "GET", f"/druid/v2/datasources/{datasource}")

    # Use INFORMATION_SCHEMA to get column types
    sql_query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?"
    columns_info = await _make_request(cluster, "POST", "/druid/v2/sql", json_data={
        "query": sql_query,
        "parameters": [{"type": "VARCHAR", "value": datasource}]
    })

    # Create type mapping
    type_mapping = {col["COLUMN_NAME"]: col["DATA_TYPE"] for col in columns_info}

    # Enhance dimensions and metrics with types
    if "dimensions" in schema:
        schema["dimensions"] = _enhance_schema_items(schema["dimensions"], type_mapping, "STRING")

    if "metrics" in schema:
        schema["metrics"] = _enhance_schema_items(schema["metrics"], type_mapping, "DOUBLE")

    return schema


@mcp.tool()
async def list_supervisors(cluster: str, include_state: bool = False) -> list[Any]:
    """List all supervisors in the cluster
    
    Args:
        cluster: Target cluster name (e.g., 'fret-dev', 'fret-prod')
        include_state: If True, includes supervisor state information
    
    Returns:
        List of supervisor IDs or full supervisor information
        
    Example:
        list_supervisors("fret-prod")
        list_supervisors("fret-prod", include_state=True)
    """
    params = {"state": "true"} if include_state else {}
    return await _make_request(cluster, "GET", "/druid/indexer/v1/supervisor", params=params)


@mcp.tool()
async def get_supervisor_status(cluster: str, supervisor_id: str) -> dict[str, Any]:
    """Get detailed status of a supervisor
    
    Args:
        cluster: Target cluster name (e.g., 'fret-dev', 'fret-prod')
        supervisor_id: ID of the supervisor
    
    Returns:
        Detailed supervisor status including health and statistics
        
    Example:
        get_supervisor_status("fret-prod", "my_supervisor")
    """
    return await _make_request(cluster, "GET", f"/druid/indexer/v1/supervisor/{supervisor_id}/status")


@mcp.tool()
async def list_tasks(
        cluster: str,
        datasource: str = "",
        state: str = "",
        created_time_interval: str = "",
        max_tasks: int = 0,
        task_type: str = "",
) -> list[Any]:
    """List tasks with optional filtering
    
    Args:
        cluster: Target cluster name (e.g., 'fret-dev', 'fret-prod')
        datasource: Filter by datasource name
        state: Filter by state (running, complete, waiting, pending)
        created_time_interval: ISO interval for task creation time
        max_tasks: Controls task visibility and limiting:
                  - Default (0): Shows ONLY running tasks (best for monitoring)
                  - N>0: Shows ALL running tasks + up to N non-running tasks
        task_type: Filter by task type
    
    Returns:
        List of tasks matching the criteria
        
    Examples:
        - list_tasks("fret-prod", datasource="sumup") - Only running sumup tasks (default)
        - list_tasks("fret-prod", datasource="sumup", max_tasks=None) - All sumup tasks
        - list_tasks("fret-prod", datasource="sumup", max_tasks=5) - Running + up to 5 non-running sumup tasks
        - list_tasks("fret-prod", state="complete", max_tasks=10) - Up to 10 completed tasks
        
    Note:
        The default behavior (max_tasks=0) prioritizes operational visibility by showing
        only running tasks, which are typically the most actionable. Use max_tasks=None
        to see historical data or max_tasks=N to get a controlled mix of both.
    """
    params = {}
    if datasource:
        params["datasource"] = datasource
    if state:
        params["state"] = state
    if created_time_interval:
        params["createdTimeInterval"] = created_time_interval
    if max_tasks is not None:
        params["max"] = max_tasks
    if task_type:
        params["type"] = task_type

    return await _make_request(cluster, "GET", "/druid/indexer/v1/tasks", params=params)


@mcp.tool()
async def get_task_status(cluster: str, task_id: str) -> dict[str, Any]:
    """Get status of a specific task
    
    Args:
        cluster: Target cluster name (e.g., 'fret-dev', 'fret-prod')
        task_id: ID of the task
    
    Returns:
        Task status information
        
    Example:
        get_task_status("fret-prod", "index_wikipedia_2023-01-01T00:00:00.000Z")
    """
    return await _make_request(cluster, "GET", f"/druid/indexer/v1/task/{task_id}/status")


@mcp.tool()
async def list_segments(
        cluster: str,
        datasource: str,
        full: bool = False
) -> list[Any]:
    """List segments for a datasource
    
    Args:
        cluster: Target cluster name (e.g., 'fret-dev', 'fret-prod')
        datasource: Name of the datasource
        full: If True, returns full segment metadata objects with size, interval, etc.
              If False, returns segment identifiers only
    
    Returns:
        List of segments with metadata (full=True) or segment identifiers (full=False)
        
    Example:
        list_segments("fret-prod", "wikipedia")
        list_segments("fret-prod", "wikipedia", full=True)
    """
    endpoint = f"/druid/coordinator/v1/metadata/datasources/{datasource}/segments"
    params = {}
    if full:
        params["full"] = "true"

    return await _make_request(cluster, "GET", endpoint, params=params)


@mcp.tool()
async def get_segments_info(cluster: str, datasource: str) -> dict[str, Any]:
    """Get aggregated segment statistics for a datasource
    
    Args:
        cluster: Target cluster name (e.g., 'fret-dev', 'fret-prod')
        datasource: Name of the datasource
    
    Returns:
        Dictionary with segment count, total size, and time range
        
    Example:
        get_segments_info("fret-prod", "wikipedia")
    """
    try:
        segments = await list_segments(cluster, datasource, full=True)

        if not segments:
            return {
                "count": 0,
                "total_size": 0,
                "earliest": None,
                "latest": None
            }

        total_size = sum(s.get("size", 0) for s in segments)

        intervals = [
            tuple(s["interval"].split("/"))
            for s in segments
            if s.get("interval") and len(s["interval"].split("/")) == 2
        ]

        return {
            "count": len(segments),
            "total_size": total_size,
            "earliest": min((i[0] for i in intervals), default=None),
            "latest": max((i[1] for i in intervals), default=None)
        }
    except Exception:
        raise


@mcp.tool()
async def get_cluster_status(cluster: str) -> dict[str, Any]:
    """Get overall cluster health status
    
    Args:
        cluster: Target cluster name (e.g., 'fret-dev', 'fret-prod')
    
    Returns:
        Dictionary with health status of each service type
        
    Example:
        get_cluster_status("fret-prod")
    """
    status: dict[str, Any] = {}

    # Check coordinator and overlord
    status["coordinator"] = await _check_leader_service(cluster, "/druid/coordinator/v1/leader")
    status["overlord"] = await _check_leader_service(cluster, "/druid/indexer/v1/leader")

    # Check router health
    try:
        ctx = mcp.get_context()
        if ctx is not None:
            app_context = ctx.request_context.lifespan_context
            assert isinstance(app_context, AppContext)

            # Validate cluster
            if cluster not in DRUID_CLUSTERS:
                raise ValueError(f"Invalid cluster '{cluster}'")

            if cluster in app_context.clients:
                client = app_context.clients[cluster]
                response = await client.get("/status/health")
                status["router"] = response.status_code == 200
            else:
                status["router"] = False
        else:
            status["router"] = False
    except (httpx.RequestError, httpx.HTTPStatusError):
        status["router"] = False

    return status


@mcp.tool()
async def list_services(cluster: str, service_type: str = '') -> list[dict[str, Any]]:
    """List active services in the cluster
    
    Args:
        cluster: Target cluster name (e.g., 'fret-dev', 'fret-prod')
        service_type: Filter by service type (coordinator, overlord, broker, historical, peon, router)
    
    Returns:
        List of active services with their metadata
        
    Example:
        list_services("fret-prod")
        list_services("fret-prod", service_type="historical")
    """
    if service_type:
        query = "SELECT server, server_type as type, tier, curr_size, max_size FROM sys.servers WHERE server_type = ?"
        servers = await _make_request(cluster, "POST", "/druid/v2/sql", json_data={
            "query": query,
            "parameters": [{"type": "VARCHAR", "value": service_type}]
        })
    else:
        query = "SELECT server, server_type as type, tier, curr_size, max_size FROM sys.servers"
        servers = await _make_request(cluster, "POST", "/druid/v2/sql", json_data={"query": query})

    return servers


@mcp.tool()
async def list_lookups(cluster: str, tier: str = "") -> dict[str, Any]:
    """List all lookups or lookups in a specific tier
    
    Args:
        cluster: Target cluster name (e.g., 'fret-dev', 'fret-prod')
        tier: Optional tier name to filter lookups
    
    Returns:
        Dictionary of lookups organized by tier
        
    Example:
        list_lookups("fret-prod")
        list_lookups("fret-prod", tier="__default")
    """
    if tier:
        endpoint = f"/druid/coordinator/v1/lookups/config/{tier}"
    else:
        endpoint = "/druid/coordinator/v1/lookups/config/all"

    return await _make_request(cluster, "GET", endpoint)


@mcp.tool()
async def get_lookup(cluster: str, lookup_id: str, tier: str = "__default") -> dict[str, Any]:
    """Get configuration for a specific lookup
    
    Args:
        cluster: Target cluster name (e.g., 'fret-dev', 'fret-prod')
        lookup_id: ID of the lookup
        tier: Tier name where the lookup exists
    
    Returns:
        Lookup configuration
        
    Example:
        get_lookup("fret-prod", "my_lookup")
        get_lookup("fret-prod", "my_lookup", tier="custom_tier")
    """
    return await _make_request(cluster, "GET", f"/druid/coordinator/v1/lookups/config/{tier}/{lookup_id}")


@mcp.tool()
async def get_lookup_status(cluster: str, lookup_id: str = "", tier: str = "__default") -> dict[str, Any]:
    """Get lookup loading status across the cluster
    
    Args:
        cluster: Target cluster name (e.g., 'fret-dev', 'fret-prod')
        lookup_id: Optional lookup ID to filter
        tier: Optional tier name to filter

    Returns:
        Dictionary showing which nodes have loaded which lookups
        
    Example:
        get_lookup_status("fret-prod")
        get_lookup_status("fret-prod", lookup_id="my_lookup")
    """
    status = await _make_request(cluster, "GET", "/druid/coordinator/v1/lookups/status")

    # Filter by tier and/or lookup_id if provided
    if tier or lookup_id:
        return {
            t: ({lid: nodes for lid, nodes in lookups.items() if lid == lookup_id} if lookup_id else lookups)
            for t, lookups in status.items()
            if not tier or t == tier
        }

    return status




@mcp.prompt()
def analyze_time_range(
        datasource: str,
        start_time: str = "",
        end_time: str = ""
) -> str:
    """Prompt for analyzing data within a time range"""
    if start_time is None or end_time is None:
        default_start, default_end = get_default_time_interval()
        start_time = start_time or default_start
        end_time = end_time or default_end

    return f"""Analyze {datasource} data between {start_time} and {end_time}.
(Default: last month same day to now)
    
Consider:
- Key metrics and their trends
- Anomalies or unusual patterns
- Top dimensions by volume
- Data quality issues

Steps:
1. Query the datasource schema to understand available dimensions and metrics
2. Run aggregation queries to see metric trends over time
3. Identify top values for key dimensions
4. Check for data gaps or quality issues"""


@mcp.prompt()
def explore_datasource(datasource: str) -> str:
    """Prompt for exploring a datasource structure and content"""
    return f"""Explore the {datasource} datasource structure.

Steps:
1. Get schema to understand dimensions and metrics
2. Query recent data sample to see actual values
3. Check segment distribution for data coverage
4. Identify key patterns and characteristics

Questions to answer:
- What are the key dimensions and metrics?
- What time range does the data cover?
- What are typical values for main dimensions?
- Are there any data quality concerns?"""


@mcp.prompt()
def monitor_ingestion(datasource: str) -> str:
    """Prompt for monitoring ingestion status"""
    return f"""Monitor ingestion status for {datasource}.

Check:
- Active supervisors and their health
- Running tasks and completion rates
- Recent segment creation
- Any errors or lag

Steps:
1. List supervisors filtering by datasource
2. Check supervisor status and lag
3. Review recent tasks and their status
4. Verify segment creation is ongoing"""


@mcp.prompt()
def compare_periods(
        datasource: str,
        period1_start: str = "",
        period1_end: str = "",
        period2_start: str = "",
        period2_end: str = ""
) -> str:
    """Prompt for comparing metrics between time periods"""
    if any(x is None for x in [period1_start, period1_end, period2_start, period2_end]):
        # Default: compare last month to month before that
        now = datetime.now()
        p1_end = now
        p1_start = (now - relativedelta(months=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        p2_end = p1_start
        p2_start = (p1_start - relativedelta(months=1)).replace(hour=0, minute=0, second=0, microsecond=0)

        period1_start = period1_start or p1_start.isoformat()
        period1_end = period1_end or p1_end.isoformat()
        period2_start = period2_start or p2_start.isoformat()
        period2_end = period2_end or p2_end.isoformat()

    return f"""Compare {datasource} metrics between two time periods:
Period 1: {period1_start} to {period1_end}
Period 2: {period2_start} to {period2_end}
(Default: last month vs month before)

Analyze:
- Metric changes (absolute and percentage)
- Dimension distribution shifts
- Data volume differences
- Any new or missing dimensions

Create queries that:
1. Aggregate key metrics for each period
2. Compare top dimension values
3. Calculate growth rates
4. Identify significant changes"""


@mcp.prompt()
def data_quality_check(datasource: str, time_interval: str = "") -> str:
    """Prompt for checking data quality"""
    if time_interval is None:
        start_time, end_time = get_default_time_interval()
        time_interval = f"{start_time}/{end_time}"

    return f"""Perform data quality checks on {datasource} for {time_interval}.
(Default: last month same day to now)

Check for:
- Null or missing values in key dimensions
- Metric anomalies (negative values, outliers)
- Data gaps or missing time periods
- Duplicate records
- Schema consistency

Queries to run:
1. Count nulls in each dimension
2. Find min/max values for metrics
3. Check time coverage and gaps
4. Verify expected dimension cardinality"""
