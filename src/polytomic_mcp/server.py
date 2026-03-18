#!/usr/bin/env python3
"""Polytomic MCP Server - Model Context Protocol server for Polytomic API."""

import os
import json
import httpx
from mcp.server.fastmcp import FastMCP

# Configuration
POLYTOMIC_API_BASE = "https://app.polytomic.com/api"
POLYTOMIC_API_KEY = os.environ.get("POLYTOMIC_API_KEY", "")

# Create MCP server
mcp = FastMCP("polytomic")


async def polytomic_request(
    endpoint: str, method: str = "GET", body: dict | None = None
) -> dict:
    """Make an authenticated request to the Polytomic API."""
    url = f"{POLYTOMIC_API_BASE}{endpoint}"
    headers = {
        "Authorization": f"Bearer {POLYTOMIC_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    async with httpx.AsyncClient(timeout=60.0) as client:
        if method == "GET":
            response = await client.get(url, headers=headers)
        elif method == "POST":
            response = await client.post(url, headers=headers, json=body or {})
        elif method == "PUT":
            response = await client.put(url, headers=headers, json=body or {})
        elif method == "PATCH":
            response = await client.patch(url, headers=headers, json=body or {})
        elif method == "DELETE":
            response = await client.delete(url, headers=headers)
        else:
            raise ValueError(f"Unsupported method: {method}")

        response.raise_for_status()
        # DELETE often returns empty response
        if response.status_code == 204 or not response.content:
            return {"status": "success"}
        return response.json()


# =============================================================================
# CONNECTIONS
# =============================================================================

@mcp.tool()
async def list_connection_types() -> str:
    """List all available connection types (connectors) in Polytomic."""
    result = await polytomic_request("/connections/types")
    return json.dumps(result, indent=2)


@mcp.tool()
async def list_connections() -> str:
    """List all data connections in Polytomic."""
    result = await polytomic_request("/connections")
    return json.dumps(result, indent=2)


@mcp.tool()
async def get_connection(id: str) -> str:
    """Get details for a specific connection.

    Args:
        id: The connection ID
    """
    result = await polytomic_request(f"/connections/{id}")
    return json.dumps(result, indent=2)


@mcp.tool()
async def create_connection(
    name: str,
    type: str,
    configuration: str,
) -> str:
    """Create a new data connection in Polytomic.

    Args:
        name: Name for the connection
        type: Connection type (e.g. 'postgresql', 'salesforce', 'snowflake')
        configuration: JSON string with connection config (varies by type)
    """
    body = {
        "name": name,
        "type": type,
        "configuration": json.loads(configuration),
    }
    result = await polytomic_request("/connections", method="POST", body=body)
    return json.dumps(result, indent=2)


@mcp.tool()
async def update_connection(
    id: str,
    name: str | None = None,
    configuration: str | None = None,
) -> str:
    """Update an existing connection in Polytomic.

    Args:
        id: The connection ID to update
        name: Optional new name for the connection
        configuration: Optional JSON string with connection config
    """
    current = await polytomic_request(f"/connections/{id}")
    current_data = current.get("data", current)

    body = {
        "name": name or current_data.get("name"),
        "type": current_data.get("type"),
        "configuration": json.loads(configuration) if configuration else current_data.get("configuration"),
    }
    result = await polytomic_request(f"/connections/{id}", method="PUT", body=body)
    return json.dumps(result, indent=2)


@mcp.tool()
async def delete_connection(id: str) -> str:
    """Delete a connection from Polytomic.

    Args:
        id: The connection ID to delete
    """
    result = await polytomic_request(f"/connections/{id}", method="DELETE")
    return json.dumps(result, indent=2)


# =============================================================================
# MODELS
# =============================================================================

@mcp.tool()
async def list_models() -> str:
    """List all data models in Polytomic."""
    result = await polytomic_request("/models")
    return json.dumps(result, indent=2)


@mcp.tool()
async def get_model(id: str) -> str:
    """Get details and fields for a specific model.

    Args:
        id: The model ID
    """
    result = await polytomic_request(f"/models/{id}")
    return json.dumps(result, indent=2)


@mcp.tool()
async def create_model(
    name: str,
    connection_id: str,
    configuration: str,
    identifier: str | None = None,
    tracking_columns: str | None = None,
) -> str:
    """Create a new data model in Polytomic.

    Args:
        name: Name for the model
        connection_id: The connection ID this model uses
        configuration: JSON string with model config (e.g. {"query": "SELECT * FROM users"} or {"table": "users"})
        identifier: Optional field name to use as unique identifier
        tracking_columns: Optional JSON array of column names for change tracking
    """
    body = {
        "name": name,
        "connection_id": connection_id,
        "configuration": json.loads(configuration),
    }
    if identifier:
        body["identifier"] = identifier
    if tracking_columns:
        body["tracking_columns"] = json.loads(tracking_columns)

    result = await polytomic_request("/models", method="POST", body=body)
    return json.dumps(result, indent=2)


@mcp.tool()
async def update_model(
    id: str,
    name: str | None = None,
    configuration: str | None = None,
    identifier: str | None = None,
    tracking_columns: str | None = None,
) -> str:
    """Update an existing data model in Polytomic.

    Args:
        id: The model ID to update
        name: Optional new name for the model
        configuration: Optional JSON string with model config (e.g. {"query": "SELECT * FROM users"})
        identifier: Optional field name to use as unique identifier
        tracking_columns: Optional JSON array of column names for change tracking
    """
    current = await polytomic_request(f"/models/{id}")
    current_data = current.get("data", current)

    body = {
        "name": name or current_data.get("name"),
        "connection_id": current_data.get("connection_id"),
        "configuration": json.loads(configuration) if configuration else current_data.get("configuration"),
    }
    if identifier:
        body["identifier"] = identifier
    elif current_data.get("identifier"):
        body["identifier"] = current_data.get("identifier")

    if tracking_columns:
        body["tracking_columns"] = json.loads(tracking_columns)
    elif current_data.get("tracking_columns"):
        body["tracking_columns"] = current_data.get("tracking_columns")

    result = await polytomic_request(f"/models/{id}", method="PUT", body=body)
    return json.dumps(result, indent=2)


@mcp.tool()
async def delete_model(id: str) -> str:
    """Delete a model from Polytomic.

    Args:
        id: The model ID to delete
    """
    result = await polytomic_request(f"/models/{id}", method="DELETE")
    return json.dumps(result, indent=2)


# =============================================================================
# MODEL SYNCS (Reverse ETL)
# =============================================================================

@mcp.tool()
async def list_syncs() -> str:
    """List all model syncs in Polytomic."""
    result = await polytomic_request("/syncs")
    return json.dumps(result, indent=2)


@mcp.tool()
async def get_sync(id: str) -> str:
    """Get details for a specific sync.

    Args:
        id: The sync ID
    """
    result = await polytomic_request(f"/syncs/{id}")
    return json.dumps(result, indent=2)


@mcp.tool()
async def create_sync(
    name: str,
    mode: str,
    target: str,
    fields: str,
    identity: str | None = None,
    schedule: str | None = None,
    filters: str | None = None,
    filter_logic: str | None = None,
) -> str:
    """Create a new model sync (reverse ETL) in Polytomic.

    Args:
        name: Name for the sync
        mode: Sync mode ('create', 'update', 'updateOrCreate', 'replace', 'append')
        target: JSON string with target config {"connection_id": "...", "object": "...", "configuration": {...}}
        fields: JSON array of field mappings [{"source": {"field": "...", "model_id": "..."}, "target": "..."}]
        identity: Optional JSON for identity mapping (required for update modes)
        schedule: Optional JSON for schedule {"frequency": "manual|hourly|daily|weekly|monthly", ...}
        filters: Optional JSON array of filters
        filter_logic: Optional filter logic string (e.g. "A AND B")
    """
    body = {
        "name": name,
        "mode": mode,
        "target": json.loads(target),
        "fields": json.loads(fields),
    }
    if identity:
        body["identity"] = json.loads(identity)
    if schedule:
        body["schedule"] = json.loads(schedule)
    if filters:
        body["filters"] = json.loads(filters)
    if filter_logic:
        body["filter_logic"] = filter_logic

    result = await polytomic_request("/syncs", method="POST", body=body)
    return json.dumps(result, indent=2)


@mcp.tool()
async def update_sync(
    id: str,
    name: str | None = None,
    mode: str | None = None,
    fields: str | None = None,
    identity: str | None = None,
    schedule: str | None = None,
    filters: str | None = None,
    filter_logic: str | None = None,
    active: bool | None = None,
) -> str:
    """Update an existing sync in Polytomic.

    Args:
        id: The sync ID to update
        name: Optional new name
        mode: Optional sync mode
        fields: Optional JSON array of field mappings
        identity: Optional JSON for identity mapping
        schedule: Optional JSON for schedule
        filters: Optional JSON array of filters
        filter_logic: Optional filter logic string
        active: Optional boolean to enable/disable sync
    """
    current = await polytomic_request(f"/syncs/{id}")
    current_data = current.get("data", current)

    body = {
        "name": name or current_data.get("name"),
        "mode": mode or current_data.get("mode"),
        "target": current_data.get("target"),
        "fields": json.loads(fields) if fields else current_data.get("fields"),
    }
    
    if identity:
        body["identity"] = json.loads(identity)
    elif current_data.get("identity"):
        body["identity"] = current_data.get("identity")

    if schedule:
        body["schedule"] = json.loads(schedule)
    elif current_data.get("schedule"):
        body["schedule"] = current_data.get("schedule")

    if filters:
        body["filters"] = json.loads(filters)
    elif current_data.get("filters"):
        body["filters"] = current_data.get("filters")

    if filter_logic is not None:
        body["filter_logic"] = filter_logic
    elif current_data.get("filter_logic"):
        body["filter_logic"] = current_data.get("filter_logic")

    if active is not None:
        body["active"] = active

    result = await polytomic_request(f"/syncs/{id}", method="PUT", body=body)
    return json.dumps(result, indent=2)


@mcp.tool()
async def delete_sync(id: str) -> str:
    """Delete a sync from Polytomic.

    Args:
        id: The sync ID to delete
    """
    result = await polytomic_request(f"/syncs/{id}", method="DELETE")
    return json.dumps(result, indent=2)


@mcp.tool()
async def activate_sync(id: str) -> str:
    """Activate (enable) a sync.

    Args:
        id: The sync ID to activate
    """
    body = {"active": True}
    result = await polytomic_request(f"/syncs/{id}/activate", method="POST", body=body)
    return json.dumps(result, indent=2)


@mcp.tool()
async def trigger_sync(id: str) -> str:
    """Manually trigger a sync to run.

    Args:
        id: The sync ID to trigger
    """
    result = await polytomic_request(f"/syncs/{id}/trigger", method="POST")
    return json.dumps(result, indent=2)


@mcp.tool()
async def get_sync_status(id: str) -> str:
    """Get the current status of a sync.

    Args:
        id: The sync ID
    """
    result = await polytomic_request(f"/syncs/{id}/status")
    return json.dumps(result, indent=2)


@mcp.tool()
async def list_sync_executions(id: str, limit: int = 10) -> str:
    """List execution history for a sync.

    Args:
        id: The sync ID
        limit: Maximum number of executions to return (default 10)
    """
    result = await polytomic_request(f"/syncs/{id}/executions?limit={limit}")
    return json.dumps(result, indent=2)


@mcp.tool()
async def get_sync_execution(sync_id: str, execution_id: str) -> str:
    """Get details for a specific sync execution.

    Args:
        sync_id: The sync ID
        execution_id: The execution ID
    """
    result = await polytomic_request(f"/syncs/{sync_id}/executions/{execution_id}")
    return json.dumps(result, indent=2)


# =============================================================================
# BULK SYNCS (ELT)
# =============================================================================

@mcp.tool()
async def list_bulk_syncs() -> str:
    """List all bulk syncs in Polytomic."""
    result = await polytomic_request("/bulk/syncs")
    return json.dumps(result, indent=2)


@mcp.tool()
async def get_bulk_sync(id: str) -> str:
    """Get details for a specific bulk sync.

    Args:
        id: The bulk sync ID
    """
    result = await polytomic_request(f"/bulk/syncs/{id}")
    return json.dumps(result, indent=2)


@mcp.tool()
async def create_bulk_sync(
    name: str,
    source_connection_id: str,
    dest_connection_id: str,
    destination_configuration: str,
    schedule: str | None = None,
    source_configuration: str | None = None,
) -> str:
    """Create a new bulk sync (ELT) in Polytomic.

    Args:
        name: Name for the bulk sync
        source_connection_id: Source connection ID
        dest_connection_id: Destination connection ID
        destination_configuration: JSON string with destination config (e.g. {"schema": "public"})
        schedule: Optional JSON for schedule
        source_configuration: Optional JSON string with source-specific config
    """
    body = {
        "name": name,
        "source_connection_id": source_connection_id,
        "dest_connection_id": dest_connection_id,
        "destination_configuration": json.loads(destination_configuration),
    }
    if schedule:
        body["schedule"] = json.loads(schedule)
    if source_configuration:
        body["source_configuration"] = json.loads(source_configuration)

    result = await polytomic_request("/bulk/syncs", method="POST", body=body)
    return json.dumps(result, indent=2)


@mcp.tool()
async def update_bulk_sync(
    id: str,
    name: str | None = None,
    schedule: str | None = None,
    destination_configuration: str | None = None,
    source_configuration: str | None = None,
) -> str:
    """Update an existing bulk sync in Polytomic.

    Args:
        id: The bulk sync ID to update
        name: Optional new name
        schedule: Optional JSON for schedule
        destination_configuration: Optional JSON string with destination config
        source_configuration: Optional JSON string with source config
    """
    current = await polytomic_request(f"/bulk/syncs/{id}")
    current_data = current.get("data", current)

    body = {
        "name": name or current_data.get("name"),
        "source_connection_id": current_data.get("source_connection_id"),
        "dest_connection_id": current_data.get("dest_connection_id"),
        "destination_configuration": json.loads(destination_configuration) if destination_configuration else current_data.get("destination_configuration"),
    }
    
    if schedule:
        body["schedule"] = json.loads(schedule)
    elif current_data.get("schedule"):
        body["schedule"] = current_data.get("schedule")

    if source_configuration:
        body["source_configuration"] = json.loads(source_configuration)
    elif current_data.get("source_configuration"):
        body["source_configuration"] = current_data.get("source_configuration")

    result = await polytomic_request(f"/bulk/syncs/{id}", method="PUT", body=body)
    return json.dumps(result, indent=2)


@mcp.tool()
async def delete_bulk_sync(id: str) -> str:
    """Delete a bulk sync from Polytomic.

    Args:
        id: The bulk sync ID to delete
    """
    result = await polytomic_request(f"/bulk/syncs/{id}", method="DELETE")
    return json.dumps(result, indent=2)


@mcp.tool()
async def activate_bulk_sync(id: str, active: bool = True) -> str:
    """Activate or deactivate a bulk sync.

    Args:
        id: The bulk sync ID
        active: True to activate, False to deactivate
    """
    body = {"active": active}
    result = await polytomic_request(f"/bulk/syncs/{id}/activate", method="POST", body=body)
    return json.dumps(result, indent=2)


@mcp.tool()
async def trigger_bulk_sync(id: str) -> str:
    """Manually trigger a bulk sync to run.

    Args:
        id: The bulk sync ID to trigger
    """
    result = await polytomic_request(f"/bulk/syncs/{id}/executions", method="POST")
    return json.dumps(result, indent=2)


@mcp.tool()
async def get_bulk_sync_status(id: str) -> str:
    """Get the current status of a bulk sync.

    Args:
        id: The bulk sync ID
    """
    result = await polytomic_request(f"/bulk/syncs/{id}/status")
    return json.dumps(result, indent=2)


@mcp.tool()
async def list_bulk_sync_executions(id: str, limit: int = 10) -> str:
    """List execution history for a bulk sync.

    Args:
        id: The bulk sync ID
        limit: Maximum number of executions to return (default 10)
    """
    result = await polytomic_request(f"/bulk/syncs/{id}/executions?limit={limit}")
    return json.dumps(result, indent=2)


@mcp.tool()
async def get_bulk_sync_schemas(id: str) -> str:
    """Get available schemas/tables for a bulk sync.

    Args:
        id: The bulk sync ID
    """
    result = await polytomic_request(f"/bulk/syncs/{id}/schemas")
    return json.dumps(result, indent=2)


@mcp.tool()
async def update_bulk_sync_schemas(id: str, schemas: str) -> str:
    """Update which schemas/tables are enabled for a bulk sync.

    Args:
        id: The bulk sync ID
        schemas: JSON array of schema configurations
    """
    body = {"schemas": json.loads(schemas)}
    result = await polytomic_request(f"/bulk/syncs/{id}/schemas", method="PATCH", body=body)
    return json.dumps(result, indent=2)


def main():
    """Run the MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
