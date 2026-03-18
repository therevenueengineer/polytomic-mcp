# Polytomic MCP Server

A Model Context Protocol (MCP) server for interacting with the Polytomic API.

## Available Tools

### Connections

| Tool | Description |
|------|-------------|
| `list_connection_types` | List all available connection types (connectors) |
| `list_connections` | List all data connections |
| `get_connection` | Get details for a specific connection |
| `create_connection` | Create a new data connection |
| `update_connection` | Update an existing connection |
| `delete_connection` | Delete a connection |

### Models

| Tool | Description |
|------|-------------|
| `list_models` | List all data models |
| `get_model` | Get details and fields for a specific model |
| `create_model` | Create a new data model |
| `update_model` | Update an existing model |
| `delete_model` | Delete a model |

### Model Syncs (Reverse ETL)

| Tool | Description |
|------|-------------|
| `list_syncs` | List all model syncs |
| `get_sync` | Get details for a specific sync |
| `create_sync` | Create a new model sync |
| `update_sync` | Update an existing sync |
| `delete_sync` | Delete a sync |
| `activate_sync` | Activate (enable) a sync |
| `trigger_sync` | Manually trigger a sync to run |
| `get_sync_status` | Get the current status of a sync |
| `list_sync_executions` | List execution history for a sync |
| `get_sync_execution` | Get details for a specific sync execution |

### Bulk Syncs (ELT)

| Tool | Description |
|------|-------------|
| `list_bulk_syncs` | List all bulk syncs |
| `get_bulk_sync` | Get details for a specific bulk sync |
| `create_bulk_sync` | Create a new bulk sync |
| `update_bulk_sync` | Update an existing bulk sync |
| `delete_bulk_sync` | Delete a bulk sync |
| `activate_bulk_sync` | Activate or deactivate a bulk sync |
| `trigger_bulk_sync` | Manually trigger a bulk sync to run |
| `get_bulk_sync_status` | Get the current status of a bulk sync |
| `list_bulk_sync_executions` | List execution history for a bulk sync |
| `get_bulk_sync_schemas` | Get available schemas/tables for a bulk sync |
| `update_bulk_sync_schemas` | Update which schemas/tables are enabled for a bulk sync |

## Setup

1. Create and activate a virtual environment:
   ```bash
   cd projects/mcp-servers/polytomic
   python -m venv venv
   source venv/bin/activate
   ```

2. Install the package:
   ```bash
   pip install -e .
   ```

3. Run the server:
   ```bash
   python -m polytomic_mcp.server
   # or
   polytomic-mcp
   ```

## Environment Variables

- `POLYTOMIC_API_KEY` - Your Polytomic API key (Bearer token)

## Usage with mcporter

The server is configured in mcporter and can be used via:

```bash
mcporter call polytomic.list_connections
mcporter call polytomic.get_sync id=<sync-id>
mcporter call polytomic.trigger_sync id=<sync-id>
```
