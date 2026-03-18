# Polytomic MCP Server

A Model Context Protocol (MCP) server for interacting with the Polytomic API.

## Available Tools

| Tool | Description |
|------|-------------|
| `list_connections` | List all data connections |
| `get_connection` | Get details for a specific connection |
| `list_syncs` | List all syncs |
| `get_sync` | Get details for a specific sync |
| `trigger_sync` | Manually trigger a sync |
| `get_sync_status` | Get sync run status |
| `list_models` | List all data models |
| `get_model` | Get model details and fields |
| `list_bulk_syncs` | List all bulk syncs |
| `trigger_bulk_sync` | Trigger a bulk sync |

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
