import os
import argparse
import logging
import httpx
from fastmcp import FastMCP
from dotenv import load_dotenv
import psycopg2
from psycopg2 import Error, sql, connect

load_dotenv()

default_log_level = os.environ.get("LOG_LEVEL", "DEGUG").upper()
log_level = getattr(logging, default_log_level, logging.INFO)
logging.basicConfig(level=log_level)
logger = logging.getLogger("[OPENGAUSE_MCP]")

def get_db_config():
    """Get database configuration from environment variables."""
    config = {
        "host": os.getenv("OPENGAUSS_HOST", "localhost"),
        "port": int(os.getenv("OPENGAUSS_PORT", "5432")), 
        "user": os.getenv("OPENGAUSS_USER"),
        "password": os.getenv("OPENGAUSS_PASSWORD"),
        "dbname": os.getenv("OPENGAUSS_DBNAME"),
    }
    if not all([config["user"], config["password"], config["dbname"]]):
        raise ValueError("Missing required database configuration")

    return config

mcp = FastMCP(name="opengauss-mcp-server",
                           instructions="""
                                                 This server provides tools to execute SQL to opengauss database.
                                                 """
                          )

@mcp.resource(
        uri="opengauss://schemas",
        name="ListSchemas",
        description="Get all schemas of the database.",
        mime_type="text/plain"
)
async def get_schemas() -> str:
    """
    Get all schemas of the database..
    """
    config = get_db_config()
    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
               cursor.execute("SELECT nspname AS schema_name FROM pg_namespace where nspname in ('public', '{}');".format(config["user"]))
               schemas = cursor.fetchall()
        result = ["Schemas in database {}:".format(config["dbname"])]  # Header
        result.extend([sch[0] for sch in schemas])
        return ", ".join(result)

    except Error as e:
        raise RuntimeError(f"Database error: {str(e)}")

@mcp.resource(
        uri="opengauss://tables",
        name="ListTables",
        description="Get all table names under current schema in qualified table names of the form <schema_name>.<table_name>.",
        mime_type="text/plain"
)
async def get_tables() -> str:
    """
    Get qualified table names of public and user's schema in the form of: <schema_name>.<table_name>.
    """
    config = get_db_config()
    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
               cursor.execute("SELECT schemaname, tablename FROM pg_tables WHERE schemaname in ('public', '{}');".format(config["user"]))
               tables = cursor.fetchall()
        result = ["Tables in database {}:".format(config["dbname"])]  # Header
        result.extend([f"{tab[0]}.{tab[1]}" for tab in tables])
        return ", ".join(result)

    except Error as e:
        raise RuntimeError(f"Database error: {str(e)}")

@mcp.resource(
        uri="opengauss://table_definitions",
        name="TableDefinitions",
        description="Get definitions of all tables in current schema.",
        mime_type="text/plain"
)
async def get_table_definitions() -> str:
    """
    Get definitions of all tables in current schema.
    """
    config = get_db_config()
    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname = current_schema();")
                tables = cursor.fetchall()
                all_defs=[]
                for tup in tables:
                    cursor.execute("""SELECT column_name, data_type, column_default, is_nullable, ordinal_position
                                                 FROM information_schema.columns
                                                 WHERE table_name = '{}' and table_schema=current_schema();""".format(tup[0]));
                    coldefs = cursor.fetchall()
                    result = [f"Definition of table {tup[0]}:"]
                    result.extend(["column_name, data_type, column_default, is_nullable, ordinal_position"])  # Header
                    result.extend([f"{col[0]},{col[1]},{col[2]},{col[3]},{col[4]}" for col in coldefs])
                    all_defs.append("\n".join(result))
                return "\n".join(all_defs)

    except Error as e:
        raise RuntimeError(f"Database error: {str(e)}")

@mcp.tool()
async def execute_query(query: str) -> str:
    """Execute an SQL commands on the openGauss server.

    Args:
        query: SQL command
    """
    config = get_db_config()
    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                # Execute regular SQL queries
                cursor.execute(query)
                # Regular SELECT queries
                if query.strip().upper().startswith("SELECT"):
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    result = [",".join(map(str, row)) for row in rows]
                    return "\n".join([",".join(columns)] + result)
                # Non-SELECT queries
                else:
                    conn.commit()
                    return f"Query executed successfully. Rows affected: {cursor.rowcount}"
    except Error as e:
        logger.error(f"Error executing SQL '{query}': {e}")
        return f"Error executing query: {str(e)}"

@mcp.tool()
async def list_tables_in_current_schema() -> str:
    """
        List table names in current schema on the openGauss server.
    """
    config = get_db_config()
    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
               cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname = current_schema();")
               tables = cursor.fetchall()
        result = ["Tables in current schema:"]  # Header
        result.extend([tab[0] for tab in tables])
        return "\n".join(result)

    except Error as e:
        raise RuntimeError(f"Database error: {str(e)}")

@mcp.tool()
async def get_table_definition(table: str, sch: str) -> str:
    """
         Get table definition.
    """
    config = get_db_config()
    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""SELECT column_name, data_type, column_default, is_nullable, ordinal_position
                                             FROM information_schema.columns
                                             WHERE table_name = '{}' and table_schema='{}';""".format(table, sch));
                coldefs = cursor.fetchall()
                result = [f"Definition of table {table}:"]
                result.extend(["column_name, data_type, column_default, is_nullable, ordinal_position"])  # Header
                result.extend([f"{col[0]},{col[1]},{col[2]},{col[3]},{col[4]}" for col in coldefs])
                return "\n".join(result)

    except Error as e:
        raise RuntimeError(f"Database error: {str(e)}")

@mcp.tool()
async def get_current_user_and_schema() -> str:
    """
         Get current schema and current user.
    """
    config = get_db_config()
    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("select current_user, current_schema;;");
                rowdata = cursor.fetchone()
                return "current user is {}, current schema is {}".format(rowdata[0], rowdata[1])

    except Error as e:
        raise RuntimeError(f"Database error: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="openGauss MCP server")
    parser.add_argument(
        "--transport",
        type=str,
        choices=["stdio", "sse", "streamable-http"],
        default="stdio",
        help="Transport method (stdio, sse or streamable-http)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port",
    )
    parser.add_argument(
        "--path",
        type=str,
        default="/mcp",
        help="Path",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Path",
    )
    parser.add_argument(
        "--log_level",
        type=str,
        choices=["debug", "info", "warning", "error", "critical"],
        default="debug",
        help="Path",
    )
    args = parser.parse_args()

    if args.transport == "stdio":
        mcp.run(transport="stdio")
    else:
        mcp.run(
            transport=args.transport,
            host=args.host,
            port=args.port,
            log_level=args.log_level,
            path=args.path
        )

if __name__ == "__main__":
    main()
