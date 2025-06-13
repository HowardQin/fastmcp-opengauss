import os
import argparse
import logging
import httpx
from fastmcp import FastMCP
from dotenv import load_dotenv
import psycopg2
from psycopg2 import Error, sql, connect

#log config for this file
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("[OPENGAUSE_MCP]")

# load environment variable from .env
load_dotenv()

# get openGauss connection parameters from env,
# everytime a resource or tool function called, 
# this will be used to open a connection.
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

# create a FastMCP 2.0 object
mcp = FastMCP(name="opengauss-fastmcp-server",
              instructions="""
              This server provides tools to 
              execute SQL to opengauss database.
              """)

# called by client, 
# return schema "public" and "$user" if availiable
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
                cursor.execute("""
                               SELECT nspname AS schema_name 
                               FROM pg_namespace 
                               WHERE nspname in ('public', '{}');
                               """
                              .format(config["user"]))
                schemas = cursor.fetchall()
        result = ["Schemas in database {}:".format(config["dbname"])]  # Header
        result.extend([sch[0] for sch in schemas])
        return ", ".join(result)
    except Error as e:
        raise RuntimeError(f"Database error: {str(e)}")

# called by client, 
# return all table names of current schema
@mcp.resource(
        uri="opengauss://tables",
        name="ListTables",
        description="""
             Get all table names in current schema,
             in qualified table names,
             of the form <schema_name>.<table_name>.
             """,
        mime_type="text/plain"
)
async def get_tables() -> str:
    """
    Get qualified table names in current schema of the form <schema_name>.<table_name>.
    """
    config = get_db_config()
    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
               cursor.execute("""SELECT schemaname, tablename 
                                 FROM pg_tables 
                                 WHERE schemaname=current_schema();
                              """)
               tables = cursor.fetchall()
        result = ["Tables in database {}:".format(config["dbname"])]  # Header
        result.extend([f"{tab[0]}.{tab[1]}" for tab in tables])
        return ", ".join(result)
    except Error as e:
        raise RuntimeError(f"Database error: {str(e)}")

# call by LLM,
# return result of any SQL command
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

# call by LLM,
# return all table names in current schema
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

# call by LLM,
# return definition of specified table
@mcp.tool()
async def get_table_definition(table: str, sch: str) -> str:
    """
         Get table definition.
    """
    config = get_db_config()
    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                                  SELECT column_name, 
                                         data_type, 
                                         column_default, 
                                         is_nullable, 
                                         ordinal_position
                                  FROM information_schema.columns
                                  WHERE table_name = '{}' and 
                                        table_schema='{}';
                               """
                               .format(table, sch));
                coldefs = cursor.fetchall()
                result = [f"Definition of table {table}:"]
                # Header
                result.extend(["column_name,data_type,column_default,is_nullable,ordinal_position"])
                result.extend([f"{col[0]},{col[1]},{col[2]},{col[3]},{col[4]}" for col in coldefs])
                return "\n".join(result)
    except Error as e:
        raise RuntimeError(f"Database error: {str(e)}")

# call by LLM,
# return return current user and schema
@mcp.tool()
async def get_current_user_and_schema() -> str:
    """
         Get current schema and current user.
    """
    config = get_db_config()
    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("select current_user, current_schema;");
                rowdata = cursor.fetchone()
                return "current user is {}, current schema is {}".format(rowdata[0], rowdata[1])
    except Error as e:
        raise RuntimeError(f"Database error: {str(e)}")

def main():
    # command line parameters passed to mcp server on starting it.
    parser = argparse.ArgumentParser(description="openGauss MCP server")
    # mcp server can talk to client in one of three protocol:
    # stdio, sse, streamable-http
    parser.add_argument(
        "--transport",
        type=str,
        choices=["stdio", "sse", "streamable-http"],
        default="stdio",
        help="Transport method (stdio, sse or streamable-http)",
    )
    # for sse or streamable-http, mcp server needs a listening port
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port this mcp server listening to",
    )
    parser.add_argument(
        "--path",
        type=str,
        default="/sse",
        help="/sse for --transport=sse, /mcp for --transport=streamable-http",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Hosts this mcp server listening to",
    )
    parser.add_argument(
        "--log_level",
        type=str,
        choices=["debug", "info", "warning", "error", "critical"],
        default="debug",
        help="Log level of mcp server internal code",
    )
    args = parser.parse_args()

    # make the mcp server run
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
