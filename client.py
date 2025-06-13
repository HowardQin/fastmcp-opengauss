from fastmcp import Client
import asyncio

async def interact_with_server():
    print("--- Creating Client ---")

    # Option 1: Connect to a server run via `python my_server.py` (uses stdio)
    # client = Client("my_server.py")

    # Option 2: Connect to a server run via `fastmcp run ... --transport sse --port 8080`
    client = Client("http://localhost:8000/sse") # Use the correct URL/port

    try:
        async with client:
            print("--- Client Connected ---")
            print("--- Call Tools ---")
            sql_result = await client.call_tool("execute_query", {"query": "SELECT tablename FROM pg_tables WHERE schemaname = 'app'"})
            print(f"\nexecute_query result: \n{sql_result[0].text}")

            sql_result = await client.call_tool("list_tables_in_current_schema")
            print(f"\nlist_tables_in_current_schema result:\n{sql_result[0].text}")

            sql_result = await client.call_tool("get_table_definition", {"table":"urls", "sch":"app"})
            print(f"\ntable_definition result:\n{sql_result[0].text}")

            sql_result = await client.call_tool("get_current_user_and_schema")
            print(f"\ncurrent user and schema result:\n{sql_result[0].text}")

            print("\n")
            print("--- Read Resources ---")
            result = await client.read_resource("opengauss://tables")
            print(f"\nopengauss://tables:\n{result[0].text}")

            result = await client.read_resource("opengauss://schemas")
            print(f"\nopengauss://schemas:\n{result[0].text}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("--- Client Interaction Finished ---")

if __name__ == "__main__":
    asyncio.run(interact_with_server())
