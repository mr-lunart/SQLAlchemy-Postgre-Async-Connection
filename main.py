from datetime import datetime
import asyncio

from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# import the singleton directly
from DBSession import SESSION_MAKER

app = FastAPI()

async def get_info(conn: AsyncSession) -> dict:
    sql = """
    SELECT 
        pg_sleep(10),
        inet_server_addr()  AS server_ip, 
        inet_server_port()  AS server_port,
        current_database()  AS database_name,
        current_user        AS user_name;
    """
    try:
        result = await conn.execute(text(sql))
        row = result.fetchone()
        return {
            "server_ip":     str(row.server_ip),
            "server_port":   row.server_port,
            "database_name": row.database_name,
            "user_name":     row.user_name,
        }
    except Exception as err:
        await conn.rollback()
        raise err

@app.get("/info")
async def slow():
    start = datetime.now()
    print(f"start at: {start}")
    async with SESSION_MAKER.session() as conn:
        print(SESSION_MAKER._engine.pool.status())
        try:
            data = await get_info(conn=conn)
        except:
            data = None
            # do something here
            pass
    end = datetime.now()
    delta = end - start
    print(f"end at: {end}")
    return {"data": data, "elapsed_seconds": delta.total_seconds()}