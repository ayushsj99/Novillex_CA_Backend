from sqlalchemy import select
from database.db import engine, user_table_metadata, users
from sqlalchemy.orm import Session

def get_metadata_by_user_id(user_id: int):
    with Session(engine) as session:
        stmt = select(user_table_metadata).where(user_table_metadata.c.user_id == user_id)
        result = session.execute(stmt).fetchall()
        return [dict(row._mapping) for row in result]

def get_metadata_by_table_hash_id(table_hash_id: int):
    with Session(engine) as session:
        stmt = select(user_table_metadata).where(user_table_metadata.c.table_hash_id == table_hash_id)
        result = session.execute(stmt).fetchall()
        return [dict(row._mapping) for row in result]

def get_metadata_by_user_and_hash(user_id: int, table_hash_id: int):
    with Session(engine) as session:
        stmt = select(user_table_metadata).where(
            user_table_metadata.c.user_id == user_id,
            user_table_metadata.c.table_hash_id == table_hash_id
        )
        result = session.execute(stmt).fetchall()
        return [dict(row._mapping) for row in result]
    
def get_user_id_by_username(username: str):
    with Session(engine) as session:
        stmt = select(users.c.id).where(users.c.username == username)
        result = session.execute(stmt).fetchone()
        return result[0] if result else None
