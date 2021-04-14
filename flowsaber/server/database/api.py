from motor.motor_asyncio import AsyncIOMotorClient


def get_db(url: str, db_name: str = "flowsaber"):
    client = AsyncIOMotorClient(url)
    db = client[db_name]

    return db
