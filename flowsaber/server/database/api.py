from motor.motor_asyncio import AsyncIOMotorClient


def get_db(url: str, db_name: str = "flowsaber"):
    client = AsyncIOMotorClient(url)
    db = client[db_name]

    return db


class DataBase(object):
    def __init__(self, url: str, db_name: str = "flowsaber"):
        self.url = url
        self.db_name = db_name
        self.db = None

    def __getattr__(self, item):
        if self.db is None:
            self.db = get_db(self.url, self.db_name)
        return getattr(self.db, item)
