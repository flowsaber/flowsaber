from motor.motor_asyncio import AsyncIOMotorClient


def get_db(url: str, db_name: str = "flowsaber", **kwargs):
    client = AsyncIOMotorClient(url, **kwargs)
    db = client[db_name]

    return db


class DataBase(object):
    def __init__(self, url: str, db_name: str = "flowsaber", **kwargs):
        self.kwargs = kwargs
        self.url = url
        self.db_name = db_name
        self.db = None

    def __getattr__(self, item):
        if self.db is None:
            self.db = get_db(self.url, self.db_name, **self.kwargs)
        return getattr(self.db, item)

    @classmethod
    def test_connection(cls, url):
        import asyncio

        async def test():
            db = DataBase(url, serverSelectionTimeoutMS=2000)
            return await db.test_db.find_one()

        return asyncio.run(test())


if __name__ == "__main__":
    print(DataBase.test_connection('mongodb://127.0.0.1:27017'))
