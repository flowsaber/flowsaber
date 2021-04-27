import fire


class Cli:

    def server(self, url: str = "mongodb://127.0.0.1:27017", **kwargs):
        """

        Parameters
        ----------
        url: str
            The mongodb url. default: mongodb://127.0.0.1:27017
        kwargs: dict
            Options send to uvicorn.run()  for example: --host=xxx --port=xxx

        Returns
        -------

        """
        import uvicorn
        from flowsaber.server.app.app import get_app
        from flowsaber.server.database.api import DataBase
        # test connection
        DataBase.test_connection(url)
        # get awgi app
        db = DataBase(url)
        app = get_app(db)
        uvicorn.run(app=app, **kwargs)

    def agent(self, server: str):
        # from flowsaber.client.agent import Agent
        pass


if __name__ == '__main__':
    fire.Fire(Cli)
