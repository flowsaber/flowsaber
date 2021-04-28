import asyncio

import fire


class Cli:
    TEST_DB_URL = 'mongodb+srv://admin:admin@flowsaber.bkirk.mongodb.net/flowsaber?retryWrites=true&w=majority'

    def server(self, url: str = TEST_DB_URL, **kwargs):
        """

        Parameters
        ----------
        url: str
            The mongodb url. default: TEST_DB_URL
        kwargs: dict
            Options send to uvicorn.run()  for example: --host=xxx --port=xxx

        Returns
        -------

        """
        import uvicorn
        from flowsaber.server.app.app import get_app
        from flowsaber.server.database.db import DataBase
        # test connection
        DataBase.test_connection(url)
        # get awgi app
        db = DataBase(url)
        app = get_app(db)
        uvicorn.run(app=app, **kwargs)

    def agent(self, server: str, id: str = None, name: str = None, labels: list = None):
        """

        Parameters
        ----------
        server: url
            The url of flowsaber server
        id
        name
        labels

        Returns
        -------

        """
        import flowsaber
        from flowsaber.client.agent import Agent
        flowsaber.context.logger.info(f"Server: {server}")
        agent = Agent(server, id, name, labels)
        flowsaber.context.logger.info(f"The agent is: Agent(id={agent.id}, name={agent.name}, labels={agent.labels})")
        flowsaber.context.logger.info(f"Stating agent ....")
        asyncio.run(agent.start())


if __name__ == '__main__':
    fire.Fire(Cli)
