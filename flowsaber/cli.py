import asyncio

import fire


class CLI:
    TEST_DB_URL = 'mongodb+srv://admin:admin@flowsaber.bkirk.mongodb.net/flowsaber?retryWrites=true&w=majority'

    def server(self, url: str = TEST_DB_URL, **kwargs):
        """

        Parameters
        ----------
        url: str
            The mongodb url. default: 127.0.0.1:27017
        kwargs: dict
            Options send to uvicorn.run()  for example: --host=xxx --port=xxx

        Returns
        -------

        """
        import uvicorn
        import os
        from flowsaber.server.database.db import DataBase
        DataBase.test_connection(url)

        os.environ['DB_URI'] = url
        uvicorn.run("flowsaber.server.app.app:app", **kwargs)

    def agent(self, server: str, id: str = None, name: str = None, labels: list = None):
        """

        Parameters
        ----------
        server: str, optional
            The url of flowsaber server, You can run `flowsaber server MONGODB_URL` to get a server address.
        id: str
            The id of the current agent.
        name: str, optional
            The name of the current agent:
        labels: list, optional
            The labels attached to the current agent.

        Returns
        -------

        """
        import flowsaber
        from flowsaber.client.agent import Agent
        flowsaber.context.logger.info(f"Server: {server}")
        agent = Agent(server, id, name, labels)
        flowsaber.context.logger.info(f"The agent is: Agent(id={agent.id}, name={agent.name}, labels={agent.labels})")
        flowsaber.context.logger.info(f"Starting agent ....")
        asyncio.run(agent.start())


if __name__ == '__main__':
    fire.Fire(CLI)
