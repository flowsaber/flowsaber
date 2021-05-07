import inspect
from typing import Any, Optional

import aiohttp
from aiohttp import ContentTypeError
from pydantic import validate_arguments


class GraphqlError(Exception):
    pass


class ServerError(Exception):
    pass


class ValidateMeta(type):
    """Metaclass that automatically wrap method with pydantic.validate_arguemnts decorator.
    """

    def __new__(cls, clsname, bases, clsdict: dict):
        for k, v in clsdict.items():
            if not k.startswith('_') and inspect.isfunction(v):
                clsdict[k] = validate_arguments(v)
        return super().__new__(cls, clsname, bases, clsdict)


class Client(object, metaclass=ValidateMeta):
    """The Graphql client used for communicating with server by sending HTTP _post reqeuests data.
    """

    def __init__(self, server_url: str, retry: int = 3):
        assert server_url and isinstance(server_url, str)
        server_url.rstrip('/')
        # must end with '/'
        # https://github.com/encode/starlette/issues/869
        self.server_url = server_url + "/graphql/"
        self.retry = retry
        self.test = True
        self.session: Optional[aiohttp.ClientSession] = None

    async def close(self):
        if self.session:
            await self.session.close()

    # json conflict with pydantic, so use underscore to pass validation
    async def _post(self, url, json=None, **kwargs) -> dict:
        if self.session is None:
            self.session = aiohttp.ClientSession()
        async with self.session.post(url, json=json, **kwargs) as rsp:
            try:
                return await rsp.json()
            except ContentTypeError as e:
                raise ServerError("The response is not in json format.") from e

    async def graphql_request(self, query: str, variables: dict, **kwargs) -> dict:
        json = {
            'query': query,
            'variables': variables
        }
        # with multiple retry
        success = False
        retry = self.retry
        error = None
        rsp = {}
        while not success and retry > 0:
            try:
                rsp = await self._post(self.server_url, json=json, **kwargs)
                success = True
            except Exception as e:
                retry -= 1
                error = e
        # raise request error
        if not success and error:
            raise error
        # raise graphql error
        if 'errors' in rsp:
            raise GraphqlError(str(rsp['errors']))
        # raise server error
        if 'success' in rsp['data'] and not rsp['data']['success']:
            raise ServerError(str(rsp['data']))

        return rsp['data']

    async def query(self, method: str, input: Any, field: str):
        res = await self.graphql('query', method, input, field)
        return res

    async def mutation(self, method: str, input: Any, field: str):
        res = await self.graphql('mutation', method, input, field)
        return res

    async def graphql(self, method_type: str, method: str, input: Any, field: str):
        input_type = type(input).__name__
        if input_type == 'str':
            input_type = "UUID"

        query = f"""
            {method_type}($input: {input_type}!) {{
                {method}(input: $input) {{
                    {field}
                }}
            }}
        """
        variables = {
            'input': input.dict() if hasattr(input, 'dict') else input
        }

        result = await self.graphql_request(query, variables)
        return result[method]
