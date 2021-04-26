import uvicorn
from ariadne import (
    load_schema_from_path,
    make_executable_schema,
    asgi
)

from flowsaber.server.app.resolvers import *
from flowsaber.server.database.api import DataBase

types = [query, mutation,
         agent, flow, task, flowrun,
         uuid_scalar, timestamp_scalar, json_scalar]

type_defs = load_schema_from_path('/Users/bakezq/Desktop/workspace/flowsaber/flowsaber/server/app/graphql_schema')
schema = make_executable_schema(type_defs, *types)

app = asgi.GraphQL(schema)


if __name__ == "__main__":
    uvicorn.run(app=app)
