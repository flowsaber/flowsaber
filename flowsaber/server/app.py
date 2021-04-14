import uvicorn
from ariadne import (
    load_schema_from_path,
    make_executable_schema,
    asgi
)

from .graphql.graphql import *

types = [query, mutation,
         agent, flow, task, flowrun,
         log_level, datetime_scalar]

type_defs = load_schema_from_path('graphql/schema/')
schema = make_executable_schema(type_defs, *types)

app = asgi.GraphQL(schema)

if __name__ == "__main__":
    uvicorn.run(app=app)
