from pathlib import Path

from ariadne import (
    load_schema_from_path,
    make_executable_schema,
    asgi
)

from flowsaber.server.app.resolvers import get_resolvers
from flowsaber.server.database.db import DataBase

SCHEMA_PATH = Path(__file__).parent.resolve() / "graphql_schema"


def logging_post_data(resolver, obj, info, **kwargs):
    if obj is None:
        print(resolver, kwargs)
    return resolver(obj, info, **kwargs)


def get_app(db: DataBase):
    assert isinstance(db, DataBase)
    resolvers = get_resolvers(db)
    types = [resolvers[k] for k in ['query', 'mutation',
                                    'agent', 'flow', 'task', 'flowrun',
                                    'uuid_scalar', 'timestamp_scalar', 'json_scalar']]
    type_defs = load_schema_from_path(SCHEMA_PATH)
    schema = make_executable_schema(type_defs, *types)
    app = asgi.GraphQL(schema, middleware=[logging_post_data])
    return app
