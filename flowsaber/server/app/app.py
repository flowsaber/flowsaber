from pathlib import Path

from ariadne import (
    load_schema_from_path,
    make_executable_schema,
    asgi
)
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

from flowsaber.server.app.resolvers import get_resolvers
from flowsaber.server.database.db import DataBase

SCHEMA_PATH = Path(__file__).parent.resolve() / "graphql_schema"


def logging_post_data(resolver, obj, info, **kwargs):
    if obj is None:
        print(resolver, kwargs)
    return resolver(obj, info, **kwargs)


def resolve_db(db) -> 'DataBase':
    from flowsaber.server.database.db import DataBase
    if not db:
        import os
        db_uri = os.getenv('DB_URI', None)
        if not db_uri:
            raise ValueError("Please use ENV $DB_URI to specify the database connection url")
        # get awgi app
        db = DataBase(db_uri)
    assert isinstance(db, DataBase)

    return db


def get_app(db: DataBase = None):
    db = resolve_db(db)
    resolvers = get_resolvers(db)
    types = [resolvers[k] for k in ['query', 'mutation',
                                    'agent', 'flow', 'task', 'flowrun',
                                    'uuid_scalar', 'timestamp_scalar', 'json_scalar']]
    type_defs = load_schema_from_path(SCHEMA_PATH)
    schema = make_executable_schema(type_defs, *types)
    graphql = asgi.GraphQL(schema, middleware=[logging_post_data], debug=True)

    middleware = [
        Middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"])
    ]
    app = Starlette(debug=True, middleware=middleware)
    app.mount("/graphql", graphql)

    return app


app = get_app()
