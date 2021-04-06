from ariadne import load_schema_from_path, make_executable_schema
from ariadne.asgi import GraphQL

from .graphql.graphql import query, mutation, log_level

type_defs = load_schema_from_path('graphql/schema/')
schema = make_executable_schema(type_defs, query, mutation, log_level)

app = GraphQL(schema)
