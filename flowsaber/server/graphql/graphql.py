import enum
from typing import Any, Dict, List

from ariadne import QueryType, MutationType, EnumType
from graphql import GraphQLResolveInfo

from flowsaber.server.database import api
from flowsaber.server.models import RunLog

query = QueryType()
mutation = MutationType()


@mutation.field('write_taskrun_logs')
async def resolve_write_run_logs(obj: Any, info: GraphQLResolveInfo, logs: List[Dict[str, Any]]) -> dict:
    received_logs = [
        RunLog.parse_obj(log).dict() for log in logs
    ]
    try:
        await api.write_run_logs(received_logs)
    except Exception as e:
        return {'success': False, 'error': str(e)}
    else:
        return {'success': True}


@mutation.field('update_flowrun')
async def update_flowrun_state(obj: Any, info: GraphQLResolveInfo, flowrun_state_input: Dict[str, str]) -> dict:
    pass


@mutation.field('update_taskrun')
async def a():
    pass


class LogLevel(enum.IntEnum):
    DEBUG = 1
    INFO = 2
    WARNING = 3
    ERROR = 4
    CRITICAL = 5


log_level = EnumType("LogLevel", LogLevel)
