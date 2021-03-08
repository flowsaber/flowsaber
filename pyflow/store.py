from typing import List

flow_stack: List['Flow'] = []


def get_up_flow() -> 'Flow':
    return flow_stack[-1] if flow_stack else None


def get_top_flow() -> 'Flow':
    return flow_stack[0] if flow_stack else None


def get_flow_stack() -> List['Flow']:
    return flow_stack
