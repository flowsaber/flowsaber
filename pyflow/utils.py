def extend_method(cls):
    def set_method(obj):
        import inspect

        funcs = []
        if inspect.isclass(obj):
            for name, func in inspect.getmembers(obj):
                if not name.startswith('__'):
                    funcs.append(func)
        elif inspect.isfunction(obj):
            funcs.append(obj)
        else:
            raise ValueError("Should be a class or function")

        for func in funcs:
            setattr(cls, func.__name__, func)

    return set_method
