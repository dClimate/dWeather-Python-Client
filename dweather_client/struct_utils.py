def tupleify(args):
    if isinstance(args, tuple):
        return args
    return (args,)