import timeit


def timeit_decorator(executions=100):
    """
    A decorator to measure the average execution time of a function over a set number of executions.
    Accepts a parameter for the number of executions to average over.
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            time = timeit.timeit(lambda: func(*args, **kwargs), number=executions)
            print(f"Average execution time over {executions} executions: {time / executions} seconds")
            return func(*args, **kwargs)

        return wrapper

    return decorator
