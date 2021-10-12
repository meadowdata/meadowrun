import time


def example_runner(arg: str):
    print(f"example_runner called with {arg}")
    time.sleep(0.1)
    return "hello " + arg
