import time


def example_runner(arg: str) -> str:
    print(f"example_runner called with {arg}")
    time.sleep(0.1)
    return "hello " + arg


def unique_per_deployment() -> str:
    return "embedded in main repo"
