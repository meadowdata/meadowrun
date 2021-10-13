import nextbeat.context


if __name__ == "__main__":
    print(f"hello there: {nextbeat.context.variables().get('foo', 'no_data')}")
