import meadowrun.context

if __name__ == "__main__":
    print(f"hello there: {meadowrun.context.variables().get('foo', 'no_data')}")
