import meadowflow.context


if __name__ == "__main__":
    print(f"hello there: {meadowflow.context.variables().get('foo', 'no_data')}")
