# Next Data Platform

Next Data Platform is a set of libraries and services that provides an integrated environment for data scientists and programmers. Components:

- nextdb: A columnar database designed to make experimentation effortless
- nextbeat: A job scheduler that automatically manages your data dependencies 

## nextdb

```python
import nextdb

# Create an in-memory nextdb instance that uses a filesystem for storage
conn = nextdb.Connection(nextdb.TableVersionsClientLocal('./data_dir'))

# Write some data
conn.write('my_table', df1)  # df1 is any pandas dataframe
# Write more data
conn.write('my_table', df2)

# Query
t = conn.read('my_table')
data = t[t['date_column'].between(
    pd.Timestamp('2011-01-01'), pd.Timestamp('2011-02-01'))].to_pd()
```
