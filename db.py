# Docker image with postgre sql created using instruction from
#https://www.saltycrane.com/blog/2019/01/how-run-postgresql-docker-mac-local-development/
#python3 -m venv venv
#source venv/bin/activate
#pip install psycopg2-binary
#python db.py
import psycopg2

conn = psycopg2.connect(
    host='localhost',
    port=54320,
    dbname='my_database',
    user='postgres',
)
cur = conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS test (id serial PRIMARY KEY, num integer, data varchar);")
cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (100, "abcdef"))
cur.execute("SELECT * FROM test;")
result = cur.fetchone()
print(result)
conn.commit()
cur.close()
conn.close()

