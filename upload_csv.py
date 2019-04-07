
#/Users/riteshsingh/Applications/VolSurface/downloads/nse_fo
import psycopg2
import os
from os import path
import csv

conn = psycopg2.connect(
    host='localhost',
    port=54320,
    dbname='my_database',
    user='postgres',
)
cur = conn.cursor()
#cur.execute("drop TABLE NSE_FUTURES")
cur.execute("CREATE TABLE IF NOT EXISTS NSE_FUTURES \
    (FILE_NAME varchar, \
    CONTRACT_D varchar, \
    PREVIOUS_S float, \
    OPEN_PRICE float, \
    HIGH_PRICE float, \
    LOW_PRICE float, \
    CLOSE_PRICE float, \
    SETTLEMENT float, \
    NET_CHANGE float, \
    OI_NO_CON int, \
    TRADED_QUA int, \
    TRD_NO_CON int, \
    TRADED_VAL float);")

file_path = "/Users/riteshsingh/Applications/VolSurface/downloads/nse_fo"

files = os.listdir(file_path)

def mk_int(s):
    s = s.strip()
    return int(s) if s else 0

def mk_float(s):
    s = s.strip()
    return float(s) if s else 0.0

for f in files:
    # if file is a futures
    if f.startswith("fo"):
        with open(file_path+"/"+f) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    print(f'Column names are {", ".join(row)}')
                    line_count += 1
                else:
                    cur.execute("INSERT INTO NSE_FUTURES (FILE_NAME, CONTRACT_D, PREVIOUS_S, OPEN_PRICE, HIGH_PRICE, \
                        LOW_PRICE, CLOSE_PRICE, SETTLEMENT, NET_CHANGE, OI_NO_CON, TRADED_QUA, TRD_NO_CON, \
                        TRADED_VAL) VALUES (%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", \
                        (f,row[0],mk_float(row[1]),mk_float(row[2]),mk_float(row[3]),mk_float(row[4]),mk_float(row[5]),\
                        mk_float(row[6]),mk_float(row[7]),mk_int(row[8]),mk_int(row[9]),mk_int(row[10]),mk_float(row[11])))
                    print(f'loaded row {line_count}.')
                    line_count += 1
            print(f'Processed {line_count} lines.')


cur.execute("SELECT * FROM test;")
result = cur.fetchone()
print(result)
conn.commit()
cur.close()
conn.close()
