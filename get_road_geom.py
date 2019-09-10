from sql_utils import query
from shapely.wkb import loads

# First read csv

import csv

rows = []

with open("results-49998.csv") as f:
    csvreader = csv.reader(f)
    header = next(csvreader, None)
    print(header)
    for row in csvreader:
        rows.append(row)

# Benchmark how long it takes to make a query

import time

BATCH_SIZE = 10000
NUM_ROWS = len(rows)

pointer = 0

# Break up the rows

link_ids = [x[0] for x in rows]
#print(len(link_ids))

all_start = time.time()
while pointer < NUM_ROWS:
    start = time.time()
    link_ids_subset = link_ids[pointer:pointer+BATCH_SIZE]
    #.print(pointer, pointer+BATCH_SIZE)
    #print(len(link_ids_subset))
    array_string = ", ".join(str(i) for i in link_ids_subset)
    array_string = f"({array_string})"
    query_string = f"SELECT geom from (SELECT DISTINCT(link_id), geom FROM streets WHERE link_id IN {array_string}) as f"
    #print(query_string)

    # Send out the query
    query_result = (query(query_string))
    for i in range(pointer, min(pointer+BATCH_SIZE,len(rows))):
        rows[i].append(query_result[i-pointer][0])
        #print(rows[i])

    end=time.time()
    print(f"Time taken to do {pointer}:{pointer+BATCH_SIZE}: {end-start}")

    pointer += BATCH_SIZE

all_end = time.time()

print(f"Time taken to make all the queries: {all_end-all_start}")

header.append("geom")

with open("results-49998-with-geom.csv", "w") as f:
    f.write(", ".join(str(i) for i in header))
    f.write('\n')

with open("results-49998-with-geom.csv", "a") as f:
    for row in rows[:NUM_ROWS]:
        f.write(", ".join(str(i) for i in row))
        f.write('\n')

