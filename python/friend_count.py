import MapReduce
import sys
# Part 1
mr = MapReduce.MapReduce()

# Part 2
def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    mr.emit_intermediate(key,value)

# Part 3
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    hm={}
    for v in list_of_values:
        hm[v]=0
    mr.emit((key, len(hm)))

# Part 4
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
