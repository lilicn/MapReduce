import sys
import MapReduce
# Part 1
mr = MapReduce.MapReduce()

# Part 2
def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    mr.emit_intermediate(key,value)
    mr.emit_intermediate(value,key)

# Part 3
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    friends={}
    for v in list_of_values:
        if v in friends:
      del friends[v]
        else:
	    friends[v]=0
    for f in friends:
        mr.emit((key, f))

# Part 4
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
