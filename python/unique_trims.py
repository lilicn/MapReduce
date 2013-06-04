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
    newKey = value[:-10]
    print "new key" + newKey
    mr.emit_intermediate(newKey,key)

# Part 3
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    mr.emit(key)

# Part 4
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
