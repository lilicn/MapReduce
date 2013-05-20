# Part 1
import MapReduce
import sys
mr = MapReduce.MapReduce()

# Part 2
def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    words = value.split() 
    for w in words:
      mr.emit_intermediate(w, key)

# Part 3
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    l=[]
    for v in list_of_values:
        if v not in l:
      	    l.append(v)
    mr.emit((key, l))

# Part 4
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
