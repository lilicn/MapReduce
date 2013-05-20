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
    print key+","+value +","+record[2]
    mr.emit_intermediate(value,record)

# Part 3
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    ll=[]
    ol=[]
    for v in list_of_values:
  print key+":"+"v:" + v[0]+","+v[1]
        if v[0]=="line_item":
            ll.append(v)
	elif v[0]=="order":
	    ol.append(v)
    if len(ll)>0 and len(ol)>0:
        for o in ol:
            for l in ll:
		x=[]
		x.extend(o)
		x.extend(l)
		mr.emit(x)

# Part 4
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
