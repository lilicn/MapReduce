import sys
import MapReduce
# Part 1
mr = MapReduce.MapReduce()

# Part 2
def mapper(record):
    # key: document identifier
    # value: document contents
    m = record[0]
    i = record[1]
    j = record[2]
    v = record[3]
    if m == 'a':
        for k in range(5):           
            mr.emit_intermediate((i,k),record)
    elif m == 'b':
        for k in range(5):
            mr.emit_intermediate((k,j),record)

# Part 3
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    #nk=key.split(",")
    al={}
    bl={}
    for v in list_of_values:
        if v[0]=='a':
            al[v[2]]=v[3]
        elif v[0]=="b":
            bl[v[1]]=v[3]
    r = 0
    for k in range(5):
        if k in al and k in bl:
            r += al[k]*bl[k]
    #newr = [nk[0],nk[1],r]
    n=[]
    n.append(key[0])
    n.append(key[1])
    n.append(r)
    mr.emit(tuple(n))

# Part 4
inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
