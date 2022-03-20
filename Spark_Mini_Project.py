from pyspark import SparkContext,SparkConf
from operator import add

def extract_make_key_value(vector):
    rdd = 0
    if vector[0] == 'A':
        rdd = (vector[1], vector[2]), 1
    else:
        rdd = None
    return rdd


def populate_make(line):
    i = 0
    make=''
    model=''
    x=[]
    for n in line:
        for m in n:
            i += 1
            if i==1:
                s=m
            if i == 2:
                if len(m)>0:
                    make = m
            if i==3:
                if len(m)>0:
                    model= m
                    x.append((s,make,model))
                else:
                    x.append((s,make,model))
        else:
            i = 0
    return x


def extract_vin_key_value(line):
    sr = line.split(',')
    return (sr[2]), (sr[1], sr[3], sr[5]),

conf=SparkConf().setAppName('InnoWatts').setMaster('local')
sc=SparkContext(conf=conf)

raw_rdd = sc.textFile('data.csv')
vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))

vin_kv.collect()
enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))

make_kv = enhance_make.map(lambda x: extract_make_key_value(x))
make_kv=make_kv.filter(lambda x: x is not None)
make_kv.collect()

accident=make_kv.reduceByKey(add)
print(accident.collect())

