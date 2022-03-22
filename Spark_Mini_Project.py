from pyspark import SparkContext,SparkConf
from operator import add


def extract_make_key_value(vector):
    if vector[0] == 'A':
        rdd = (vector[1], vector[2]), 1
    else:
        rdd = None
    return rdd


def populate_make(entry):
    x = []
    line = list(entry)
    for n in line:
        s = n[0]
        if len(n[1]) > 0:
            make, model = n[1], n[2]
        
        x.append((s, make, model))
    return x


def extract_vin_key_value(line):
    sr = line.split(',')
    return (sr[2]), (sr[1], sr[3], sr[5]),


conf = SparkConf().setAppName('InnoWatts').setMaster('local')
sc = SparkContext(conf=conf)

raw_rdd = sc.textFile('data.csv')
vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))

vin_kv.collect()
enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))

make_kv = enhance_make.map(lambda x: extract_make_key_value(x))
make_kv = make_kv.filter(lambda x: x is not None)
make_kv.collect()

accident = make_kv.reduceByKey(add)
print(accident.collect())

