from datasets.Dataset import Dataset
from utilities.SparkUtils import SparkUtils


#rootPath = "/home/david/Escritorio/microarray/0002"
#rootPath = "/Users/davgutavi/Desktop/test/0002"
rootPath = "/Users/davgutavi/Desktop/BigDataCluGen/Datasets/elutriation/"


# dt = Dataset(rootPath)
# dt.dataset.show(5)
# st = dt.getTimeIntanceAtributteSlices([0, 1, 2], [0, 3, 10, 11], [0, 2])
# st.show(20)
# t1 = dt.getTimeInstanceSlices([0, 1, 2], [0, 3, 10, 11], indexCol=True)
# t1.show(20)
sp = SparkUtils()



df = sp.session.read \
            .option("delimiter", ";") \
            .option("ignoreLeadingWhiteSpace", "true") \
            .option("ignoreTrailingWhiteSpace", "true") \
            .csv(rootPath)

df.show(5)

#l = [1,2,3,4,5]
#print(*l)
#del l[len(l)-1]
#print(*l)

# nc = len(microarray.columns)
# gi = nc - 1
# ti = nc - 2
# microRdd = microarray.rdd
# rddMapeado = microRdd.keyBy(lambda r: (r[ti],r[gi]))
# lt = [0]
# lg = [0, 2, 4]
# mapedTimesGenes = rddMapeado.filter(lambda f: f[0][0] in lt).filter(lambda f: f[0][1] in lg)
# print(*mapedTimesGenes.collect(),sep="\n")
# ci = [1,4,5]
# def f1 (r:Row,ci:list):
#     l = []
#     for e in ci:
#          l.append(r[e])
#     return l
# msl = mapedTimesGenes.map(lambda n: f1(n[1],ci))
# print(*msl.collect(),sep="\n")