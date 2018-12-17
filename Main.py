from datasets.Dataset import Dataset


rootPath = "/home/david/Escritorio/microarray/0002/"

dt = Dataset(rootPath)

st = dt.getTGCslices([0,1,2], [0,3,10,11],[0,2,4,5],indexCol=True)

st.show(20)


t1 = dt.getTGslices([0,1,2], [0,3,10,11],indexCol=False)

t1.show(20)


l = [1,2,3,4,5]

print(*l)

del l[len(l)-1]

print(*l)


#
# nc = len(microarray.columns)

# gi = nc - 1
# ti = nc - 2
#
# microRdd = microarray.rdd
#
# rddMapeado = microRdd.keyBy(lambda r: (r[ti],r[gi]))
#
# lt = [0]
# lg = [0, 2, 4]
#
# mapedTimesGenes = rddMapeado.filter(lambda f: f[0][0] in lt).filter(lambda f: f[0][1] in lg)
#
# # print(*mapedTimesGenes.collect(),sep="\n")
#
# ci = [1,4,5]
#
#
# def f1 (r:Row,ci:list):
#     l = []
#     for e in ci:
#          l.append(r[e])
#     return l
#
# msl = mapedTimesGenes.map(lambda n: f1(n[1],ci))
#
# print(*msl.collect(),sep="\n")
#
#
