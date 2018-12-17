import os
from pyspark.sql.types import *
from utilities.SparkSessionUtils import SparkUtils
from pyspark.sql.functions import monotonically_increasing_id, lit


class Dataset:

    def __init__(self, rootPath):

        fileList = os.listdir(rootPath)

        pathList = []

        for e in fileList: pathList.append(os.path.join(rootPath, e))

        pathList.sort()

        path = pathList[0]

        f = open(path, "r")
        firsLine = f.readline()

        delimiter = ","

        firstList = firsLine.split(delimiter)
        if len(firstList) == 1:
            delimiter = ";"
            firstList = firsLine.split(delimiter)
        if len(firstList) == 1:
            delimiter = "/t"
            firstList = firsLine.split(delimiter)

        f.close()

        ncol = len(firstList)

        sfList = []

        for i in range(0, ncol - 1):
            sf = StructField(str("c" + str(i)), DoubleType(), True)
            sfList.append(sf)

        st = StructType(sfList)

        spark = SparkUtils("/home/david/SparkSessionProperties.ini")
        sc = spark.sparkContext
        session = spark.session

        path = pathList[0]

        df = session.read \
            .option("delimiter", delimiter) \
            .option("ignoreLeadingWhiteSpace", "true") \
            .option("ignoreTrailingWhiteSpace", "true") \
            .csv(path, schema=st)

        self.dataset = df. \
            withColumn("TimeId", lit(0)) \
            .withColumn("GenId", monotonically_increasing_id())

        for i in range(1, len(pathList) - 1):
            path = pathList[i]

            df = session.read \
                .option("delimiter", delimiter) \
                .option("ignoreLeadingWhiteSpace", "true") \
                .option("ignoreTrailingWhiteSpace", "true") \
                .csv(path, schema=st)

            self.dataset = self.dataset.union(df \
                                              .withColumn("TimeId", lit(i)) \
                                              .withColumn("GenId", monotonically_increasing_id()))

    def getTslicesT(self, lt, indexCol=True):

        aux = self.dataset.TimeId.isin(lt)

        sl = self.dataset.columns

        if not(indexCol):
            colN = len(sl)
            del sl[colN-2]
            del sl[colN-1]

        return self.dataset.select(sl).where(aux)


    def getGslices(self, lg, indexCol=True):

        aux = self.dataset.GenId.isin(lg)

        sl = self.dataset.columns

        if not (indexCol):
            del sl[len(sl)-1]
            del sl[len(sl)-1]

        return self.dataset.select(sl).where(aux)

    def getTGslices(self, lt,lg, indexCol=True):

        aux1 = self.dataset.TimeId.isin(lt)

        aux2 = self.dataset.GenId.isin(lg)

        sl = self.dataset.columns

        if not (indexCol):
            del sl[len(sl)-1]
            del sl[len(sl)-1]

        return self.dataset.select(sl).where(aux1).where(aux2)


    def getTGCslices(self, lt, lg, lc, indexCol=True):

        aux1 = self.dataset.TimeId.isin(lt)

        aux2 = self.dataset.GenId.isin(lg)

        colN = len(self.dataset.columns)

        sl = []
        for i in lc: sl.append(self.dataset.columns[i])

        if indexCol:
           sl.append(self.dataset.columns[colN-2])
           sl.append(self.dataset.columns[colN-1])

        return self.dataset.select(sl).where(aux1).where(aux2)