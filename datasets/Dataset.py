import os
from pyspark.sql.types import *
from utilities.SparkUtils import SparkUtils
from pyspark.sql.functions import monotonically_increasing_id, lit

#sparkInitPath = "/Users/davgutavi/SparkSessionProperties.ini"
#sparkInitPath = "/home/david/SparkSessionProperties.ini"
sparkInitPath =  "/Users/davgutavi/Desktop/BigDataCluGen/SparkSessionProperties.ini"

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

        spark = SparkUtils(initPath=sparkInitPath)
        session = spark.session

        path = pathList[0]

        df = session.read \
            .option("delimiter", delimiter) \
            .option("ignoreLeadingWhiteSpace", "true") \
            .option("ignoreTrailingWhiteSpace", "true") \
            .csv(path, schema=st)

        self.dataset = df.withColumn("InstanceId", monotonically_increasing_id()).withColumn("TimeId", lit(0))

        for i in range(1, len(pathList) - 1):
            path = pathList[i]

            df = session.read \
                .option("delimiter", delimiter) \
                .option("ignoreLeadingWhiteSpace", "true") \
                .option("ignoreTrailingWhiteSpace", "true") \
                .csv(path, schema=st)

            #self.dataset = self.dataset.union(df \
                                            # .withColumn("InstanceId", monotonically_increasing_id()) \
                                            #.withColumn("TimeId", lit(i)))

            self.dataset = self.dataset.union(df.withColumn("InstanceId", monotonically_increasing_id()).withColumn("TimeId", lit(0)))


    def getTimeSlices(self, lt, indexCol=True):

        aux = self.dataset.TimeId.isin(lt)

        sl = self.dataset.columns

        if not(indexCol):
            colN = len(sl)
            del sl[colN-2]
            del sl[colN-1]

        return self.dataset.select(sl).where(aux)


    def getInstanceSlices(self, lg, indexCol=True):

        aux = self.dataset.InstanceId.isin(lg)

        sl = self.dataset.columns

        if not (indexCol):
            del sl[len(sl)-1]
            del sl[len(sl)-1]

        return self.dataset.select(sl).where(aux)

    def getTimeInstanceSlices(self, lt, lg, indexCol=True):

        aux1 = self.dataset.TimeId.isin(lt)

        aux2 = self.dataset.InstanceId.isin(lg)

        sl = self.dataset.columns

        if not (indexCol):
            del sl[len(sl)-1]
            del sl[len(sl)-1]

        return self.dataset.select(sl).where(aux1).where(aux2)


    def getTimeIntanceAtributteSlices(self, lt, lg, lc, indexCol=True):

        aux1 = self.dataset.TimeId.isin(lt)

        aux2 = self.dataset.InstanceId.isin(lg)

        sl = []
        for i in lc: sl.append(self.dataset.columns[i])

        if indexCol:
           sl.append(self.dataset.columns[len(self.dataset.columns)-2])
           sl.append(self.dataset.columns[len(self.dataset.columns)-1])

        return self.dataset.select(sl).where(aux1).where(aux2)