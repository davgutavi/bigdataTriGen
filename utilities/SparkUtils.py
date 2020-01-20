from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
import configparser

EMPTY_VALUE = ""

class SparkUtils:

    initPath = "SparkSessionPorperties.ini"
    singleton = None

    def __new__(cls, **kwargs):
        if not cls.singleton:
            cls.singleton = object.__new__(SparkUtils)
        return cls.singleton

    def __init__(self):

        self.initPath

            config = configparser.ConfigParser()
            config.read(initPath)
            appName = config["DEFAULT"]["appName"]
            master = config["DEFAULT"]["master"]
            localDir = config["DEFAULT"]["spark.local.dir"]
            checkPointDir = config["DEFAULT"]["checkPointDir"]
            logLevel = config["DEFAULT"]["log.level"]
            timeout = config["OTHERS"]["spark.network.timeout"]
            heartbeatInterval = config["OTHERS"]["spark.executor.heartbeatInterval"]
            executorMemory = config["OTHERS"]["spark.executor.memory"]
            driverMemory = config["OTHERS"]["spark.driver.memory"]
            coresMax = config["OTHERS"]["spark.cores.max"]
            executorUri = config["OTHERS"]["spark.executor.uri"]
            self.session = SparkSession.builder.master(master).appName(appName).getOrCreate()
            self.session.sparkContext.setLogLevel(logLevel)
            if localDir != EMPTY_VALUE:          self.session.conf.set("spark.local.dir", localDir)
            if checkPointDir != self.EMPTY_VALUE:     self.session.sparkContext.setCheckpointDir(checkPointDir)
            if timeout != self.EMPTY_VALUE:           self.session.conf.set("spark.network.timeout", timeout)
            if heartbeatInterval != self.EMPTY_VALUE: self.session.conf.set("spark.executor.heartbeatInterval",heartbeatInterval)
            if executorMemory != self.EMPTY_VALUE:    self.session.conf.set("spark.executor.memory", executorMemory)
            if driverMemory != self.EMPTY_VALUE:      self.session.conf.set("spark.driver.memory", driverMemory)
            if coresMax != self.EMPTY_VALUE:          self.session.conf.set("spark.cores.max", coresMax)
            if executorUri != self.EMPTY_VALUE:       self.session.conf.set("spark.executor.uri", executorUri)
            self.sparkContext = self.session.sparkContext
            self.sqlContext = SQLContext(self.sparkContext)


# def getSparkObjects(appName: String, master: String = ""): (SparkSession, SparkContext, SQLContext) = {
#
#     Logger.getLogger("org").setLevel(Level.OFF)
#
#
# Logger.getLogger("akka").setLevel(Level.OFF)
#
# var
# config: SparkConf = null
#
# if (master.equalsIgnoreCase(""))
# {
#
#     config = new
# SparkConf().setAppName(appName)
#
# }
# else {
#
#     config = new
# SparkConf().setAppName(appName).setMaster(master)
#
# }
#
# val
# session: SparkSession = SparkSession.builder.config(config).getOrCreate()
#
# val
# sc: SparkContext = session.sparkContext
#
# val
# sql: SQLContext = session.sqlContext
#
# Tuple3(session, sc, sql)
#
# }