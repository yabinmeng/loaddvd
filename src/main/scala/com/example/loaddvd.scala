package com.example

import java.util.Properties

import com.datastax.spark.connector._
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import java.sql.{Connection, DriverManager, Statement}

import com.example.loaddvd.config

object loaddvd extends App {

  val config = ConfigFactory.load("application.conf").getConfig("conf")

  /**
   * Reading source - RDBMS information
   */
  val rdbmsCfg = config.getConfig("rdbms")
  val rdbmsSrvIp = rdbmsCfg.getString("ip")
  val rdbmsSrvPort = rdbmsCfg.getString("port")
  val rdbmsUsrName = rdbmsCfg.getString("user_name")
  val srcDBName = rdbmsCfg.getString("db_name")
  val srcTblNames = rdbmsCfg.getStringList("tbl_names")
  // PostgreSQL JDBC connection URL
  val rdbmsConnUrl = "jdbc:postgresql://" + rdbmsSrvIp + ":" + rdbmsSrvPort + "/" + srcDBName


  /**
   * Reading source - DSE/C* cluster information
   */
  val dseCfg = config.getConfig("dse")
  val cassSrvIp = dseCfg.getString("contact_point_ip")
  val cassSrvPort = dseCfg.getString("contact_point_port")
  val tgtKsName = dseCfg.getString("ks_name")
  val tgtTblName = dseCfg.getString("tbl_name")
  val sparkCfg = dseCfg.getConfig("cassandra.spark")
  val sparkMasterIp = sparkCfg.getString("master_ip")
  val sparkDriverIp = sparkCfg.getString("driver_ip")
  val sparkDriverPort = sparkCfg.getInt("driver_port")
  // Spark master address URL
  val dseSparkMasterUrl = "dse://" +  sparkMasterIp + ":" + cassSrvPort


  var rdbmsConnnection:Connection = null
  var spark:SparkSession = null

  /**
   * Used to determine how many partitions are needed for parallel reading
   * from an RDBMS table
   */

  def getNumPartitionsForParallelRead( queryStmt: Statement, tblName: String, numRecPerPartition: Int ) = {
    var srcTblPkColName = ""
    var minval = 0
    var maxval = 0
    var numPartitions = 0


    // First, get the primary key column name for the specified table
    var sqlQueryStr =
      "select " +
        "kcu.table_schema, " +
        "kcu.table_name,  " +
        "tco.constraint_name, " +
        "kcu.ordinal_position as position, " +
        "kcu.column_name as key_column " +
        "from information_schema.table_constraints tco " +
        "join information_schema.key_column_usage kcu on " +
        "kcu.constraint_name = tco.constraint_name and " +
        "kcu.constraint_schema = tco.constraint_schema and " +
        "kcu.constraint_name = tco.constraint_name " +
        "where tco.constraint_type = 'PRIMARY KEY' and kcu.table_name = '" + tblName + "'"

    var resultSet = queryStmt.executeQuery(sqlQueryStr)

    // [NOTE] Partition by the first primary key column
    if ( resultSet.next() ) {
      srcTblPkColName = resultSet.getString("key_column")
    }

    // Second, read min and max primary key value (used later for calculating "numPartitions")
    if ( srcTblPkColName != null ) {
      sqlQueryStr = "select min(" + srcTblPkColName + "), max(" + srcTblPkColName + ") from " + tblName
      resultSet = queryStmt.executeQuery(sqlQueryStr)

      if (resultSet.next()) {
        minval = resultSet.getString(1).toInt
        maxval = resultSet.getString(2).toInt
        println(tblName + "." + srcTblPkColName + "(min, max) = (" + minval + ", " + maxval + ").")

        numPartitions = (maxval - minval) / numRecPerPartition + 1
      }
    }

    (srcTblPkColName, minval, maxval, numPartitions)
  }

  def exitWithCode(connection: Connection, spark: SparkSession, code: Int): Unit = {
    // Close connection to the source RDBMS
    if ( connection != null ) {
      connection.close()
    }

    // Close connection to Spark
    if ( spark != null ) {
      spark.close()
    }

    System.exit(code)
  }

  try {
    /**
     * Read from source RDBMS using JDBC driver
     */
    Class.forName("org.postgresql.Driver")
    rdbmsConnnection = DriverManager.getConnection(rdbmsConnUrl, rdbmsUsrName, "")
    val statement = rdbmsConnnection.createStatement()

    /**
     * Establish connection to Spark
     */
    val sparkConf = new SparkConf()
      .setMaster(dseSparkMasterUrl)
      .setAppName("loaddvd")
      .set("spark.cassandra.connection.host", sparkMasterIp)
      .set("spark.cassandra.connection.port", cassSrvPort)
      // -- Needed for submitting a job in client deployment mode
      //.set("spark.driver.host", sparkDriverIp)
      //.set("spark.driver.port", sparkDriverPort.toString)
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    import spark.implicits._


    if ( !srcTblNames.isEmpty ) {
      val numRecordPerPartition = config.getInt("num_record_per_partition")

      /**
       * STEP 1: Read "film" table into Spark
       */
      val filmTblName = srcTblNames.get(0)

      val (srcTblPkColName0, minval0, maxval0, numPartitions0) =
        getNumPartitionsForParallelRead(
          statement,
          filmTblName,
          numRecordPerPartition
        )
      if ( numPartitions0 == 0 ) {
        println("Failed to calculate parallel reading partitions for table \"" + filmTblName + "\"")
        exitWithCode(rdbmsConnnection, spark, 10)
      }

      // Do parallel reading
      var filmTblDF = spark.read
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", rdbmsConnUrl)
        .option("dbtable", filmTblName)
        .option("user", rdbmsUsrName)
        .option("lowerBound", minval0)
        .option("upperBound", maxval0)
        .option("numPartitions", numPartitions0)
        .option("partitionColumn", srcTblPkColName0)
        .load()
        .drop("last_update")
      //== Debug purpose ==
      //filmTblDF.printSchema()
      //filmTblDF.show(5)
      filmTblDF.createOrReplaceTempView(filmTblName)


      /**
       * STEP 2: Read "actor" table
       */
      val actorTblName = srcTblNames.get(1)
      var (srcTblPkColName1, minval1, maxval1, numPartitions1) =
        getNumPartitionsForParallelRead(
          statement,
          actorTblName,
          numRecordPerPartition
        )
      if (numPartitions1 == 0) {
        println("Failed to calculate parallel reading partitions for table \"" + actorTblName + "\"")
        //exitWithCode(rdbmsConnnection, spark, 20)
      }

      // Do parallel reading
      val actorTblDF = spark.read
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", rdbmsConnUrl)
        .option("dbtable", actorTblName)
        .option("user", rdbmsUsrName)
        .option("lowerBound", minval1)
        .option("upperBound", maxval1)
        .option("numPartitions", numPartitions1)
        .option("partitionColumn", srcTblPkColName1)
        .load()
        .drop("last_update")
      //== Debug purpose ==
      //actorTblDF.printSchema()
      //actorTblDF.show(5)
      actorTblDF.createOrReplaceTempView(actorTblName)

      /**
       * STEP 3: Read "film_actor" table
       */
      val filmActorTblName = srcTblNames.get(2)
      val (srcTblPkColName2, minval2, maxval2, numPartitions2) =
        getNumPartitionsForParallelRead(
          statement,
          filmActorTblName,
          numRecordPerPartition
        )
      if (numPartitions2 == 0) {
        println("Failed to calculate parallel reading partitions for table \"" + filmActorTblName + "\"")
        exitWithCode(rdbmsConnnection, spark, 30)
      }

      // Do parallel reading
      val filmActorTblDF = spark.read
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", rdbmsConnUrl)
        .option("dbtable", filmActorTblName)
        .option("user", rdbmsUsrName)
        .option("lowerBound", minval2)
        .option("upperBound", maxval2)
        .option("numPartitions", numPartitions2)
        .option("partitionColumn", srcTblPkColName2)
        .load()
        .drop("last_update")
      //== Debug purpose ==
      //filmActorTblDF.printSchema()
      //filmActorTblDF.show(5)
      filmActorTblDF.createOrReplaceTempView(filmActorTblName)

      /**
       * STEP 4: Combine "film", "actor", "film_actor" DataFrames together into
       *         one consolidated table
       */
      val combinedDF = spark.sql(
        "select f.*, concat(a.last_name, ', ', a.first_name) as actor_name " +
          "from film as f, actor as a, film_actor as fa " +
          "where f.film_id  = fa.film_id  and fa.actor_id = a.actor_id "
      )
      combinedDF.printSchema()

      /**
       * STEP 5: Write the combined "film_actor" table in DSE/C*
       */
      // Check if the target keyspace and table exists
      val sysSchemaTableDF = spark.read
        .cassandraFormat("tables", "system_schema")
        .load()
        .filter("table_name == '" + tgtTblName + "'")
        .filter("keyspace_name == '" + tgtKsName + "'")
      val exists = sysSchemaTableDF.count()
      println("Target table exists = " + exists)

      if ( exists == 0 ) {
        // Create a C* table (using DataFrame functions
        combinedDF.createCassandraTable(
          tgtKsName,
          tgtTblName,
          partitionKeyColumns = Some(Seq("film_id")),
          clusteringKeyColumns = Some(Seq("actor_name")))
      }

      combinedDF.write
        .cassandraFormat(tgtTblName, tgtKsName)
        .mode("append")
        .save()

      // Write to an existing C* table (using RDD function)
      // -- just for reference
      //implicit def arrayToList[A](arr: Array[A]) = arr.toList
      /*
      val colNameArray: Array[String] = srcTblDF.schema.names
      srcTblDF.rdd.saveToCassandra(
        tgtKsName,
        tgtTblName,
        SomeColumns(colNameArray.map(ColumnName(_)): _*)
      )
      */
    }
  }
  catch {
    case e : Throwable => e.printStackTrace()
  }

  exitWithCode(rdbmsConnnection, spark, 0)
}