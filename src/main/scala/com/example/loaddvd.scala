package com.example

import java.util.Properties

import com.datastax.spark.connector._
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import java.sql.{Connection, DriverManager, Statement}

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
  val srcTblName = rdbmsCfg.getString("tbl_name")
  // PostgreSQL JDBC connection URL
  val rdbmsConnUrl = "jdbc:postgresql://" + rdbmsSrvIp + ":" + rdbmsSrvPort + "/" + srcDBName;


  /**
   * Reading source - DSE/C* cluster information
   */
  val cassCfg = config.getConfig("cassandra")
  val cassSrvIp = cassCfg.getString("contact_point_ip")
  val cassSrvPort = cassCfg.getString("contact_point_port")
  val tgtKsName = cassCfg.getString("ks_name")
  val tgtTblName = cassCfg.getString("tbl_name")
  val sparkCfg = config.getConfig("cassandra.spark")
  val sparkMasterIp = sparkCfg.getString("master_ip")
  val sparkDriverIp = sparkCfg.getString("driver_ip")
  val sparkDriverPort = sparkCfg.getInt("driver_port")
  // Spark master address URL
  val dseSparkMasterUrl = "dse://" +  sparkMasterIp + ":" + cassSrvPort


  var rdbmsConnnection:Connection = null
  var spark:SparkSession = null

  try {
    /**
     * Read from source RDBMS using JDBC driver
     */
    Class.forName("org.postgresql.Driver")
    rdbmsConnnection = DriverManager.getConnection(rdbmsConnUrl, rdbmsUsrName, "")
    val statement = rdbmsConnnection.createStatement();

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
        "where tco.constraint_type = 'PRIMARY KEY' and kcu.table_name = '" + srcTblName + "'"

    var resultSet = statement.executeQuery(sqlQueryStr)
    var srcTblPkColName = ""

    // NOTE: for this test, the source table only has a single-column primary key
    resultSet.next()
    srcTblPkColName = resultSet.getString("key_column")

    // Second, read min and max primary key value (used later for calculating "numPartitions")
    if ( !srcTblName.isEmpty ) {
      sqlQueryStr = "select min(" + srcTblPkColName + "), max(" + srcTblPkColName + ") from " + srcTblName;
      resultSet = statement.executeQuery(sqlQueryStr)

      resultSet.next()
      val minval = resultSet.getString(1).toInt
      val maxval = resultSet.getString(2).toInt

      println(srcTblName + "." +srcTblPkColName + "(min, max) = (" + minval + ", " + maxval + ").\n" )


      /**
       * Write to DSE/C* using Spark
       */
      // Establish connection to Spark
      val sparkConf = new SparkConf()
        .setMaster(dseSparkMasterUrl)
        .setAppName("loadfilm")
        .set("spark.cassandra.connection.host", sparkMasterIp)
        .set("spark.cassandra.connection.port", cassSrvPort)
        // Needed for standalone-master
        .set("spark.driver.host", sparkDriverIp)
        .set("spark.driver.port", sparkDriverPort.toString)

      val spark = SparkSession
        .builder()
        .config(sparkConf)
        .getOrCreate()

      import spark.implicits._

      // Do parallel reading
      val numPartitions = (maxval - minval) / config.getInt("num_record_per_parition")

      var srcTblDF = spark.read
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", rdbmsConnUrl)
        .option("dbtable", srcTblName)
        .option("user", rdbmsUsrName)
        .option("lowerBound", minval)
        .option("upperBound", maxval)
        .option("numPartitions", numPartitions)
        .option("partitionColumn", srcTblPkColName)
        .load()

      // Drop the standard "last_update" column
      srcTblDF = srcTblDF.drop("last_update")

      //== Debug purpose ==
      srcTblDF.printSchema()
      srcTblDF.show(5)

      // Create a C* table

      // NOTE: Having issues with DataFrame related functions
      //       Don't know the solution yet; actively investigating
      // E.g. one error when creating a C* table using DF function is as below
      //    java.lang.NoSuchMethodError: 'com.datastax.spark.connector.DataFrameFunctions com.datastax.spark.connector.package$.toDataFrameFunctions(org.apache.spark.sql.Dataset)'
      /*
      srcTblDF.createCassandraTable(
        tgtKsName,
        tgtTblName,
        partitionKeyColumns = Some(Seq(srcTblPkColName)))

      srcTblDF.write
        .cassandraFormat(tgtKsName, tgtTblName)
        //.mode("append")
        .save()
      */

      // Write to an existing C* table
      //implicit def arrayToList[A](arr: Array[A]) = arr.toList
      val colNameArray: Array[String] = srcTblDF.schema.names
      srcTblDF.rdd.saveToCassandra(
        tgtKsName,
        tgtTblName,
        SomeColumns(colNameArray.map(ColumnName(_)): _*)
      )
    }
  }
  catch {
    case e : Throwable => e.printStackTrace()
  }

  // Close connection to the source RDBMS
  if ( rdbmsConnnection != null ) {
    rdbmsConnnection.close()
  }

  // Close connection to Spark
  if ( spark != null ) {
    spark.close()
  }

  System.exit(0)
}