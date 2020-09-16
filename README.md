# Overview 

This repo demonstrates how to use DSE Analytics (Apach Spark) to load data from multiple RDBMS (PostgreSQL) tables; combine and massage the data a bit; and the data write data into on Cassandra (C*) table.

The program reads data from PostgreSQL tables using corresponding [JDBC driver](https://jdbc.postgresql.org/) and write data into C* table using DataStax [Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector). 

The environment setup is as below:
* OS (of all server instances): Ubuntu 16.04.7 LTS (Xenial Xerus)
* PostgresSQL server version 12.4
* DSE 6.8.3 cluster with Analytics enabled
* sbt version: 1.3.13
* scala version: 2.11.12 (**NOTE**: currently Spark Cassandra Connector is compatible wit scala 2.11.x and 2.12.x)

# Prepare PostgreSQL Server and Sample Data Set

## Install and Configure PostgreSQL server

* Run the following commands to install PostgreSQL
```
$ sudo apt update
$ sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
$ wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
$ sudo apt update
$ sudo apt -y install postgresql
```

* **Configure PostgreSQL server**

**NOTE**: the configuration change as demonstrated below is just for demo purpose of having a running PostgreSQL server that can be accessed externally with minimum hassles. 

By default, the installed PostgreSQL server only listens on localhost and doesn't allow connection from other hosts. We need to change this. There are 2 configuration files that we need to modify. Both of them are located under */etc/postgresql/12/main* folder. Go tho this folder and make the following changes:

  * **postgresql.conf**: change listen_address from 'localhost' to '*' 
  ```
  listen_addresses = '*'
  ```
  
  * **pg_hba.conf**: add the following line under IPv4 section
  ```
  # IPv4 local connections:
  host    all             all             0.0.0.0/0               trust
  ```

After making the above changes, make sure to restart PostgreSQL server. Once the server is back online, we can connect to PostgreSQL server from external, such as from the DSE cluster node. 
```
$ sudo service postgresql restart
``` 

* **Load Sample Data Set (Source)**

The sample data set we're going to use in this demo is from the following website:
https://www.postgresqltutorial.com/postgresql-sample-database/

This sample data set represents a [DVD rental ER model](https://github.com/yabinmeng/loaddvd/blob/master/src/main/resources/dvd-rental-sample-database-diagram.png)

The [sample data set](https://github.com/yabinmeng/loaddvd/blob/master/sample_dataset/dvdrental.tar) can be loaded into a PostgreSQL database named *dvdrental* using the following command:
```
$ pg_restore -U postgres -d dvdrental ./dvdrental.tar
```

## Target C* Table Schema 

In this demo, we're only going to focus on the following 3 tables:
* film
* actor
* film_actor

The **goal** is to denormalize the film data and the actor data into one consolidated C* table with some minor modification. The C* table schema looks like this:

```
CREATE TABLE testks.film_actor (
    film_id int,
    actor_name text,
    description text,
    fulltext text,
    language_id smallint,
    length smallint,
    rating text,
    release_year int,
    rental_duration smallint,
    rental_rate decimal,
    replacement_cost decimal,
    special_features list<text>,
    title text,
    PRIMARY KEY (film_id, actor_name)
)
```

**NOTE**

In this demo program, we don't need to create the above C* schema manually in-advance. The program is going to infer the C* table schema from the original PostgreSQL table schema and/or the required modification. 

However, the C* table schema created this way does have some limitations. For example, in the above schema, all non-primary-key columns can be made *static* because they're all film related data, aka, they're the same for all rows (*actor_name*) under one partition (*film_id*).

We definitely can create a more appropriate C* table schema manually in advance. The program will simply skip the step of creating a C* table.

# Program Introduction

The program is a Spark program that needs to be submitted to the DSE (Analytics) cluster for execution. At high level, the program logic is simple. But there are a few things that are worthy a few more explanation. 

## Configuration file (application.conf)

The program reads some key settings from a configuration file named *application.conf*. The content of this file is very straight-forward and self-explanatory except the following ones:

* **num_record_per_partition**: When spark reading data from a PostgreSQL database, the reads can be executed in parallel with smaller chunks of data. This setting helps control the parallelism of spark data reading.

* **dse.spark.driver_ip** and **dse.spark.driver_host**: Ignore these 2 settings. They were used for testing purpose and are not relevant anymore. They are kept here simply for book keeping purpose.  

```
conf {
    num_record_per_partition = 500

    rdbms {
        ip = "<postgres_ip_address>"
        port = 5432
        user_name = "<db_user_name>"
        db_name = "dvdrental"
        tbl_names = ["film", "actor", "film_actor"]
    }
    dse {
        contact_point_ip = "<dse_ip_address>"
        contact_point_port = 9042
        ks_name = "testks"
        tbl_name = "film_actor"

        spark {
            master_ip = "<dse_master_ip>"

            driver_ip = "<driver_host_ip>"
            driver_port = 51460
        }
    }
}
```

## Parallel Read Data from a PostgreSQL table

In order to get better read performance, Spark reads each PostgreSQL table concurrently with multiple partitions. The partitioning column used in this program is each table's primary key column. For a table with multiple primary key columns, the first one is used.

The number of the partitions that are used by Spark is calculated by the following formula:

```
numPartitions = ( max(partitioningColumn) - min(paritioningColumn) ) / num_record_per_partition + 1
```

The program is able to automatically identify each PostgreSQL table's primary key and therefore the partitioning column to be used by Spark.

## Write C* Table

The program writes denormalized data into C* table using Spark SQL and Spark Cassandra Connector. **NOTE** that Spark SQL is not necessary. We can achieve the same goal by simply using Spark DataSet/DataFrame functions directly. Spark SQL is used here purely for demo purpose for those who're more comfortable with "SQL style" data manipulation.

Because Spark SQL is used, this program needs to be submitted in *cluster* mode if the submitting client machine is outside the DSE cluster. This is because in order to use Spark SQL, Hive metastore needs to be accessible to the submitting client machine (or to be more precisely, to be accessible to *spark driver*). The Hive metastore in DSE (Analytics) cluster, by default, is not visible externally.

## Execute the Program

* Use the following command to build a Uber jar ("loaddvd-assembly-1.0.jar")
```
$ sbt clean assembly 
```

* Once the Uber jar file is created, copy it to some location on the Spark master node in the DSE cluster. 

* Then run the following command to submit the Spark job (assuming the submitting client machine is remotely outside the DSE cluster).
```
$ dse spark-submit --master dse://<dse_node_ip>:9042 --deploy-mode cluster --class com.example.loaddvd <some_location>/loaddvd-assembly-1.0.jar
```

# Appendix: Misc. Topics of Using JDBC with Spark

## Using RDBMS JDBC Driver with Spark Shell

In a DSE (Analytics) cluster, we can launch a Spark Shell REPL (Read Eval Print Loop) by running the following command on any node in the cluster.
```
$ dse spark
```

In order to be able access a remote RDBMS via JDBC, we need to first download the  JDBC driver for the corresponding RDBMS; then we start Spak Shell with specific extra jars. For PostgreSQL RDBMS (as we used in this dmo), the procedure is as below:
```
$ wget https://jdbc.postgresql.org/download/postgresql-42.2.16.jar

$ dse spark --jars postgresql-42.2.16.jar
```

Once in the Spark shell, we can read data using JDBC like this (using PostgreSQL as an example):
```
scala> val jdbcDF = (
     |     spark.read.format("jdbc")
     |     .option("driver", "org.postgresql.Driver")
     |     .option("url", "jdbc:postgresql://<ip_address>:5432/dvdrental")
     |     .option("dbtable", "actor")
     |     .option("user", "postgres")
     |     .load()
     | )
jdbcDF: org.apache.spark.sql.DataFrame = [actor_id: int, first_name: string ... 2 more fields]

scala> jdbcDF.printSchema()
root
 |-- actor_id: integer (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- last_update: timestamp (nullable = true)


scala> jdbcDF.show(5)
+--------+----------+------------+--------------------+
|actor_id|first_name|   last_name|         last_update|
+--------+----------+------------+--------------------+
|       1|  Penelope|     Guiness|2013-05-26 14:47:...|
|       2|      Nick|    Wahlberg|2013-05-26 14:47:...|
|       3|        Ed|       Chase|2013-05-26 14:47:...|
|       4|  Jennifer|       Davis|2013-05-26 14:47:...|
|       5|    Johnny|Lollobrigida|2013-05-26 14:47:...|
+--------+----------+------------+--------------------+
only showing top 5 rows
```

## JDBC Read Predicate Push-down

When using JDBC to read data in Spark, if we want to limit the scope of the selected data, one way is to apply Spark "filter()" function on the created JDBC data frame. Using the above Spark shell example, we can do this via the following command:
```
scala> jdbcDF.filter($"actor_id".between(20, 25)).show()
+--------+----------+---------+--------------------+
|actor_id|first_name|last_name|         last_update|
+--------+----------+---------+--------------------+
|      20|   Lucille|    Tracy|2013-05-26 14:47:...|
|      21|   Kirsten|  Paltrow|2013-05-26 14:47:...|
|      22|     Elvis|     Marx|2013-05-26 14:47:...|
|      23|    Sandra|   Kilmer|2013-05-26 14:47:...|
|      24|   Cameron|   Streep|2013-05-26 14:47:...|
|      25|     Kevin|    Bloom|2013-05-26 14:47:...|
+--------+----------+---------+--------------------+
```  

This method, however, doesn't do predicate (filtering condition) push-down to the RDBMS. It reads all data in an RDBMS table and do the filtering within Spark. For a large table, this will put pressure on Spark and will not have good performance.

For Spark JDBC data read, the only way to achieve predicate push-down is to explicitly specify the SQL statement with where condition, as below. Pay attention to the new "query" option part (**Note** "query" option is mutually exclusive with "dbtable" option and we need to remove "dbtable" option here). 

```
scala> val jdbcDF2 = (
     |     spark.read.format("jdbc")
     |     .option("driver", "org.postgresql.Driver")
     |     .option("url", "jdbc:postgresql://<ip_address>:5432/dvdrental")
     |     .option("user", "postgres")
     |     .option("query", "select * from actor where actor_id between 20 and 25")
     |     .load()
     | )
jdbcDF: org.apache.spark.sql.DataFrame = [actor_id: int, first_name: string ... 2 more fields]

scala> jdbcDF2.show
+--------+----------+---------+--------------------+
|actor_id|first_name|last_name|         last_update|
+--------+----------+---------+--------------------+
|      20|   Lucille|    Tracy|2013-05-26 14:47:...|
|      21|   Kirsten|  Paltrow|2013-05-26 14:47:...|
|      22|     Elvis|     Marx|2013-05-26 14:47:...|
|      23|    Sandra|   Kilmer|2013-05-26 14:47:...|
|      24|   Cameron|   Streep|2013-05-26 14:47:...|
|      25|     Kevin|    Bloom|2013-05-26 14:47:...|
+--------+----------+---------+--------------------+
)
```

**NOTE** that in the above example, data frame *jdbcDF2* only reads 5 records as specified in the SQL statement.
