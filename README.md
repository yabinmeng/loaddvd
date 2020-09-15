# Overview 

This demo demonstrates how to use DSE Analytics (Apach Spark) to load data from multiple RDBMS (PostgreSQL) tables; combine and massage the data a bit; and the data write data into on Cassandra (C*) table.

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

