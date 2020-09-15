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

After making the above changes, we can connect to PostgreSQL server from external, such as from the DSE cluster node.

* **Load Sample Data Set**

The sample data set we're going to use in this demo is from the following website:
https://www.postgresqltutorial.com/postgresql-sample-database/

This sample data set represents a [DVD rental ER model]() 