# GetDailyRevenue
This is a Spark Project to get the Daily revenue of the products based on many parameters which are explained in detail in this project.

### Problem Statement

### UML Diagram

<img src="https://github.com/kamireddig/GetDailyRevenue/blob/master/Retail_DB_UML.png" width="900">

**Technologies and Conecepts used in this project**
1. Hadoop
   1. HDFS
   2. HIVE
   3. SQOOP
2. Spark
   1. Spark SQL
   2. Spark RDD
   3. Spark Dataframes

**System Configurations**
1. Personal Laptop: MacOS 10.15.4 (Catalina)
2. UNIX System hostname: gw02.itversity.com
   1. Connect using **'ssh gw02.itversity.com'**
   2. Prompts with a password. (Give the password provided to you.)

**Data Locations**
1. Data is present in the server 'gw02.itversity.com' in the HDFS Cluster in path: '/public/retail_db'
   1. Access the data in the path using the command
      > **hadoop fs -ls /public/retail_db**

### Sqoop
<p>Apache Sqoop allows easy import and export of data from structured data stores such as relational databases, enterprise data warehouses, and NoSQL systems. Using Sqoop, you can provision the data from external system on to HDFS, and populate tables in Hive and HBase.
Sqoop integrates with Oozie, allowing you to schedule and automate import and export tasks. Sqoop uses a connector based architecture which supports plugins that provide connectivity to new external systems.</p>

Access mysql in the server
> mysql -u retail_user -h ms.itversity.com -p </br>
> password: itversity

1. **Sqoop Import**   

<p>The below two loads are full loads. To use incremental load, use the below syntax in the sqoop commands.</p>
-> –incremental <mode>  (Modes: append & lastModified)
-> –check-column <column name>
-> –last value <last check column value>

**Sqoop Import into HDFS**

Link to Sqoop Documentation: https://sqoop.apache.org/docs/1.4.3/SqoopUserGuide.html#_literal_sqoop_import_literal
<<Check 7.2.2 Table 3>>

When prompted for password:</br>
>Enter Password: itversity

- sqoop import --connect jdbc:mysql://ms.itversity.com:3306/retail_db --username retail_user -P --table orders --target-dir=/user/pratikgaurav/sqoop_import/retail_db/orders -m 1
- sqoop import --connect jdbc:mysql://ms.itversity.com:3306/retail_db --username retail_user -P --table order_items --target-dir=/user/pratikgaurav/sqoop_import/retail_db/order_items -m 1
- sqoop import --connect jdbc:mysql://ms.itversity.com:3306/retail_db --username retail_user -P --table products --target-dir=/user/pratikgaurav/sqoop_import/retail_db/products -m 1
- sqoop import --connect jdbc:mysql://ms.itversity.com:3306/retail_db --username retail_user -P --table departments --target-dir=/user/pratikgaurav/sqoop_import/retail_db/departments -m 1
- sqoop import --connect jdbc:mysql://ms.itversity.com:3306/retail_db --username retail_user -P --table customers --target-dir=/user/pratikgaurav/sqoop_import/retail_db/customers -m 1
- sqoop import --connect jdbc:mysql://ms.itversity.com:3306/retail_db --username retail_user -P --table categories --target-dir=/user/pratikgaurav/sqoop_import/retail_db/categories -m 1

**Sqoop Import into HIVE**

Link to Sqoop Documentation: https://sqoop.apache.org/docs/1.4.3/SqoopUserGuide.html#_literal_sqoop_import_literal
<<Check 7.2.9 Table 8>>

When prompted or password:</br>
>Enter Password: itversity

- sqoop import --connect jdbc:mysql://ms.itversity.com:3306/retail_db --username retail_user -P --table orders --mysql-delimiters --hive-import --hive-overwrite --create-hive-table --hive-table retail_db.orders_pk -m 1
- sqoop import --connect jdbc:mysql://ms.itversity.com:3306/retail_db --username retail_user -P --table order_items --mysql-delimiters --hive-import --hive-overwrite --create-hive-table --hive-table retail_db.order_items_pk -m 1
- sqoop import --connect jdbc:mysql://ms.itversity.com:3306/retail_db --username retail_user -P --table products --mysql-delimiters --hive-import --hive-overwrite --create-hive-table --hive-table retail_db.products_pk -m 1
- sqoop import --connect jdbc:mysql://ms.itversity.com:3306/retail_db --username retail_user -P --table departments --mysql-delimiters --hive-import --hive-overwrite --create-hive-table --hive-table retail_db.departments_pk -m 1
- sqoop import --connect jdbc:mysql://ms.itversity.com:3306/retail_db --username retail_user -P --table customers --mysql-delimiters --hive-import --hive-overwrite --create-hive-table --hive-table retail_db.customers_pk -m 1
- sqoop import --connect jdbc:mysql://ms.itversity.com:3306/retail_db --username retail_user -P --table categories --mysql-delimiters --hive-import --hive-overwrite --create-hive-table --hive-table retail_db.categories_pk -m 1

2. **Sqoop Export**

3. **Sqoop Merge**

### HIVE
<p>Apache Hive is a data warehouse software project built on top of Apache Hadoop for providing data query and analysis.[2] Hive gives a SQL-like interface to query data stored in various databases and file systems that integrate with Hadoop.</p>
