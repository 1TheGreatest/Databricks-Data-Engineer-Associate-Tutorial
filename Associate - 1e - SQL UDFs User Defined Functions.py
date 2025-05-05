# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog `hive_metastore`;
# MAGIC use schema `default`;
# MAGIC
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC ### User-Defined Functions
# MAGIC User Defined Functions (UDFs) in Spark SQL allow you to register custom SQL logic as functions in a database, making these methods reusable anywhere SQL can be run on Databricks. These functions are registered natively in SQL and maintain all of the optimizations of Spark when applying custom logic to large datasets.
# MAGIC
# MAGIC At minimum, creating a SQL UDF requires a function name, optional parameters, the type to be returned, and some custom logic.
# MAGIC
# MAGIC Below, a simple function named **sale_announcement** takes an item name and item price as parameters. It returns a string that announces a sale for an item at 80% of its original price

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION sale_announcement(item_name STRING, item_price INT)
# MAGIC RETURNS STRING
# MAGIC RETURN concat("The ", item_name, " is on sale for $", round(item_price*0.8, 0));
# MAGIC
# MAGIC
# MAGIC SELECT *, sale_announcement(name, price) AS message from item_lookup_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scoping and Permissions of SQL UDFs
# MAGIC SQL user-defined functions:
# MAGIC - Persist between execution environments (which can include notebooks, DBSQL queries, and jobs).
# MAGIC - Exist as objects in the metastore and are governed by the same Table ACLs as databases,tables, or views.
# MAGIC - To create a SQL UDF, you need **USE CATALOG** on the catalog, and **USE SCHEMA** and **CREATE FUNCTION** on the schema.
# MAGIC - To use a SQL UDF, you need **USE CATALOG** on the catalog, **USE SCHEMA** on the schema, and **EXECUTE** on the function.
# MAGIC
# MAGIC We can use **DESCRIBE FUNCTION** to see where a function was registered and basic Information about expected Inputs and what is returned (and even more information with DESCRIBE FUNCTION EXTENDED).

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION EXTENDED sale_announcement

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Viewing functions in catalogs
# MAGIC
# MAGIC SELECT current_catalog()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simple Control Flow Functions
# MAGIC Combining SQL UDFs with control flow in the form of CASE / WHEN clauses provides optimized execution for control flows within SQL workloads. Tne standard SQL syntactic construct **CASE WHEN** allows the evaluation of multiple conditional statements with alternative outcomes based on table contents.
# MAGIC
# MAGIC Here, we demonstrate wrapping this control flow logic in a function that will be reusable anywhere we can execute SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION item_preference(name STRING, price INT)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN name = "Apple" THEN "This is my favourite fruit"
# MAGIC   WHEN name = "Grape" THEN "This is another favourite"
# MAGIC   WHEN price >= 100 THEN concat("I would wait until the ", name, " is on sale for $", round(price*0.8, 0) )
# MAGIC   ELSE concat("I don't need a ",name )
# MAGIC END;
# MAGIC
# MAGIC SELECT *, item_preference(name,price) FROM item_lookup_bronze;

# COMMAND ----------

# DA.cleanup()
