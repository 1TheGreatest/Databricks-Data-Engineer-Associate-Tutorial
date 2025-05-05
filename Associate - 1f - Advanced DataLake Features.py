# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog `hive_metastore`;
# MAGIC use schema `default`;
# MAGIC
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS students;
# MAGIC
# MAGIC CREATE TABLE students
# MAGIC   (id INT,name STRING,value DOUBLE);
# MAGIC
# MAGIC INSERT INTO students VALUES (1,"SOLO", 1.0);
# MAGIC INSERT INTO students VALUES (2,"OLO", 2.4);
# MAGIC INSERT INTO students VALUES (3,"AKOLO", 3.3);
# MAGIC
# MAGIC INSERT INTO students VALUES 
# MAGIC   (4,"MIKE", 4.5),
# MAGIC   (5,"KLO", 5.6),
# MAGIC   (6,"SAO", 6.8);
# MAGIC
# MAGIC UPDATE students
# MAGIC SET value = value + 1
# MAGIC WHERE name LIKE "S%";
# MAGIC
# MAGIC DELETE FROM students
# MAGIC WHERE value > 6;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW updates(id,name,value,type) AS VALUES
# MAGIC   (2, "Omar", 15.2, "update"),
# MAGIC   (3,"", null, "delete"),
# MAGIC   (7, "Blue", 7.7, "insert"),
# MAGIC   (11, "Diya", 8.8, "update");
# MAGIC
# MAGIC   MERGE INTO students b
# MAGIC   USING updates u
# MAGIC   ON b.id=u.id
# MAGIC   WHEN MATCHED AND u.type="update"
# MAGIC     THEN UPDATE SET *
# MAGIC   WHEN MATCHED AND u.type="delete"
# MAGIC     THEN DELETE
# MAGIC   WHEN NOT MATCHED AND u.type="insert"
# MAGIC     THEN INSERT  * ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY students

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deletion Vectors
# MAGIC Note that the log includes an OPTIMIZE operation, yet we never called OPTIMIZE on the students table. If you open the operationParameters for the OPTIMIZE operation, you will see that auto: true. This is because Deletion Vectors
# MAGIC triggered auto-compaction. When we delete rows from a table, Deletion Vectors mark those rows for deletion but do not rewrite the underlying Parquet files. This helps reduce the so-called small file problem, where a table is made up of a large number of small Parquet files. However, Deletion Vectors will trigger auto-compaction, and the underlying files are re-written.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM students VERSION AS OF 7

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM students;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM students

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY students

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE students TO VERSION AS OF 7;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM students;
