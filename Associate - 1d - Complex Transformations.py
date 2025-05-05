# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog `hive_metastore`;
# MAGIC use schema `default`;
# MAGIC
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create table technique

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM events_strings LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM events_strings 
# MAGIC WHERE value:event_type = "error" 
# MAGIC ORDER BY key LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC Let's use the JSON string example above to derive the schema, then parse the entire JSON column into STRUCT types.
# MAGIC
# MAGIC • schema_of_json() returns the schema derived from an example JSON string.
# MAGIC • from_json() parses a column containing a JSON string into a STRUCT type using the specified schema.
# MAGIC
# MAGIC After we unpack the JSON string to a STRUCT type, let's unpack and flatten all STRUCT fields into columns.
# MAGIC
# MAGIC * unpacking can be used to flatten a STRUCT; col_name.* pulls out the subfields of col_name into their own columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT schema_of_json('{"event_type": "purchase", "timestamp": 1744492072, "location": {"country": "IN", "city": "New York"}, "devices": ["tablet", "mobile", "mobile"], "items": [{"sku": "65ac8e80", "qty": 4, "price": 180.23}, {"sku": "cbb9ac76", "qty": 4, "price": 153.06}, {"sku": "e1a387bb", "qty": 3, "price": 127.65}], "error": null, "tags": ["evening", "morning"]}') AS schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW parsed_events AS SELECT json.* FROM(
# MAGIC   SELECT from_json(value, 'STRUCT<devices: ARRAY<STRING>, error: STRING, event_type: STRING, items: ARRAY<STRUCT<price: DOUBLE, qty: BIGINT, sku: STRING>>, location: STRUCT<city: STRING, country: STRING>, tags: ARRAY<STRING>, timestamp: BIGINT>' ) AS json FROM events_strings
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM parsed_events LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Manipulate Arrays
# MAGIC Spark SQL has a number of functions for manipulating array data, including the following:
# MAGIC - explode() separates the elements of an array into multiple rows; this creates a new row for each element.
# MAGIC - size() provides a count for the number of elements in an array for each row.
# MAGIC
# MAGIC The code below explodes the items field (an array of structs) into multiple rows and shows events containing arrays with 3 or more items.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW exploded_events AS 
# MAGIC SELECT *, explode(items) AS item
# MAGIC FROM parsed_events;
# MAGIC     
# MAGIC SELECT * FROM exploded_events WHERE size(items) >2 ;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE exploded_events

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nesting Functions
# MAGIC We may want to see a list of all events associated with each user_id and we can collect all items that have been in a user's cart at any time for any event. Let's walk through how we can accomplish this.
# MAGIC
# MAGIC ### Step 1
# MAGIC We use **collect_set ()** to gather ("collect") all unique values in a group, including arrays. We use it here to collect all unique
# MAGIC **item_id's** in our items array of structs.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, collect_set(items.sku) AS cart_history
# MAGIC FROM exploded_events
# MAGIC GROUP BY user_id
# MAGIC ORDER BY user_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2
# MAGIC We use **flatten ()** to pull all items into a single array.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, flatten(collect_set(items.sku) AS cart_history)
# MAGIC FROM exploded_events
# MAGIC GROUP BY user_id
# MAGIC ORDER BY user_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3
# MAGIC Because there were multiple sets of items involved, there are duplicate values in our array.
# MAGIC We use **array-distinct ()** to remove these duplicates.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, array_distinct(flatten(collect_set(items.sku) AS cart_history))
# MAGIC FROM exploded_events
# MAGIC GROUP BY user_id
# MAGIC ORDER BY user_id

# COMMAND ----------

# MAGIC %md
# MAGIC We use **collect_set()** twice in the below cell. one to call event name's, and again on the item_id's or item_sku in the items column. We nest the sceond call to **collect_set()** in our **flatten()** and **array-distinct** calls as outlined above.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, 
# MAGIC   collect_set(event_name) AS event_history,
# MAGIC   array_distinct(flatten(collect_set(items.sku) AS cart_history))
# MAGIC FROM exploded_events
# MAGIC GROUP BY user_id
# MAGIC ORDER BY user_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combine and Reshape Data
# MAGIC
# MAGIC #### Join Tables
# MAGIC Spark SQL supports standard JOIN operations (inner, outer, left, right, anti, cross, semi).
# MAGIC Here we join the exploded events dataset with a lookup table to grab the standara printed item name

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW item_purchases AS
# MAGIC SELECT * 
# MAGIC FROM (SELECT *,explode(items) AS item FROM sales) a
# MAGIC INNER JOIN item_lookup b 
# MAGIC ON a.item.item_id = b.item_id;
# MAGIC
# MAGIC SELECT * FROM item_purchases;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pivot Tables
# MAGIC We can use PIVOT to view data from different perspectives by rotating unique values in a specified pivot column into multiple columns based on an aggregate function.
# MAGIC
# MAGIC • The **PIVOT** clause follows the table name or subquery specified in a FROM clause, which is the input for the pivot table.
# MAGIC
# MAGIC • Unique values in the pivot column are grouped and aggregated using the provided aggregate expression, creating a separate column for each unique value in the resulting pivot table.
# MAGIC
# MAGIC The following code cell uses **PIVOT** to flatten out the item purchase information contained in several fields derived from the sales dataset. This flattened data format can be useful for dashboarding, but also useful for applying machine learning algorithms for inference or prediction

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM item_purchases
# MAGIC PIVOT (
# MAGIC   sum(items.quantity) for item_id IN(
# MAGIC     "P_FOAM_K",
# MAGIC     "M_STAN_Q",
# MAGIC     "ETC..."
# MAGIC   )
# MAGIC )

# COMMAND ----------

## Always cleanup
