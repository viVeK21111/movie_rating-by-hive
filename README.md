# movie_rating-by-hive
Using hive to analyze and find insights from the data. Hive is used in hdfs where the data is split into different nodes in a cluster and processed parallelly. 

creating three tables in hive, 'ratep' which is partitioned by userid and 'rate' which is 
unpartitioned. Loaded the data of raing_s.csv into the table 'rate' and later inserted into 
the partitioned table 'ratep' using dynamic partitioning

To enable dynamic partitionig 
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INTERNAL WORKING OF HIVE QUERY IN HDFS

Map Phase:

Both tables are read and processed by mappers.
Each mapper emits key-value pairs where the key is the join key (movieid in your case) and the value is the row from the table.

Shuffle and Sort Phase:

The key-value pairs from both tables are shuffled across the network so that all key-value pairs with the same join key end up on the same reducer.
The data is sorted by the join key within each reducer.

Reduce Phase:

Each reducer receives all key-value pairs for a specific join key.
The reducer merges the data from both tables based on the join key, producing the joined rows.

# how partitioning in hive optimizes the join operation between the tables

->Reduces the amount of data scanned from the partitioned table, especially for 
  large datasets.
->Improves join performance by focusing only on relevant partitions.



#(Bonus point) hive optimizes the join operation if both tables are bucketed based on join key

