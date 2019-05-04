# callrecords
## target
Analyze call data.
Count the number of calls and duration per day, month, and year for users.
## technology used
hadoop,zookeeper,flume,kafka,HBase,MapReduce,MySQL
## project realization
### producer
Randomly generate data including calling number, called number, call setup time, and call duration.
### consumer
Collect real-time data through flume to kafka and then consume with hbase.
### analysis
Use MapReduce to count the number of calls and duration per day, month, and year for every user.
Save data directly into MySQL for future queries.
