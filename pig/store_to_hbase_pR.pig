
A = load '$input' using PigStorage() as (term:chararray, pageRank:double);
dump A;
store A into 'hbase://$table' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage ('pageRank:pageRank');