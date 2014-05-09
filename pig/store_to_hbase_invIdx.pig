A = load '$input' using PigStorage() as (term:chararray, df:int ,invIdx:map[(int,())]);
dump A;
store A into 'hbase://$table' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage ('invIdx:df, invIdx:invIdx');