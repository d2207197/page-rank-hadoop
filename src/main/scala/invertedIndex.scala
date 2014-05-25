package cc.nlplab

import scala.language.implicitConversions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{ LongWritable, Text, Writable, IntWritable,DoubleWritable, WritableComparator, WritableComparable, NullWritable}
import java.util.Scanner

import org.apache.hadoop.hbase.util.Writables



import scala.collection.mutable.ArrayBuffer


import org.apache.hadoop.mapreduce.{ReduceContext, MapContext, Partitioner}
import scamr.MapReduceMain
import scamr.conf.{LambdaJobModifier, ConfigureSpeculativeExecution}
import scamr.io.{InputOutput, LongArrayWritable}
import scamr.io.WritableConversions._
import scamr.mapreduce.mapper.SimpleMapper
import scamr.mapreduce.reducer.SimpleReducer
import scamr.mapreduce.lambda.{LambdaMapContext, LambdaReduceContext}
import scamr.mapreduce.{MapReducePipeline, MapReduceJob, MapOnlyJob}
import scamr.mapreduce.lib.{TextInputMapper, IdentityMapper}

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, HColumnDescriptor}
import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get}
import org.apache.hadoop.hbase.util.Bytes

import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.util.ReflectionUtils


import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem


class InvertedIndexMapper(context: MapContext[_, _, _, _]) extends TextInputMapper[TextPairWC, TermInfo](context)  {

  override def map(fileOffset: LongWritable, line: Text): Unit = {

    val titlePattern = """(?<=<title>)[^<]+(?=</title>)""".r
    val textPattern = """(?<=<text[^>]{0,30}>)[^<]+(?=</text>)""".r
    val title = (titlePattern findFirstIn line get).trim
    val textMatch = textPattern findFirstMatchIn line get
    val (textOffset, text) = (textMatch.start, textMatch.matched)
    val word_re = "[a-zA-Z]+".r

    word_re.findAllMatchIn(text) foreach { m =>
      val term = m.matched
      // val totalOffset = fileOffset + textOffset + m.start
      emit(new TextPairWC(term, title), new TermInfo(title, 1, ArrayBuffer((fileOffset.toInt, textOffset + m.start))))
    }

    // val sc = new Scanner(text).useDelimiter("[^a-zA-Z]")

    // while (sc.hasNext) {
    //   val term = sc.next
    //   if (term.length > 0) {
    //     val scanOffset = sc.`match`.start
    //     val totalOffset = fileOffset + textOffset + scanOffset
    //     emit(new TextPairWC(term, title), new TermInfo(title, 1, ArrayBuffer(totalOffset)))
    //   }
    // }
  }
}

class InvertedIndexCombiner(context: ReduceContext[_,_,_,_]) extends SimpleReducer[TextPairWC, TermInfo, TextPairWC, TermInfo](context)  {

  override def reduce(termTitle: TextPairWC, termInfos: Iterator[TermInfo]): Unit = {
    val termInfo = termInfos reduceLeft { (ti1, ti2) =>
      new TermInfo(ti1.title, ti1.tf + ti2.tf, ti1.ofs ++ ti2.ofs)
    }
    emit(termTitle, termInfo)
  }
}

class TermPartitioner extends Partitioner[TextPairWC, TermInfo] {
  override def getPartition(termFile: TextPairWC, termInfo: TermInfo, numPartitions: Int): Int = (termFile._1.hashCode & Int.MaxValue) % numPartitions
}


class FirstKeyGroupComparator protected () extends WritableComparator(classOf[TextPairWC], true) {
  override def compare(w1: WritableComparable[_], w2: WritableComparable[_]): Int = 
    w1.asInstanceOf[TextPairWC]._1  compareTo w2.asInstanceOf[TextPairWC]._1
}


class InvertedIndexReducer(context: ReduceContext[_,_,_,_]) extends SimpleReducer[TextPairWC, TermInfo, TextAndIntWC, TermInfoArray](context)  {

  override def reduce(termTitle: TextPairWC, termInfos: Iterator[TermInfo]): Unit = {
    val termInfosArrayBW : TermInfoArray= new TermInfoArray ++= termInfos
    if (termInfosArrayBW.size < 3000)
      emit(new TextAndIntWC(termTitle._1, termInfosArrayBW.size), termInfosArrayBW)
  }
}




object InvertedIndexMapReduce extends MapReduceMain {
  override def run(conf: Configuration, args: Array[String]): Int = {
    val inputDirs = args.init
    val outputDir = args.last

    val pipeline = MapReducePipeline.init(conf) -->
    new InputOutput.TextFileSource(inputDirs) -->
    new MapReduceJob(
      classOf[InvertedIndexMapper],
      classOf[InvertedIndexCombiner],
      classOf[InvertedIndexReducer],
      "build inverted index") ++
    LambdaJobModifier { job =>
      job.setGroupingComparatorClass(classOf[FirstKeyGroupComparator])
      job.setPartitionerClass(classOf[TermPartitioner]) } -->
    new InputOutput.SequenceFileSink[TextAndIntWC, TermInfoArray](s"$outputDir")
    val (isSuccess, jobs) = pipeline.execute
    if (!isSuccess) return 1

    return 0
  }

}


object InvertedIndexToHBase extends MapReduceMain {

  override def run(conf: Configuration, args: Array[String]): Int = {
    val outputDir = args(0)
    val hbaseTblName = args(1)
    val config = HBaseConfiguration.create(conf)
    // config.set("hbase.master","localhost:60000")
    // config.set("hbase.zookeeper.quorum", "nlp1.cs.nthu.edu.tw");
    val hbase = new HBaseAdmin(config)
    if (!hbase.tableExists(hbaseTblName)) {
      println(s"hbase table doesn't exist, creating...: $hbaseTblName")
      val mathTable = new HTableDescriptor(hbaseTblName)
      val gradeCol = new HColumnDescriptor("ii")
      mathTable.addFamily(gradeCol)
      hbase.createTable(mathTable)
    }
    else println(s"$hbaseTblName exist")

    val hbaseTable = new HTable(config, hbaseTblName)

    val fs = FileSystem.get(conf)
    val path = new Path(outputDir)

    val inputFiles = fs.listStatus(path)
    val filePaths = for {
      fileStat <- inputFiles
      filePath = fileStat.getPath
      if filePath.getName startsWith "part"
    } yield filePath
    println(s"reading $filePaths")

    for (filePath <- filePaths)
    {
      val reader = new SequenceFile.Reader(fs, filePath, conf)
      var key = ReflectionUtils.newInstance(reader.getKeyClass, conf).asInstanceOf[TextAndIntWC]
      var value = ReflectionUtils.newInstance(reader.getValueClass, conf).asInstanceOf[TermInfoArray]
      var position = reader.getPosition
      while (reader.next(key, value)) {
        val syncSeen = if (reader.syncSeen()) "*" else ""
        println(s"$position, $syncSeen, $key, $value")

        position = reader.getPosition

        val put_data = new Put(key._1.toString.getBytes)
        put_data.add(Bytes.toBytes("ii"), Bytes.toBytes("df"), Bytes.toBytes(key._2))
        put_data.add(Bytes.toBytes("ii"), Bytes.toBytes("ii"), Writables.getBytes(value))
        hbaseTable.put(put_data)
      }
      reader.close
    }
    0
  }
}
