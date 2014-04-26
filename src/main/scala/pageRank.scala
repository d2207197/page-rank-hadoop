package cc.nlplab

import scala.language.implicitConversions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{NullWritable, LongWritable, Text, Writable, IntWritable, WritableComparator, WritableComparable}


import org.apache.hadoop.mapreduce.{ReduceContext, MapContext, Partitioner}
import scamr.MapReduceMain
import scamr.conf.{LambdaJobModifier, ConfigureSpeculativeExecution}
import scamr.io.{InputOutput, TextArrayWritable}
import scamr.io.WritableConversions._
import scamr.io.tuples.Tuple2WritableComparable
import scamr.mapreduce.mapper.SimpleMapper
import scamr.mapreduce.reducer.SimpleReducer
import scamr.mapreduce.{MapReducePipeline, MapReduceJob, MapOnlyJob}
import scamr.mapreduce.lib.{TextInputMapper, IdentityMapper}
import java.io.{DataInput, DataOutput}


import grizzled.slf4j.Logging



object WritableConversions2 {
  implicit def textArrayToTextArrayWritable(value: Array[Text]): TextArrayWritable =
    new TextArrayWritable(value.map((x: Text) => x))
  implicit def textArrayWritableToTextArray(value: TextArrayWritable): Array[Text] =
    value.get.asInstanceOf[Array[Text]]
}

import WritableConversions2._

class PageRankLinks(var pageRank: IntWritable, var links: TextArrayWritable) extends Writable {
  def this() = this(0, Array[String]())

  override def toString: String =
    s"""PageRankLinks( $pageRank, [${links.get.mkString(", ")}])"""
  override def readFields(in: DataInput) : Unit = {
    pageRank.readFields(in)
    links.readFields(in)
  }
  override def write(out: DataOutput) : Unit = {
    pageRank.write(out)
    links.write(out)
  }
}


class ExtractInfoMapper(context: MapContext[_, _, _, _])
    extends TextInputMapper[Text, Text](context) with Logging{
  private val titlePattern = raw"""(?<=<title>)[^<]+(?=</title>)""".r
  private val linkPattern = raw"""(?<=\[\[)[^\]]+(?=\]\])""".r
  override def map(offset: LongWritable, line: Text): Unit = {
    titlePattern.findFirstIn(line:String) match {
      case Some(title) =>
        linkPattern.findAllIn(line:String) foreach (link =>
          {
            val (linkTrimed, titleTrimed) = (link.trim, title.trim)
            emit(titleTrimed, linkTrimed)
            emit(" " + linkTrimed, "")
          }
        )
      case None => warn("No Title Found: $offset")
    }
  }
}

class FilterLinksReducer(context: ReduceContext[_,_,_,_])
    extends SimpleReducer[Text, Text, Text, PageRankLinks](context) with Logging {

  override def reduce(title: Text, links: Iterator[Text]): Unit = {
    if (title startsWith " ") {

      val linksSet: Set[Text] = links.toSet - new Text()
      println(s"""accept $title ${linksSet mkString " | " }""")
      if (linksSet.size > 0)
        emit(title.trim, new PageRankLinks(1, linksSet.toArray: Array[Text]))
      else
      {
        println(s"NULL title: $title")
        emit(title.trim, new PageRankLinks(1, Array("NULL")))
      }
    }
    else println(s"""reject $title """)
  }
}


// import scala.collection.JavaConversions._

class TitlePartitioner extends Partitioner[Text, Text] {
  override def getPartition(title: Text, link: Text, numPartitions: Int): Int = {
    (title.trim.hashCode & Int.MaxValue) % numPartitions
  }
}

class GroupComparator protected ()
    extends WritableComparator(classOf[Text], true) {
  override def compare(w1: WritableComparable[_], w2: WritableComparable[_]): Int = {
    w1.asInstanceOf[Text].trim compareTo w2.asInstanceOf[Text].trim
  }
}

object ExampleSortedWordCountMapReduce extends MapReduceMain {
  override def run(conf: Configuration, args: Array[String]): Int = {
    val inputDirs = args.init
    val outputDir = args.last
    val pipeline = MapReducePipeline.init(conf) -->  // hint: start by adding a data source with -->
    new InputOutput.TextFileSource(inputDirs) --> // hint: use --> to direct data into or out of a stage
    new MapReduceJob(classOf[ExtractInfoMapper], classOf[FilterLinksReducer], "step 1: Build Graph") ++
    LambdaJobModifier { job => job.setGroupingComparatorClass(classOf[GroupComparator]) } ++
    LambdaJobModifier { job => job.setPartitionerClass(classOf[TitlePartitioner]) } -->
    // new MapReduceJob(classOf[TextTextIdentityMapper], classOf[ConcatLinksReducer], "step 2 concatenate links") --> 
    new InputOutput.TextFileSink[Text, PageRankLinks](outputDir)
      // hint: use ++ to add ConfModifiers or JobModifiers to a TaskStage or a StandAloneJob
      // ConfigureSpeculativeExecution(false, false) ++
      // LambdaJobModifier { _.setNumReduceTasks(1) } --> // hint: use --> to chain MR jobs into pipelines
      // new MapReduceJob(classOf[CombineCountAndWordIntoTupleMapper], classOf[OutputSortedCountsReducer],
        // "ScaMR sorted word count example, stage 2") -->
    if (pipeline.execute) 0 else 1
  }
}


