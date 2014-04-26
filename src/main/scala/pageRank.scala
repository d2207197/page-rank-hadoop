package cc.nlplab

// import scala.language.implicitConversions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{NullWritable, LongWritable, Text, Writable, IntWritable}

import org.apache.hadoop.mapreduce.{ReduceContext, MapContext}
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
  // implicit def textToString(value: Text): String = value.toString
  // implicit def stringToText(value: String): Text = new Text(value)
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
    val lineString = line.toString
    titlePattern.findFirstIn(lineString) match {
      case Some(title) =>
        linkPattern.findAllIn(lineString) foreach (link => emit(new Text(link), new Text(title)))
        emit(new Text(title), new Text())
      case None => warn("No Title Found: $offset")
    }
  }
}

class FilterLinksReducer(context: ReduceContext[_,_,_,_])
    extends SimpleReducer[Text, Text, Text, Text](context) with Logging {

  override def reduce(link: Text, titles: Iterator[Text]): Unit = {
    val titlesList = titles.toList
    if (titlesList contains (new Text())) {
      println(s"""accept $link ${titlesList mkString " | " }""")
      titlesList foreach (title  => if (title != (new Text())) emit(title, link))
    }
    else println(s"""reject $link ${titlesList mkString " | " }""")
  }
}

class TextTextIdentityMapper(context: MapContext[_,_,_,_]) 
    extends IdentityMapper[Text, Text](context)

class ConcatLinksReducer(context: ReduceContext[_,_,_,_])
    extends SimpleReducer[Text, Text, Text, PageRankLinks](context) with Logging {
  override def reduce(title: Text, links: Iterator[Text]): Unit = {
    val linksArray = links.toArray
    if (linksArray.length > 0)
      emit(title, new PageRankLinks(1, linksArray))
    else
    {
      println(s"NULL title: $title")
      emit(title, new PageRankLinks(1, Array("NULL")))
    }
  }
}

// import scamr.mapreduce.lib.SumReducer

// class WordCountReducer(context: ReduceContext[_,_,_,_]) extends SumReducer[Text](context)

// class LongAndTextWritableComparable(tuple: (LongWritable, Text))
// extends Tuple2WritableComparable[LongWritable, Text](tuple) {
//   def this(a1: LongWritable, a2: Text) = this((a1, a2))
//   def this() = this((new LongWritable, new Text))
// }

// class CombineCountAndWordIntoTupleMapper(context: MapContext[_, _, _, _])
// extends SimpleMapper[Text, LongWritable, LongAndTextWritableComparable, NullWritable](context) {
//   override def map(word: Text, count: LongWritable): Unit =
//     emit(new LongAndTextWritableComparable(count, word), NullWritable.get)
// }

// class OutputSortedCountsReducer(context: ReduceContext[_, _, _, _])
// extends SimpleReducer[LongAndTextWritableComparable, NullWritable, Text, LongWritable](context) {
//   override def reduce(key: LongAndTextWritableComparable, ignored: Iterator[NullWritable]): Unit = emit(key._2, key._1)
// }


class GroupComparator protected ()
    extends WritableComparator(classOf[TextPairWC], true) {
  override def compare(w1: WritableComparable, w2: WritableComparable): Int = {
    w1.asInstanceOf[Text].toString.trim compareTo w2.asInstanceOf[Text].toString.trim
  }
}

object ExampleSortedWordCountMapReduce extends MapReduceMain {
  override def run(conf: Configuration, args: Array[String]): Int = {
    val inputDirs = args.init
    val outputDir = args.last
    val pipeline = MapReducePipeline.init(conf) -->  // hint: start by adding a data source with -->
    new InputOutput.TextFileSource(inputDirs) --> // hint: use --> to direct data into or out of a stage
    new MapReduceJob(classOf[ExtractInfoMapper], classOf[FilterLinksReducer], "step 1: Build Graph") ++
    LambdaJobModifier { job => job.setGroupingComparatorClass(classOf[GroupComparator]) } -->
    new MapReduceJob(classOf[TextTextIdentityMapper], classOf[ConcatLinksReducer], "step 2 concatenate links") --> 
    new InputOutput.TextFileSink[Text, PageRankLinks](outputDir)
      // hint: use ++ to add ConfModifiers or JobModifiers to a TaskStage or a StandAloneJob
      // ConfigureSpeculativeExecution(false, false) ++
      // LambdaJobModifier { _.setNumReduceTasks(1) } --> // hint: use --> to chain MR jobs into pipelines
      // new MapReduceJob(classOf[CombineCountAndWordIntoTupleMapper], classOf[OutputSortedCountsReducer],
        // "ScaMR sorted word count example, stage 2") -->
    if (pipeline.execute) 0 else 1
  }
}


