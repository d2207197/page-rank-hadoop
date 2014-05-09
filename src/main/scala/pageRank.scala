package cc.nlplab

import scala.language.implicitConversions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{ LongWritable, Text, Writable, IntWritable,DoubleWritable, WritableComparator, WritableComparable, NullWritable}


import org.apache.hadoop.mapreduce.{ReduceContext, MapContext, Partitioner}
import scamr.MapReduceMain
import scamr.conf.{LambdaJobModifier, ConfigureSpeculativeExecution}
import scamr.io.{InputOutput, TextArrayWritable}
import scamr.io.WritableConversions._
import scamr.io.tuples.Tuple2WritableComparable
import scamr.mapreduce.mapper.SimpleMapper
import scamr.mapreduce.reducer.SimpleReducer
import scamr.mapreduce.lambda.{LambdaMapContext, LambdaReduceContext}
import scamr.mapreduce.{MapReducePipeline, MapReduceJob, MapOnlyJob}
import scamr.mapreduce.lib.{TextInputMapper, IdentityMapper}
import java.io.{DataInput, DataOutput}

import  org.apache.hadoop.mapred.Task.Counter.REDUCE_OUTPUT_RECORDS


import grizzled.slf4j.Logging



object WritableConversions2 {
  implicit def textArrayToTextArrayWritable(value: Array[Text]): TextArrayWritable =
    new TextArrayWritable(value.map((x: Text) => x))
  implicit def textArrayWritableToTextArray(value: TextArrayWritable): Array[Text] =
    value.get.asInstanceOf[Array[Text]]
}

import WritableConversions2._

class PageRankLinks(var pageRank: DoubleWritable, var links: TextArrayWritable) extends Writable {

  def this() = this(0, new TextArrayWritable(Array[Text]()))

  override def toString: String =
    s"""PageRankLinks( $pageRank, [${links.get.mkString(", ")}])"""

  override def readFields(in: DataInput) : Unit = {
    pageRank.readFields(in)
    // if (in.readBoolean()) {
    // links = Array[String]()
    // links.set(Array[String]())
    links.readFields(in)
    // }
    // else
    // links = Array[String]()
  }
  override def write(out: DataOutput) : Unit = {
    pageRank.write(out)
    if (links == null || links.get == null)
      links = new TextArrayWritable(Array[Text]())
    // out.writeBoolean(false)
    // else {
    // out.writeBoolean(true)
    links.write(out)
    
  }
}


class DoubleAndTextWC(tuple: (DoubleWritable, Text))
    extends Tuple2WritableComparable[DoubleWritable, Text](tuple) {
  def this (a1: DoubleWritable, a2: Text) = this((a1, a2))
  def this() = this((new DoubleWritable, new Text))
}


// t:1 -> l:2 l:3 l:4 l:5

// l:2 -> t:1
// l:3 -> t:1
// l:4 -> t:1
// l:5 -> t:1
// " "l:2 -> ""
// " "l:1 -> ""
// " "l:1 -> ""
// " "l:1 -> ""
// " "l:1 -> ""
// " NULL" -> t:1

// t:2 -> l:1 l:3 l:4

// l:1 -> t:2
// l:3 -> t:2
// l:4 -> t:2
// " "t:2 -> ""
// " NULL" -> t:2

// t:3 -> l:1 l:2 l:4

// l:1 -> t:3
// l:3 -> t:3
// l:4 -> t:3
// " "t:3 -> ""
// " NULL" -> t:3

// t:5 ->
// " "t:5 -> ""
// " NULL" -> t:5


// -------------------------
// " "t:1 -> "", t:2, t:3
// " "t:2 -> "", t:1, t:3
// " "t:3 -> "", t:1, t:2
// " "t:5 -> ""
// l:4 -> t:1, t:2, t:3    # delete
// " NULL" -> t:1, t:2, t:3, t:5


// ----------
// t:2 -> l:1
// NULL -> t:2
// t:3 -> l:1
// NULL -> t:3
// t:1 -> t:2
// t:1 -> NULL





class ExtractInfoMapper(context: MapContext[_, _, _, _]) extends TextInputMapper[Text, Text](context) with Logging {
  private val titlePattern = raw"""(?<=<title>)[^<]+(?=</title>)""".r
  private val linkPattern = raw"""(?<=\[\[)[^\]]+(?=\]\])""".r

  override def map(offset: LongWritable, line: Text): Unit = {
    titlePattern.findFirstIn(line:String) match {
      case Some(title) =>
        val titleTrimed = title.trim
        linkPattern.findAllIn(line:String) foreach (link =>
          {
            val linkTrimed = link.trim
            emit(linkTrimed, titleTrimed)
          }
        )
        emit(" " + titleTrimed, "")
        emit("", titleTrimed)
      case None => warn("No Title Found: $offset")
    }
  }
}


class TitlePartitioner extends Partitioner[Text, Text] {
  override def getPartition(title: Text, link: Text, numPartitions: Int): Int = {
    (title.trim.hashCode & Int.MaxValue) % numPartitions
  }
}

class GroupComparator protected () extends WritableComparator(classOf[Text], true) {
  override def compare(w1: WritableComparable[_], w2: WritableComparable[_]): Int = {
    w1.asInstanceOf[Text].trim compareTo w2.asInstanceOf[Text].trim
  }
}


class LinkNameComparator protected () extends WritableComparator(classOf[Text], true) {

  override def compare(w1: WritableComparable[_], w2: WritableComparable[_]): Int = {
    w1.asInstanceOf[Text].trim compareTo w2.asInstanceOf[Text].trim match {
      case 0 => w1.asInstanceOf[Text] compareTo w2.asInstanceOf[Text]
      case result @ _ => result
    }
  }
}

class FilterLinksReducer(context: ReduceContext[_,_,_,_]) extends SimpleReducer[Text, Text, Text, Text](context) with Logging {

  override def reduce(_link: Text, titles: Iterator[Text]): Unit = {
    println(s"""get ${_link}""")

    if (_link.toString == "") {
      val titlesSet: Set[Text] = titles.toSet - new Text()
      println(s"""keep all titles ${titlesSet mkString " | " }""")
      titlesSet foreach { title =>
        emit(title, "NULL")
        // emit(title, "")
        // emit("NULL", title)
      }
    }
    else if (_link startsWith " ") {
      val link: Text = _link.toString drop 1
      val titlesSet: Set[Text] = titles.toSet - new Text()
      println(s"""accept $link ${titlesSet mkString " | " }""")
      if (titlesSet.size >= 1){ // dangling titles or normal titles
        emit(link, "NULL")     
        emit("NULL", link)      // NULL ->
      }
      titlesSet foreach { title =>
        emit(title, link)
        emit("NULL", title)
      }
      // empty titles

    }
    else println(s"""reject non existing link ${_link} """)
  }
}



// class TextTextIdentityMapper(context: MapContext[_,_,_,_])
//     extends IdentityMapper[Text, Text](context)

// class ConcatLinksReducer(context: ReduceContext[_,_,_,_])
//     extends SimpleReducer[Text, Text, Text, PageRankLinks](context) with Logging {
//   override def reduce(title: Text, links: Iterator[Text]): Unit = {
//     val linksArray = links.toArray
//     if (linksArray.length > 0)
//     else
//     {
//     }
//   }
// }


// import scala.collection.JavaConversions._


class SortPRComparator protected () extends WritableComparator(classOf[DoubleAndTextWC], true) {

  override def compare(w1: WritableComparable[_], w2: WritableComparable[_]): Int = {
    - (w1.asInstanceOf[DoubleAndTextWC] compareTo w2.asInstanceOf[DoubleAndTextWC])
  }
}


object PageRankMapReduce extends MapReduceMain {

  val nullText = new Text("NULL")

  def idendMap(input: Iterator[(Text, Text)], ctx: LambdaMapContext): Iterator[(Text, Text)] =
    for ((title, link) <- input) yield (title, link)


  def concatLinksReduce(input: Iterator[(Text, Iterator[Text])], ctx: LambdaReduceContext): Iterator[(Text, PageRankLinks)] =
    for {
      (title, links) <- input
      linksArray = (for {
        link <- links.toSet
        // if link != nullText
        if link.toString != ""
      } yield link).toArray
    } yield {
      ctx._context.getCounter("pageRank", "titleCount").increment(1)
      linksArray.length match {
        case 0 =>
          println(s"empty title: $title")
          (title, new PageRankLinks(1, Array[String]()))
        case 1 =>
          println(s"NULL title: $title")
          (title, new PageRankLinks(1, linksArray))
        case _ =>
          println(s"normal title: $title")
          val linksArrayNoNull = for {
            link <-linksArray
            if link.toString != "NULL"
          } yield link
          (title, new PageRankLinks(1, linksArrayNoNull))
      }
    }

  def pageRankMap(input: Iterator[(Text, PageRankLinks)], ctx: LambdaMapContext): Iterator[(Text, PageRankLinks)] =
    input flatMap { item =>
      val (title, pageRankLinks) = item
      val (pageRank, links: Array[Text]) = (pageRankLinks.pageRank, pageRankLinks.links.toArray)

      (title, new PageRankLinks(0, pageRankLinks.links)) :: {
        for (link <- links.toList)
        yield (link, new PageRankLinks(pageRank / links.length, new TextArrayWritable))
      }
    }

  def pageRankReduce(input: Iterator[(Text, Iterator[PageRankLinks])], ctx: LambdaReduceContext): Iterator[(Text, PageRankLinks)] =
    for {
      (title, manyPageRankLinks) <- input
    } yield {
      // val numTitles = ctx._context.getCounter("pageRank", "titleCount").getValue
      val numTitles:Double = ctx._context.getConfiguration.get("numTitles").toInt

      val alpha = 0.15
      print(s"$title => " )

      val (prevPageRank, pageRank, links) = manyPageRankLinks.foldLeft(0: Double, 0: Double, new TextArrayWritable(Array[Text]()) ){
        (pRLinksTuple, pRLinks) =>
        print(s" $pRLinks" )
        if (pRLinks.links.get.length != 0)
          (pRLinks.pageRank, pRLinksTuple._2, pRLinks.links)
        else
          (pRLinksTuple._1, pRLinksTuple._2 + pRLinks.pageRank, pRLinksTuple._3)
      }
      println()

      ctx._context.getCounter("pageRank", "sumChange").increment( math.abs(pageRank - prevPageRank)*1000 toInt )


      (title, new PageRankLinks((alpha*(1/numTitles)+(1-alpha) * pageRank), links))
    }


  def sortPRMap(input: Iterator[(Text, PageRankLinks)], ctx: LambdaMapContext): Iterator[(DoubleAndTextWC, NullWritable)] =
    for {
      (title, pageRankLinks) <- input
    } yield {
      (new DoubleAndTextWC(pageRankLinks.pageRank, title), NullWritable.get )
    }

  def sortPRReduce(input: Iterator[(DoubleAndTextWC, Iterator[NullWritable])], ctx: LambdaReduceContext): Iterator[(Text, Text)] =
    for {
      (pageRankTitle, ignoredNull) <- input
    } yield {
      (pageRankTitle._2, pageRankTitle._1.toString)
    }




  override def run(conf: Configuration, args: Array[String]): Int = {
    val inputDirs = args.init
    val outputDir = args.last

    // val numTitles = conf.get("numTitles").toInt

    val pipeline = MapReducePipeline.init(conf) -->
    new InputOutput.TextFileSource(inputDirs) -->
    new MapReduceJob(classOf[ExtractInfoMapper], classOf[FilterLinksReducer], "step 1: Build Graph") ++
    LambdaJobModifier { job =>
      job.setGroupingComparatorClass(classOf[GroupComparator])
      job.setPartitionerClass(classOf[TitlePartitioner])
      job.setSortComparatorClass(classOf[LinkNameComparator])} -->
    new MapReduceJob(idendMap _, concatLinksReduce _, "step 1: concatLinks") -->
    // new MapReduceJob(pageRankMap _, pageRankReduce _, "step 3: pageRank") -->
    // new InputOutput.TextFileSink[Text, PageRankLinks](s"$outputDir-graph")
    new InputOutput.SequenceFileSink[Text, PageRankLinks](s"$outputDir-0")
    val (isSuccess, jobs ) = pipeline.execute
    if ( isSuccess == false)
      return 1
    val numTitles = jobs.head.getCounters.findCounter(REDUCE_OUTPUT_RECORDS).getValue
    conf.set("numTitles", numTitles.toString)

    println(s"\033[1;32mnumTitles = $numTitles\033[m")

    (1 to 50).toStream map { i => 
      val pipelinePR = MapReducePipeline.init(conf) -->
      new InputOutput.SequenceFileSource[Text, PageRankLinks](Array(s"$outputDir-${i-1}")) -->
      new MapReduceJob(pageRankMap _, pageRankReduce _, s"step2: pageRank-$i") -->
      new InputOutput.SequenceFileSink[Text, PageRankLinks](s"$outputDir-$i")
      val (isSuccess, jobs ) = pipelinePR.execute
      if ( isSuccess == false)
        return 1
      val avgChange = jobs.head.getCounters.findCounter("pageRank", "sumChange").getValue / numTitles.toDouble / 1000
      println(s"\033[1;31mPageRank $i iteration. avgChange = $avgChange\033[m")
      (i, avgChange)
    } takeWhile (x => x._1 < 10 || x._2 > 0.2) toList

    println(s"\033[1;32mPageRank Sort\033[m")

    val pipelineSort = MapReducePipeline.init(conf) -->
    new InputOutput.SequenceFileSource[Text, PageRankLinks](Array(s"$outputDir-10")) -->
    new MapReduceJob(sortPRMap _, sortPRReduce _, "step3: sort pageRank") ++
    LambdaJobModifier { job =>
      job.setSortComparatorClass(classOf[SortPRComparator])} -->
    new InputOutput.TextFileSink[Text, Text](s"$outputDir-final")
    if ( pipelineSort.execute._1 == false)
      return 1
    return 0


    // hint: use ++ to add ConfModifiers or JobModifiers to a TaskStage or a StandAloneJob
    // ConfigureSpeculativeExecution(false, false) ++
    // LambdaJobModifier { _.setNumReduceTasks(1) } --> // hint: use --> to chain MR jobs into pipelines
    // new MapReduceJob(classOf[CombineCountAndWordIntoTupleMapper], classOf[OutputSortedCountsReducer],
    // "ScaMR sorted word count example, stage 2") -->

  }
}


