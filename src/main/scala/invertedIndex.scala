package cc.nlplab

import scala.language.implicitConversions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{ LongWritable, Text, Writable, IntWritable,DoubleWritable, WritableComparator, WritableComparable, NullWritable}
import java.util.Scanner




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



class InvertedIndexMapper(context: MapContext[_, _, _, _]) extends TextInputMapper[TextPairWC, TermInfo](context)  {

  override def map(fileOffset: LongWritable, line: Text): Unit = {

    val titlePattern = """(?<=<title>)[^<]+(?=</title>)""".r
    val textPattern = """(?<=<text[^>]{0,30}>)[^<]+(?=</text>)""".r
    val title = (titlePattern findFirstIn line get).trim
    val textMatch = textPattern findFirstMatchIn line get
    val (textOffset, text) = (textMatch.start, textMatch.matched)

    val sc = new Scanner(text).useDelimiter("[^a-zA-Z]")

    while (sc.hasNext) {
      val term = sc.next
      if (term.length > 0) {
        val scanOffset = sc.`match`.start
        val totalOffset = fileOffset + textOffset + scanOffset
        emit(new TextPairWC(term, title), new TermInfo(title, 1, ArrayBuffer(totalOffset)))
      }
    }
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


class InvertedIndexReducer(context: ReduceContext[_,_,_,_]) extends SimpleReducer[TextPairWC, TermInfo, Text, Text](context)  {

  override def reduce(termTitle: TextPairWC, _termInfos: Iterator[TermInfo]): Unit = {
    val termInfos = Vector() ++ _termInfos
    val termInfos_str = termInfos map { ti =>
      val ofs_tuple = ti.ofs mkString ("(", ",", ")")
      s"${ti.title}#(${ti.tf},${ofs_tuple})"
    } mkString ("[",",","]")

    emit(termTitle._1, s"${termInfos.size}	${termInfos_str}")
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
    new InputOutput.TextFileSink[Text, Text](s"$outputDir")
    val (isSuccess, jobs) = pipeline.execute
    if (!isSuccess) return 1



    return 0
  }

}
