package cc.nlplab

import org.apache.hadoop.fs.Path

import scamr.MapReduceMain
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem


import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, HColumnDescriptor}
import org.apache.hadoop.hbase.client.{HBaseAdmin,HTable,Put,Get}
import org.apache.hadoop.hbase.util.Bytes

import org.apache.hadoop.hbase.util.Writables

import org.apache.hadoop.io.{ LongWritable, Text, Writable, IntWritable,DoubleWritable, WritableComparator, WritableComparable, NullWritable}


import org.apache.hadoop.hbase.client.coprocessor.AggregationClient

import org.apache.hadoop.hbase.client.Scan

import scalax.io._

case class PageInfo(title: String, pageRank: Double, offsets: Vector[(Long,Long)])
 
object Query {

  implicit def String2Bytes(str: String) = str.getBytes

  var prTable: HTable = null
  var invTable: HTable = null
  var titleCount: Int = 0


  def retrieveInv(term: String): (Int, TermInfoArray) = {
    val getInfo = new Get(term)
    val res = invTable get getInfo
    val df: IntWritable = Writables.getWritable(res.getValue("ii", "df"), new IntWritable).asInstanceOf[IntWritable]
    val termInfos: TermInfoArray = Writables.getWritable( res.getValue("ii", "ii"), new TermInfoArray ).asInstanceOf[TermInfoArray]
    (df.get, termInfos)
  }

  def retrievePR(page: String): Double = {
    val getInfo = new Get(page)
    val res = prTable get getInfo
    val pr: DoubleWritable = Writables.getWritable(res.getValue("pr", "pr"), new DoubleWritable).asInstanceOf[DoubleWritable]
    // val termInfos: TermInfoArray = Writables.getWritable( res.getValue("ii", "ii"), new TermInfoArray ).asInstanceOf[TermInfoArray]
    pr.get
  }

  def tfIdf(  tf: Double,  df: Double) = tf * Math.log(titleCount / df)

  def main( args: Array[String]): Unit = {
    val conf = new Configuration
    conf.addResource(new Path("core-site.xml"))
    conf.addResource(new Path("hbase-site.xml"))
    conf.addResource(new Path("hdfs-site.xml"))
    val prTblName = args(0)
    val invTblName = args(1)
    titleCount = args(2).toInt
    // val filePath = new Path(args(3))
    val filePath = args(3)
    val terms = args drop 4


    val config = HBaseConfiguration.create(conf)
    // val hdfs = FileSystem.get(conf);

    // val file = io.Source.fromInputStream(hdfs.open(filePath)).toStream
    // val file = Resource.fromInputStream(hdfs.open(filePath))
    val file = Resource.fromRandomAccessFile(new java.io.RandomAccessFile(filePath, "r"))


    val hbase = new HBaseAdmin(config)
    prTable = new HTable(config, prTblName )
    invTable = new HTable(config, invTblName )

    // val ac = AggregationClient(conf)
    // ac.rowCount(prTblName, ) 


    val pages = for {
      term <- terms
      (df, termInfos) = retrieveInv(term)
      termInfo <- termInfos
      pr = retrievePR(termInfo.title)
    } yield {
      PageInfo(termInfo.title, 0.5*tfIdf(termInfo.tf, df) + 0.5*pr, Vector[Tuple2[Long,Long]]() ++ termInfo.ofs)
    }

    pages foreach { pageInfo =>
      println(f"""\033[1;34m-----------------------------------------------------\033[m
                 |\033[1m${pageInfo.title}%-50s${pageInfo.pageRank}%10.4f\033[m""".stripMargin('|'))
      pageInfo.offsets foreach { offset =>
        // val content = file.slice(offset.toInt - 20, offset.toInt + 30).mkString
        val lineOffsetInt = offset._1.toInt
        val textOffsetInt = offset._2.toInt
        val doc = new String(file.bytes.drop(lineOffsetInt).slice(0, textOffsetInt*2).toArray)
        val segment = doc.slice(textOffsetInt - 20, textOffsetInt + 30)
        // val content = (file.bytes.drop(lineOffsetInt).slice(offsetInt - 10 , offsetInt + 20) map ( _.toChar)) .mkString
        // val content = file.chars.slice(offset.toInt -10, offset.toInt + 20).mkString
        println(s"""$offset: $segment""")
      }
    }
  }
}

object QueryTest extends App{


  def time[A](a: => A) = {
    val now = System.nanoTime
    val result = a
    val micros = (System.nanoTime - now) / 1000
    println("%d microseconds".format(micros))
    result
  }

  val conf = new Configuration
  conf.addResource(new Path("core-site.xml"))
  conf.addResource(new Path("hbase-site.xml"))
  conf.addResource(new Path("hdfs-site.xml"))

  val filePath = new Path("/opt/HW2/100M/input-100M")
  val config = HBaseConfiguration.create(conf)
  // val hdfs = FileSystem.get(conf);
  // val file = io.Source.fromInputStream(hdfs.open(filePath)).toStream
  // val file1 = Resource.fromInputStream(hdfs.open(filePath))
  val file2 = Resource.fromRandomAccessFile(new java.io.RandomAccessFile("../input-100M", "r"))

  // time(
  //   file.slice(35670127 - 10, 35670127 + 20).mkString)
  // time(
    // println(file1.chars.slice(35670127 -10, 35670127 + 20).mkString)
  // )
  time(println(file2.chars.slice(35670127 -10, 35670127 + 20).mkString))


}
