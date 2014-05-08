package cc.nlplab

import scamr.io.tuples.Tuple2WritableComparable
import org.apache.hadoop.io.{ Text, Writable, IntWritable}
import java.io.{DataInput, DataOutput}
import scala.collection.mutable.ArrayBuffer


class TextPairWC(tuple: (Text, Text)) extends Tuple2WritableComparable[Text, Text](tuple) {
  def this (a1: Text, a2: Text) = this((a1, a2))
  def this() = this((new Text, new Text))
}

class TextAndIntWC(tuple: (Text, IntWritable)) extends Tuple2WritableComparable[Text, IntWritable](tuple) {
  def this (a1: Text, a2: IntWritable) = this((a1, a2))
  def this() = this((new Text, new IntWritable))
}

class TermInfo(var title: String, var tf: Int, var ofs: ArrayBuffer[Long]) extends Writable {
  def this() = this("", 0, ArrayBuffer[Long]())

  override def toString: String =
    s"""TermInfo($title, $tf, [${ofs.mkString(", ")}])"""
  override def readFields(in: DataInput) : Unit = {
    val titleText = new Text()
    titleText.readFields(in)
    title = titleText.toString

    tf = in.readInt

    // if (ofs == null)
    //   ofs = Vector[Int]()
    ofs = for (i <- ArrayBuffer.range(0, in.readInt)) yield in.readLong  
  }
  override def write(out: DataOutput): Unit = {
    new Text(title).write(out)
    out.writeInt(tf)

    out.writeInt(ofs.size)
    ofs foreach {out.writeLong(_)}
  }
}


class ArrayBufferW[T <% Writable ](implicit m: scala.reflect.Manifest[T]) extends ArrayBuffer[T] with Writable {
   def readFields(in: DataInput) {
     this.clear
     for (i <- 1 to in.readInt) {
       val data = m.erasure.newInstance.asInstanceOf[T]
       data.readFields(in)
       this += data
     }
  }
  def write(out: DataOutput) {
    out.writeInt(this.size)
    this foreach { data: T => data.write(out) }
  }
}
// implicit def arrayBufferW[T <% Printable](a: ArrayBuffer[T]) = new PrintableArray(a)
