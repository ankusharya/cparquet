import $ivy.`com.github.mjakubowski84::parquet4s-core:1.7.0`
import $ivy.`org.apache.hadoop:hadoop-client:3.3.0`
import $ivy.`com.lihaoyi::fansi:0.2.10`

import fansi.Color._
object ColorList {

  val colorList = List(Cyan,LightBlue,LightGray,LightMagenta, LightYellow, Red, Yellow, Blue, DarkGray, Green, LightCyan, LightGreen, LightRed,Magenta,
    Cyan,LightBlue,LightGray,LightMagenta, LightYellow, Red, Yellow, Blue, DarkGray, Green, LightCyan, LightGreen, LightRed,Magenta,
    Cyan,LightBlue,LightGray,LightMagenta, LightYellow, Red, Yellow, Blue, DarkGray, Green, LightCyan, LightGreen, LightRed,Magenta,
    Cyan,LightBlue,LightGray,LightMagenta, LightYellow, Red, Yellow, Blue, DarkGray, Green, LightCyan, LightGreen, LightRed,Magenta)
}

trait ColorDisplay {
  def display(seq: Seq[String]) = {
    println(seq.zipWithIndex.map { case (x, i) => ColorList.colorList(i)(x) }.
      mkString(","))
  }
}
import com.github.mjakubowski84.parquet4s.{BinaryValue, DoubleValue, FloatValue, IntValue, LongValue, NullValue, ParquetIterable, ParquetReader, RowParquetRecord}

class ParquetUtil(file: String) extends ColorDisplay {
  private def read: ParquetIterable[RowParquetRecord] =
    ParquetReader.read[RowParquetRecord](file, options = ParquetReader.Options())


  private def header(seq: RowParquetRecord) = {
    seq.map(x => x._1).toSeq
  }

  private def value(seq: RowParquetRecord): Seq[String] = {
    seq.map(x => x._2 match {
      case LongValue(value) => value.toString
      case IntValue(value) => value.toString
      case FloatValue(value) => value.toString
      case DoubleValue(value) => value.toString
      case BinaryValue(value) => value.toStringUsingUTF8
      case NullValue => ""
      case _ => ""

    }).toSeq
  }

  def process = {
    val itr = read.iterator
    val first = itr.next()
    val colNames = header(first)
    val firstValue = value(first)

    // Display column-names
    displayData(colNames)

    // Display first row
    displayData(firstValue)

    // Display the rest
    while( itr.hasNext){
      val nextData = itr.next()
      displayData(value(nextData))
    }
  }

  private def displayData(g:Seq[String]) = display(g)

}

object ParquetUtil {
  def apply(file: String) = new ParquetUtil(file)
}


@main
def main(path: String) = {

ParquetUtil(path).process
}
