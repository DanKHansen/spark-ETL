import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark._
import java.util.zip.ZipInputStream
import scala.io.Source
import org.apache.commons.csv._

object blog {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()                                         //create a new spark config
    val sc = new SparkContext(sparkConf)                                    //build a spark context
    val sqlContext = new SQLContext(sc)                                     //build a SQLContext
    import sqlContext.implicits._

    sc.binaryFiles("/user/example/zip_dir"                                //make an RDD from *.zip files in HDFS
      .flatMap((file: (String, PortableDataStream)) => {                  //flatmap to unzip each file
    val zipStream = new ZipInputStream(file._2.open)                //open a java.util.zip.ZipInputStream
    val entry = zipStream.getNextEntry                              //get the first entry in the stream
    val iter = Source.fromInputStream(zipStream).getLines           //place entry lines into an iterator
      iter.next;                                                      //pop off the iterator's first line
      iter                                                            //return the iterator
    })
      .map(parse)                                                       //parse RDD into columns
      .toDF(Seq("name", "score", "confidence", "comment") : _*)           //convert RDD to DataFrame
      .filter($"name" !== "EMPTY")                                        //filter out unwanted rows
      .filter($"score" > 50 || $"score" < -50)                            //...
      .filter($"confidence" > 3.0)                                        //...
      .saveAsParquetFile("/user/example/quoteTable/result.parquet")       //write to parquet
  }

  //a helper function to convert a line into a tuple of the correct datatypes using org.apache.commons.csv
  val parse = (line : String) => {
    val parser =                                                            //build a CSVParser
      CSVParser.parse(line, CSVFormat.DEFAULT.withRecordSeparator("\n"))    //with a custom CSVFormat
    val fields = parser.getRecords.get(0)                                   //parse the line into columns
    parser.close                                                            //close the parser
    var tempInt = fields.get(1).toInt                                       //cast int field to int
    if(tempInt < -100)        tempInt = -100                                //clamp int to -100
    else if(tempInt > 100)    tempInt = 100                                 //clamp int to -100
    val tempDouble = fields.get(2).toDouble                                 //cast double field to double
    (fields.get(0), tempInt, tempDouble * tempDouble, fields.get(3))        //return a tuple of our columns
  }
}