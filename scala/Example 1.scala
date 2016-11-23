import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark._
import java.util.zip.ZipInputStream
import scala.io.Source

object example {

	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf()                                         //create a new spark config
		val sc = new SparkContext(sparkConf)                                    //build a spark context
		 
		sc.binaryFiles("/user/example/zip_dir", 10)                             //make an RDD from *.zip files in HDFS
			.flatMap((file: (String, PortableDataStream)) => {                  //flatmap to unzip each file
				val zipStream = new ZipInputStream(file._2.open)                //open a java.util.zip.ZipInputStream
				val entry = zipStream.getNextEntry                              //get the first entry in the stream
				val iter = Source.fromInputStream(zipStream).getLines           //place entry lines into an iterator
				iter.next                                                       //pop off the iterator's first line
				iter                                                            //return the iterator
			})
			.saveAsTextFile("/user/example/quoteTable_csv/result.csv")          //save the RDD to a text file in HDFS
	}
}