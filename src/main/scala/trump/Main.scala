package trump

import com.mongodb.util.JSON
import com.mongodb.{BasicDBList, BasicDBObject}
import org.apache.spark.SparkContext

object Main {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext("local[*]","test")
    val rdd = sparkContext.textFile(args(0),12)
//    val rdd = Source.fromFile(args(0)).getLines()
    val count = rdd
      .map{ line =>
        try{
          val count = JSON.parse(line).asInstanceOf[BasicDBObject]
            .get("entities").asInstanceOf[BasicDBObject]
            .get("user_mentions").asInstanceOf[BasicDBList]
            .toArray().length
          (line, count)
        }catch {case e:Exception =>
          println(line)
          ("", 0)
        }
      }
//      .count{ count => count > 0}
      .filter{ case (line, count) => count > 0}
      .map(_._1)
      .repartition(1)
      .saveAsTextFile("/mnt/share/Petrov/mongodata/Trump/mentionTweets")

    println("Count "+ count)
//      .foreach(println)
  }
}