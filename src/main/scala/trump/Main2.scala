package trump

import com.mongodb.util.JSON
import com.mongodb.{BasicDBList, BasicDBObject}
import org.apache.spark.SparkContext

object Main2 {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext("local[*]","test")
    val rdd = sparkContext.textFile(args(0),12)

    rdd.flatMap { line =>
        try{
          val mentions: Array[BasicDBObject] = JSON.parse(line).asInstanceOf[BasicDBObject]
            .get("entities").asInstanceOf[BasicDBObject]
            .get("user_mentions").asInstanceOf[BasicDBList]
            .toArray().map(_.asInstanceOf[BasicDBObject])
          mentions
        }catch {case e:Exception =>
          Array[BasicDBObject]()
        }
      }
      .map { mention => (mention.getString("screen_name"), 1) }
      .reduceByKey(_ + _)
      .groupBy { case (user, mentionsCount) => mentionsCount }
      .map {case (mentionsCount: Int, users: Iterable[(String, Int)]) => (mentionsCount, users.toSet.size)}
      .collect()
      .sortBy(_._1)
      .foreach{case (mentionsCount, usersSize) => println(s"$mentionsCount,$usersSize")}
  }
}