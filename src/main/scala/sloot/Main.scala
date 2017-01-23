package sloot

import com.mongodb.BasicDBObject
import com.mongodb.util.JSON
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
/**
  * Created by max on 12.01.17.
  */
object Main {
  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext("local[*]","test")
    val textkey = "filtered_text"

    val textFile = sparkContext.textFile("/home/max/PycharmProjects/SlootTopicAnalisys/postsfiltered.json", 12)
    val jsonRdd = textFile.map(line => JSON.parse(line).asInstanceOf[BasicDBObject])

    val data = sparkContext.parallelize( jsonRdd.filter{json => json.getString(textkey).length < 100}.take(5000))
    val texts = data.map(json => { json.getString(textkey).replace("\n"," ").split(" ").toSeq })


    val hashingTF = new HashingTF()
    val tf = hashingTF.transform(texts)
//
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf).cache()

//    val zip = tfidf.zip(jsonRdd)

//    val dict = texts.flatMap(word => word).distinct().map(text => (hashingTF.indexOf(text), text)).collect().toMap

//    tf.foreach(println)
//    tfidf.foreach(println)



    val clusters = KMeans.train(tfidf, 20, 20)


    tfidf
      .map {t => clusters.predict(t)}
      .zip(data)
      .groupBy(_._1)
      .repartition(1)
      .foreach { g=>
        val clusterId = g._1
        println(clusterId,"!"*100)
        val posts = g._2
        posts.foreach(p=> println(p._2.get("key"), p._2.get(textkey)))
      }

//    for (center <- clusters.clusterCenters) {
//      System.out.println(" " + center.toSparse)
//    }


//    clusters.clusterCenters.foreach{(vector: Vector) =>
//
//      val label = clusters.predict(vector)
//      val points = vector.toSparse
//      println(label)
//      println(points)
//      println(points.indices.map { i => dict(i) }.toList)
//      println(vector.toDen.se.values.zipWithIndex.filter(_._1 != 0).toList)
//    }


  }
}
