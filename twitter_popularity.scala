
// package org.apache.spark.streaming.twitter
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

object Main extends App {

	if ( args.length < 2 ) {
		System.err.println("Usage: <num_seconds_per_streaming_rdd> <filtertext>...")
		System.exit(1)
	}

	println(s"I got executed with ${args size} args, they are: ${args mkString ", "}")

    val filters = args

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", "U2UbUq8DX2WlbfbR9m3nY0tcW")
    System.setProperty("twitter4j.oauth.consumerSecret", "m4NHVi738gGyzD4pNLP8lxuoGOlV4TqUwKL0MHqtm1sGwkB9lV")
    System.setProperty("twitter4j.oauth.accessToken", "286789101-m9DbIXjfU5zeddtIsMRtgQ5DeNy64hnDOVr2KFnB")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "H8DSy6MrLmMNnqk9IJh4JiTuk0XsDAmTfNgwmcb9OuQvk")

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    println(s"I got executed with ${args size} args, they are: ${args mkString ", "}")

    // def get_user_and_text()

    val statuses = stream.map(status => status.getUser().getScreenName())
    statuses.print()
    statuses.saveAsTextFiles("tachyon://localhost:19998/users")


    // val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    // val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
    //                  .map{case (topic, count) => (count, topic)}
    //                  .transform(_.sortByKey(false))

    // val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
    //                  .map{case (topic, count) => (count, topic)}
    //                  .transform(_.sortByKey(false))


    // // Print popular hashtags
    // topCounts60.foreachRDD(rdd => {
    //   val topList = rdd.take(10)
    //   println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
    //   topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    // })

    // topCounts10.foreachRDD(rdd => {
    //   val topList = rdd.take(10)
    //   println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
    //   topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    // })


    ssc.start()
    ssc.awaitTermination()

    statuses.saveAsTextFiles("tachyon://localhost:19998/users2")

}
