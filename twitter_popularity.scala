object Main extends App {

	import org.apache.spark._
	import org.apache.spark.streaming._
	import org.apache.spark.streaming.twitter._

	println(s"I got executed with ${args size} args, they are: ${args mkString ", "}")

	// your code goes here
	val ssc = new StreamingContext(conf, Seconds(2))
	val sparkConf = new SparkConf().setAppName("twitter_popularity")
	val stream = TwitterUtils.createStream(ssc, None)
	val users = stream.map(status => status.getUser.getId)

	users.print()

    ssc.start()
    ssc.awaitTermination()


}
