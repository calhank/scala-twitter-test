
// package org.apache.spark.streaming.twitter
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

object Main extends App {

	if ( args.length < 3 ) {
		System.err.println("Usage: <num_seconds_per_streaming_rdd> <total_runtime> <num top hashtags> <filtertext>...")
		System.exit(1)
	}

	val window = args(0).toInt
    val runtime = args(1).toInt
    val top = args(2).toInt
    val filters = args.takeRight( args.length - 3 )

	println("\nTwitter Hashtag Streaming\nBatch Length: %s seconds\nRuntime : %s seconds\nNumber of Top Tweets: %s\nFilters: %s\n".format(window, runtime, top, filters.mkString(", ")))

    System.setProperty("twitter4j.oauth.consumerKey", "U2UbUq8DX2WlbfbR9m3nY0tcW")
    System.setProperty("twitter4j.oauth.consumerSecret", "m4NHVi738gGyzD4pNLP8lxuoGOlV4TqUwKL0MHqtm1sGwkB9lV")
    System.setProperty("twitter4j.oauth.accessToken", "286789101-m9DbIXjfU5zeddtIsMRtgQ5DeNy64hnDOVr2KFnB")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "H8DSy6MrLmMNnqk9IJh4JiTuk0XsDAmTfNgwmcb9OuQvk")

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
    val ssc = new StreamingContext(sparkConf, Seconds( window ))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    val tweets = stream.map(status => ( status.getUser().getScreenName(), status.getText().split(" ") ) )

    val parsedTweets = tweets.map{ case (user, text) => (user,
    	text.filter(_.startsWith("#")),
    	text.filter(_.startsWith("@")) 
    	) }

    val parsedTweetsWithHash = parsedTweets.filter { 
    	case (_, hashtags, _) => hashtags.length > 0 
    }

	val hashfirst = parsedTweetsWithHash.flatMap {
		case(user, hashtags, ats) => hashtags.map( 
			tag => ( tag, "(" + user + " " + ats.mkString(", ") + ")" ) 
			)  
	}

	// val hashfirst = parsedTweetsWithHash.flatMap{ case(user, hashtags, ats) => hashtags.map( tag => ( tag, (user, ats)) )  }
	
	hashfirst.persist(StorageLevel.OFF_HEAP)

	val aggregatedHashtags = hashfirst.window(Seconds(runtime), Seconds(window)).combineByKey( 
		(users: String) => (users, 1),
		(combiner: (String, Int), users: String) => {
			val (new_users, count) = combiner
			( new_users + " " + users, count + 1 )
			},
		(first: (String, Int), second: (String, Int)) => {
			val (users1, count1) = first
			val (users2, count2) = second
			(users1 + " " + users2, count1 + count2)
			},
		new org.apache.spark.HashPartitioner(5))
	.map{ case (tag, (users, count)) => (count, (tag, users))}
	.transform(_.sortByKey(false))

	aggregatedHashtags.foreachRDD( rdd => {
		println("\nTop %s Results for Last %s Seconds:".format(top, window))
		rdd.collect().take(top).foreach{ case ( num, (tag, users) ) => {
			println("::: %s | %s appearances\nUSERS: %s".format( tag, num, users ) )
			} }
		})

	ssc.start()
	ssc.awaitTerminationOrTimeout(runtime * 1000)
	ssc.stop(true, true)
	System.exit(1)

}
