
// package org.apache.spark.streaming.twitter
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

object Main extends App {

	if ( args.length < 3 ) {
		System.err.println("Usage: <num_seconds_per_streaming_rdd> <total_runtime> <num top hashtags> <filtertext>...")
		System.exit(1)
	}

	val window = args(0).toInt
    val top = args(1).toInt
    val runtime = args(2).toInt
    val filters = args.takeRight( args.length - 3 )

	println("\nTwitter Hashtag Streaming\nBatch Length: %s seconds\nRuntime : %s seconds\nTop Tweets: %s\nFilters: %s\n".format(args(0), args(1), args(2), filters.mkString(", ")))

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", "U2UbUq8DX2WlbfbR9m3nY0tcW")
    System.setProperty("twitter4j.oauth.consumerSecret", "m4NHVi738gGyzD4pNLP8lxuoGOlV4TqUwKL0MHqtm1sGwkB9lV")
    System.setProperty("twitter4j.oauth.accessToken", "286789101-m9DbIXjfU5zeddtIsMRtgQ5DeNy64hnDOVr2KFnB")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "H8DSy6MrLmMNnqk9IJh4JiTuk0XsDAmTfNgwmcb9OuQvk")

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
    val ssc = new StreamingContext(sparkConf, Seconds( 2 ))
    // val ssc = new StreamingContext(sparkConf, Seconds( 2 ) )
    val stream = TwitterUtils.createStream(ssc, None, filters)

    val tweets = stream.map(status => ( status.getUser().getScreenName(), status.getText().split(" ") ) )

    val parsedTweets = tweets.map{ case (user, text) => (user, text.filter(_.startsWith("#")), text.filter(_.startsWith("@")) ) }

    val parsedTweetsWithHash = parsedTweets.filter{ case (_, hashtags, _) => hashtags.length > 0 }

    // parsedTweetsWithHash.foreachRDD( rdd => {
    // 		rdd.take(10).foreach{ case (user, tags, ats) => println("%s tweeted %s at %s\n".format(user, tags.mkString(", "), ats.mkString(", ") )) }
    // 	})

	// val hashfirst = parsedTweetsWithHash.flatMap{ case(user, hashtags, ats) => hashtags.map( tag => ( tag, (user, ats) ) ) }

	val hashfirst = parsedTweetsWithHash.flatMap{ case(user, hashtags, ats) => hashtags.map( tag => ( tag, user + " " + ats.mkString(" ") + " ") )  }

	hashfirst.foreachRDD( rdd => {
		rdd.take(top).foreach{
			case (tag, users) => println("%s with users: %s".format(tag, users))
			}
		})
	
	// hashfirst.persist(StorageLevel.OFF_HEAP)

	// val aggregatedHashtags = hashfirst.window(Seconds(runtime), Seconds(window)).combineByKey( 
	// 	(tag: Set[String]) => (tag, 1),
	// 	(combiner: (Set[String], Int), tag: Set[String]) => ( combiner._1 ++ tags, combiner._2 + 1 ),
	// 	(comb1: (Set[String], Int), comb2: (Set[String], Int)) => (comb1._1 ++ comb2._1, comb1._2 + comb2._2),
	// 	new org.apache.spark.HashPartitioner(10/2))
	// .map{ case (tag, (users, count)) => (count, (tag, users))}
	// .transform(_.sortByKey(false))

	// aggregatedHashtags.foreachRDD( rdd => {
	// 	out = rdd.collect()
	// 	println("Top Results\n%s".format(out.mkString("\n")))
	// 	})


	// val hashgroup = hashfirst.groupByKey().map{ case (tag, arr) => (tag, arr.foreach{ case ( user, at, num ) => } ) }

	// hashgroup.print()



	// val hashnum = parsedTweetsWithHash.flatMap{ case (user, hashtags, ats) => hashtags.map( (_,1) ) }

    // hashfirst.foreachRDD( rdd => {
    // 		println("\nTop %s Tweets".format(top))
    // 		rdd.take(top).foreach{ case (tag, user, ats) => println("%s by %s at %s".format(tag, user, ats.mkString(", ") )) }
    // 	})

    // val topHashtags = hashnum.reduceByKeyAndWindow(_ + _ , Seconds(2)).map{case(hash, num) => (num, hash)}.transform(_.sortByKey(false)).map{case(num, hash)=>(hash, num)}

    // val topHashtags = hashnum.reduceByKeyAndWindow(_ + _ , Seconds(2)).join(hashfirst).reduceByKey(  ( a, b ) => ( a._1 + b._1  )  )/\

    // topHashtags.print()

    // topHashtags.foreachRDD( rdd => {
    // 	val ranks = rdd.take(top)
    // 	val joinstuff = ranks.map{case(count, tag)=>(tag,count)}.join(hashfirst)
    // 	joinstuff.print()
    // 	} )

    // val joinstuff = topHashtags.join(hashfirst)

    // joinstuff.print()

    // statuses.saveAsTextFiles("http://50.23.16.227:19998/statuses")


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
	ssc.awaitTerminationOrTimeout(runtime * 1000)
	ssc.stop(true, true)

	// println("Top Results:\n%s".format(out.mkString("\n")))

}
