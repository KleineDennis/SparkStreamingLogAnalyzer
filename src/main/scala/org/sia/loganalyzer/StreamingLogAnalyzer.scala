package org.sia.loganalyzer

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.matching.Regex

/**
 * Spark Streaming application for analyzing access logs coming as text messages from a Kafka topic,
 * calculating various statistics and writing the results back to Kafka, to a different topic.
 *
 * The incoming messages should be in this format:
 * <yyyy-MM-dd> <hh:mm:ss.SSS> <client IP address> <client session ID> <URL visited> <HTTP method> <response code> <response duration>
 *
 * The statistics calculated are:
 *     - number of active sessions (those sessions whose last request happened less than sessionTimeout seconds ago).
 *     - number of requests per second
 *     - number of errors per second
 *     - number of ads per second
 */
object StreamingLogAnalyzer {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(s"""
        |Usage: DirectKafkaWordCount <brokers> <groupId> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <groupId> is a consumer group name to consume from topics
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, groupId, topics) = args

    val conf = new SparkConf().setAppName("Spark-Streaming Log Analyzer") //.setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("/Users/denniskleine/Documents/workspace/checkpoint")

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])

    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    case class LogLine(time: Long, ipAddr: String, sessId: String, url: String, method: String, respCode: Int, respTime: Int)

    val df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")

    // Get the lines for tests
    val lines = kafkaStream.map(_.value)
    lines.print()

    val logsStream = kafkaStream.flatMap{ t => {
      val fields = t.value().split(" ")
      try {
        List(LogLine(df.parse(fields(0) + " " + fields(1)).getTime(), fields(2), fields(3), fields(4), fields(5), fields(6).toInt, fields(7).toInt))
      }
      catch {
        case e: Exception => { System.err.println("Wrong line format: "+t); List() }
      }
    }}


    //logStream contains parsed LogLines
//    val logsStream = kafkaStream.flatMap { t => {
//      val fields = t.value.split(" ")
//      try {
//        List(LogLine(df.parse(fields(0) + " " + fields(1)).getTime(), fields(2), fields(3), fields(4), fields(5), fields(6).toInt, fields(7).toInt))
//      }
//      catch {
//        case e: Exception => { System.err.println("Wrong line format: "+t); List() }
//      }
//    }}


{
//    ///////////////////////////////////////////////////////////////
//    //CALCULATE NUMBER OF SESSIONS
//    ///////////////////////////////////////////////////////////////
//
//    //contains session id keys its maximum (last) timestamp as value
//    val maxTimeBySession = logsStream.map(r => (r.sessId, r.time)).reduceByKey(
//      (max1, max2) => {
//        Math.max(max1, max2)
//      })//reduceByKey
//
//    //update state by session id
//    val stateBySession = maxTimeBySession.updateStateByKey((maxTimeNewValues: Seq[Long], maxTimeOldState: Option[Long]) => {
//      if (maxTimeNewValues.size == 0) { //only old session exists
//        //check if the session timed out
//        if (System.currentTimeMillis() - maxTimeOldState.get > SESSION_TIMEOUT_MILLIS)
//          None //session timed out so remove it from the state
//        else
//          maxTimeOldState //preserve the current state
//
//      } else if (maxTimeOldState.isEmpty) //this is a new session; no need to check the timeout
//          Some(maxTimeNewValues(0)) //create the new state using the new value (only one new value is possible because of the previous reduceByKey)
//        else //both old and new events with this session id found; no need to check the timeout
//          Some(Math.max(maxTimeNewValues(0), maxTimeOldState.get))
//    })//updateStateByKey
//
//    //returns a DStream with single-element RDDs containing only the total count
//    val sessionCount = stateBySession.count()
//
//    //logLinesPerSecond contains (time, LogLine) tuples
//    val logLinesPerSecond = logsStream.map(l => ((l.time / 1000) * 1000, l))
//
//
//    ///////////////////////////////////////////////////////////////
//    //CALCULATE REQUESTS PER SECOND
//    ///////////////////////////////////////////////////////////////
//
//    //this combineByKey counts all LogLines per unique second
//    val reqsPerSecond = logLinesPerSecond.combineByKey(
//      l => 1L,
//      (c: Long, ll: LogLine) => c + 1,
//      (c1: Long, c2: Long) => c1 + c2,
//      new HashPartitioner(numberPartitions),
//      true)
//
//
//    ///////////////////////////////////////////////////////////////
//    //CALCULATE ERRORS PER SECOND
//    ///////////////////////////////////////////////////////////////
//    val errorsPerSecond = logLinesPerSecond.
//      //leaves in only the LogLines with response code starting with 4 or 5
//      filter(l => { val respCode = l._2.respCode / 100; respCode == 4 || respCode == 5 }).
//      //this combineByKey counts all LogLines per unique second
//      combineByKey(r => 1L,
//        (c: Long, r: LogLine) => c + 1,
//        (c1: Long, c2: Long) => c1 + c2,
//        new HashPartitioner(numberPartitions),
//        true)
//
//
//    ///////////////////////////////////////////////////////////////
//    //CALCULATE NUMBER OF ADS PER SECOND
//    ///////////////////////////////////////////////////////////////
//    val adUrlPattern = new Regex(".*/ads/(\\d+)/\\d+/clickfw", "adtype")
//    val adsPerSecondAndType = logLinesPerSecond.
//      //filters out the LogLines whose URL's don't match the adUrlPattern.
//      //LogLines that do match the adUrlPattern are mapped to tuples ((timestamp, parsed ad category), LogLine)
//      flatMap(l => {
//        adUrlPattern.findFirstMatchIn(l._2.url) match {
//          case Some(urlmatch) => List(((l._1, urlmatch.group("adtype")), l._2))
//          case None => List()
//        }
//      }).
//      //this combineByKey counts all LogLines per timestamp and ad category
//      combineByKey(r => 1.asInstanceOf[Long],
//        (c: Long, r: LogLine) => c + 1,
//        (c1: Long, c2: Long) => c1 + c2,
//        new HashPartitioner(numberPartitions),
//        true)
//
//    //data key types for the output map
//    val SESSION_COUNT = "SESS"
//    val REQ_PER_SEC = "REQ"
//    val ERR_PER_SEC = "ERR"
//    val ADS_PER_SEC = "AD"
//
//    //maps each count to a tuple (timestamp, a Map containing the count under the REQ_PER_SEC key)
//    val requests = reqsPerSecond.map(sc => (sc._1, Map(REQ_PER_SEC -> sc._2)))
//    //maps each count to a tuple (timestamp, a Map containing the count under the ERR_PER_SEC key)
//    val errors = errorsPerSecond.map(sc => (sc._1, Map(ERR_PER_SEC -> sc._2)))
//    //maps each count to a tuple (current time with milliseconds removed, a Map containing the count under the SESSION_COUNT key)
//    val finalSessionCount = sessionCount.map(c => ((System.currentTimeMillis / 1000) * 1000, Map(SESSION_COUNT -> c)))
//    //maps each count to a tuple (timestamp, a Map containing the count per category under the key ADS_PER_SEC#<ad category>)
//    val ads = adsPerSecondAndType.map(stc => (stc._1._1, Map(s"$ADS_PER_SEC#${stc._1._2}" -> stc._2)))
//
//    //all the streams are unioned and combined
//    val finalStats = finalSessionCount.union(requests).union(errors).union(ads).
//      //and all the Maps containing particular counts are combined into one Map per timestamp.
//      //This one Map contains all counts under their keys (SESSION_COUNT, REQ_PER_SEC, ERR_PER_SEC, etc.).
//      reduceByKey((m1, m2) => m1 ++ m2)
//
//    //Each partitions uses its own Kafka producer (one per partition) to send the formatted message
//    finalStats.foreachRDD(rdd => {
//      rdd.foreachPartition(partition => {
//        KafkaProducerWrapper.brokerList = brokerList.get
//        val producer = KafkaProducerWrapper.instance
//        partition.foreach {
//          case (s, map) =>
//            producer.send(
//              statsTopic.get,
//              s.toString,
//              s.toString + ":(" + map.foldLeft(new Array[String](0)) { case (x, y) => { x :+ y._1 + "->" + y._2 } }.mkString(",") + ")")
//        }//foreach
//      })//foreachPartition
//    })//foreachRDD
}


    println("Starting the streaming context... Kill me with ^C")

    ssc.start()
    ssc.awaitTermination()
  }

}


/**
 * Case class and the matching object for lazily initializing a Kafka Producer.
 */
case class KafkaProducerWrapper(brokerList: String) {
  val producerProps = {
    val prop = new Properties
    prop.put("metadata.broker.list", brokerList)
    prop
  }
  val p = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)

//  val  props = new Properties()
//  props.put("bootstrap.servers", "localhost:9092")
//  props.put("acks", "0")
//  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//
//  val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)

  def send(topic: String, key: String, value: String) {
    p.send(new ProducerRecord(topic, key.toCharArray.map(_.toByte), value.toCharArray.map(_.toByte)))
//    producer.send(new ProducerRecord(topic, key.toCharArray.map(_.toByte), value.toCharArray.map(_.toByte)))
  }
}

object KafkaProducerWrapper {
  var brokerList = ""
  lazy val instance = new KafkaProducerWrapper(brokerList)
}

