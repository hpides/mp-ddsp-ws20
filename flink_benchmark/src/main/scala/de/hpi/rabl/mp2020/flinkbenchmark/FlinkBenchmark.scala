package de.hpi.rabl.mp2020.flinkbenchmark

import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkBenchmark {

  def main(args: Array[String]): Unit = {
    println("Up and running...")
    if (args.indexOf("--adPort") == -1) System.exit(1)
    if (args.indexOf("--checkoutPort") == -1) System.exit(1)
    if (args.indexOf("--ip") == -1) System.exit(1)
    if (args.indexOf("--outputPath") == -1) System.exit(1)

    val ip: String = args(args.indexOf("--ip") + 1)
    val outputPath: String = args(args.indexOf("--outputPath") + 1)
    val adPort: Int = args(args.indexOf("--adPort") + 1).toInt
    val checkoutPort: Int = args(args.indexOf("--checkoutPort") + 1).toInt

    // get the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by reading a text file
    val adStream = env
      .socketTextStream(ip, adPort)
      .map[(Int, Int, Double, Long, Long, Long)](splitAdStream(_))
      .keyBy(_._1)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))
      .reduce {
        (ad1, ad2) =>
          (ad1._1, 0, ad1._3 + ad2._3, Math.max(ad1._4, ad2._4), Math.max(ad1._5, ad2._5), ad1._6 + ad2._6)
      }

    val checkoutStream = env
      .socketTextStream(ip, checkoutPort)
      .map[(Int, Int, Int, Double, Long, Long)](splitCheckoutStream(_))
      .filter(_._3 != 0)
      .keyBy(_._3)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))
      .reduce {
        (checkout1, checkout2) =>
          (0, 0, checkout1._3, checkout1._4 + checkout2._4, Math.max(checkout1._5, checkout2._5), Math.max(checkout1._6, checkout2._6))
      }

    val joinedStream = adStream
      .join(checkoutStream)
      .where(_._1)
      .equalTo(_._3)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1))) { (ad, checkout) =>
        val eventTime = Math.max(ad._4, checkout._5)
        val processingTimeStart = if (eventTime == ad._4) ad._5 else checkout._6
        (ad._1, checkout._4 - ad._3, eventTime, processingTimeStart, ad._6)
      }

    joinedStream
      .map[(Int, Double, Long, Long, Long, Long)] { t: (Int, Double, Long, Long, Long) =>
        (t._1, t._2, t._3, t._4, System.currentTimeMillis(), t._5)
      }
      .writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE)

    env.execute("FlinkBenchmark")
  }

  // ad_id, user_id, cost, timestamp event, timestamp processing
  def splitAdStream(string: String): (Int, Int, Double, Long, Long, Long) = {
    val array = string.split(",")
    (array(0).toInt, array(1).toInt, array(2).toDouble, array(3).toLong, System.currentTimeMillis(), 1)
  }

  // purchase_id, user_id, ad_id, value, timestamp event, timestamp processing
  def splitCheckoutStream(string: String): (Int, Int, Int, Double, Long, Long) = {
    val array = string.split(",")
    (array(0).toInt, array(1).toInt, array(2).toInt, array(3).toDouble, array(4).toLong, System.currentTimeMillis())
  }
}

