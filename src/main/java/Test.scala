import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object Test {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
//    val benv = ExecutionEnvironment.getExecutionEnvironment
//    val data = benv.readTextFile("")
//    data.map((_,1L)).groupBy(0).sum(1).print()

    val data = senv.socketTextStream("192.168.181.10",9999,'\n')

    val wordCounts= data
//      .flatMap{w=>w.split("\\s")}
      .map(w=>WordCount(w.split(",")(0),w.split(",")(1).toLong))
//      .map(w=>WordCount(w,1))
      .keyBy("word")
      .max("count")
//      .countWindowAll(5)
//      .countWindow(10,2)
//      .timeWindow(Time.seconds(5),Time.seconds(1))
//      .sum("count")

    wordCounts.print().setParallelism(1)
    senv.execute("Stream")
  }
  case class WordCount(word:String,count:Long)
}
