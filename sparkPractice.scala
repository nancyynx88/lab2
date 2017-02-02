
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by Yang NX on 2/1/2017.
  */
object sparkPractice {
  def main(args: Array [String]) {
    System.setProperty("hadoop.home.dir","C:\\Users\\Yang NX\\Documents\\winutils");
    val sparkConf=new sparkConf().setAppName("sparkPractice").setMaster("local[*]")
    val sc = new SparkContext(SparkConf)
    val input = sc.textFile ("input.txt")
    val wc = input.flatMap(line=>{line.split(" ")}).map(word=>(word,1)).cache()
    val output = wc.reduceByKey(_+_)
    output.saveAsTextFile("output")
    val o = output.collect()
    var s:String="Words:Count \n"
    o.foreach{case(word,count)=>{

      s+=word + " : " + count + "\n"
    }}

  }
}