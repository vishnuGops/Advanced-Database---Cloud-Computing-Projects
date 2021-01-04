import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Partition {
  var depth = 6

  def main(args: Array[ String ]): Unit = {
    val conf = new SparkConf().setAppName("Graph Partition")
    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0))

    def readgraph(row:Array[String]): (Long,Long,List[Long])={

      (row(0).toLong,row(0).toLong,row.tail.map(_.toString.toLong).toList)
    }

    var graph = data.map(line => { readgraph(line.split(","))  })
    val oldGraph = graph.map( line => (line._1,line))

    for(i <- 1 to depth){

      graph = graph.flatMap(
        map => map match{ case (a, b, list) => (a, b) :: list.map(x => (x, b) ) } )
        .reduceByKey((a, b) => math.min(a,b))
        .join(oldGraph)
        .map(a => (a._2._2._2, a._2._1, a._2._2._3))

    }
    val graphFinal = graph.map(node => (node._2, 1))
      .reduceByKey(_+_).sortByKey().map {case (a,b) => a+" "+b}

    graph.saveAsTextFile("output")
    graph.foreach(println)
  }

}