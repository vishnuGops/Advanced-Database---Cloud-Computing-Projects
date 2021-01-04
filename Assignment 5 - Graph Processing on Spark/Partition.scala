//Vishnu Gopal Rajan
//1001755911


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Partition {

  val depth = 6

  def main ( args: Array[ String ] ):Unit = {
    val conf = new SparkConf().setAppName("Partition")
    val sc = new SparkContext(conf)
    var count:Int = 0
    val depth:Int = 6
	
	
    var graph = sc.textFile(args(0)).map(line => {
      val data = line.split(",")
	  
	  //the graph cluster ID is set to -1 except for the first 5 nodes 
      var cluster:Long= -1
      if (count < 5) 
	  {
        cluster = data(0).toLong
      } else {
        cluster = -1
      }
      count = count + 1
      (data(0).toLong, cluster, data.drop(1).toList.map(_.toLong))
    })

    for(i<- 1 to depth){
      graph = graph.flatMap { case (id, cluster, adj) => (id, cluster) :: adj.map{ x => (x, cluster) } }
        .reduceByKey(_ max _)
        .join(graph.map(a=>(a._1,(a))))
        .map({case (id,(old,adjacent))=> {
		
          var cluster:Long= -1
          if(adjacent._2 < 0){
            cluster = old

          }
          else
            cluster = adjacent._2
          (id,cluster,adjacent._3)
        }
        })
    }
    var finalgraph = graph.map(b=>(b._2,1)).countByKey()
    finalgraph.foreach(c=>println(c._1,c._2))

  }
}