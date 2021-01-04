//Vishnu Gopal Rajan
//1001755911

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object KMeans {

  type Point=(Double,Double)
  var centroids: Array[Point] = Array[Point]()

  def centroid (p: Point, x: Point): Double ={

    var centroid = Math.sqrt ((p._1 - x._1) * (p._1 - x._1) + (p._2 - x._2) * (p._2 - x._2) );
    return centroid
  }

  def main (args: Array[String]): Unit ={

    val conf = new SparkConf().setAppName("KMeans")
    val sc = new SparkContext(conf)

	var count : Int = 0;
	var Xval : Double =0.0;
	var Yval : Double =0.0;
		
    centroids = sc.textFile(args(1)).map( line => { val a = line.split(",")
      (a(0).toDouble,a(1).toDouble)}).collect
    var points=sc.textFile(args(0)).map(line=>{val b=line.split(",")
      (b(0).toDouble,b(1).toDouble)})

    for(i<- 1 to 5){
      val cs = sc.broadcast(centroids)
      centroids = points.map { p => (cs.value.minBy(centroid(p,_)), p) }
        .groupByKey().map { case(c,new_centroid)=>
		
        count=0
        Xval=0.0
        Yval=0.0

        for(temp <- new_centroid) {
           count = count+1
           Xval+=temp._1
           Yval+=temp._2
        }
        (Xval/count,Yval/count)

      }.collect
    }

	centroids.foreach(println)
    }

}