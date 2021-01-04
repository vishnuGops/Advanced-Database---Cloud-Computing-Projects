import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io._


object Partition {
  def main ( args: Array[String] ) {
  	val conf = new SparkConf().setAppName("Partition")
  	val sc = new SparkContext(conf)
  	var count=0

    val edges: RDD[Edge[Long]] = sc.textFile(args(0)).map( line => { val (node, adj) = line.split(",").splitAt(1)
    (node(0).toLong,adj.toList.map(_.toLong)) } )
    .flatMap( x => x._2.map(y => (x._1, y)))
    .map(nodes => Edge(nodes._1, nodes._2,(-1).toLong))
  	val get_vertices = sc.textFile(args(0)).map(line => {
        val (node, _) = line.split(",").splitAt(1)
  		var v = (-1).toLong
  		if(count < 5){ count=count + 1;
        v=node(0).toLong
  		}
        v
  	})
  	val temp = get_vertices.filter(_ != -1).collect().toList
	val graph: Graph[Long, Long] = Graph.fromEdges(edges, "Default").mapVertices((id, _) => {
        var centroid = (-1).toLong
        if (temp.contains(id)) {centroid = id}
        centroid
      })

    val i = graph.pregel(Long.MinValue, 6)(
      (vid, oldV, newV) => {
        if (oldV == -1) {math.max(oldV, newV)} 
        else {oldV}},
      triplet => {Iterator((triplet.dstId, triplet.srcAttr))},
      (a, b) => math.max(a, b)
    )

    var res = i.vertices.map{case (id, centroid) =>(centroid, 1)}.reduceByKey(_ + _).sortByKey()
    res.collect.foreach(println)
  }
}