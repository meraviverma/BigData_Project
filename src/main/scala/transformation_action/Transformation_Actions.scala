package transformation_action

import SparkDefGuide.Chapter5.Chapter5.sc
import conf.SparkConfiguration
import org.apache.spark.Partitioner

//All Transformations and Actions in Spark
object Transformation_Actions extends SparkConfiguration {
  def main(args: Array[String]):Unit= {

    import sc.implicits._
    //narrow
      /*each partition of the parent RDD is used by at most one partition of the child RDD
      */
    //wide
    /*multiple child RDD partitions may depend on a single parent RDD partition*/

    //MAP
    println("########## MAP #################")
    val x_map=sc.sparkContext.parallelize(List("a","b","c"))
    val y_map=x_map.map(z=>(z,1))
    println(x_map.collect().mkString(","))
    println(y_map.collect().mkString(","))

    //FILTER
    println("############# FILTER ##############")
    val x_fltr=sc.sparkContext.parallelize(List(1,2,3))
    val y_fltr=x_fltr.filter(n=>n%2 ==1)
    println(x_fltr.collect().mkString(","))
    println(y_fltr.collect().mkString(","))

    //FLATMAP
    println("############### FLATMAP ###############")
    val x_flatmap=sc.sparkContext.parallelize(List(1,2,3))
    val y_flatmap=x_flatmap.flatMap(n=>List(n,n*100,42))
    println(x_flatmap.collect().mkString(","))
    println(y_flatmap.collect().mkString(","))

    //GROUPBY
    println("############ GROUPBY ###################")
    val x_grpby=sc.sparkContext.parallelize(List("ram","freed","james","steve"))
    val y_grpby=x_grpby.groupBy(w=>w.charAt(0))
    println(y_grpby.collect().mkString(","))

    //GROUPBYKEY
    println("############# GROUPBYKEY #################")
    val x_grpbykey=sc.sparkContext.parallelize(List(('B',5),('B',4),('A',3),('A',2),('A',1)))
    val y_grpbykey=x_grpbykey.groupByKey()
    println(y_grpbykey.collect().mkString(","))

    //REDUCEBYKEY VS GROUPBYKEY
    println("############### REDUCEBYKEY VS GROUPBYKEY ###############")
    val words = Array("one", "two", "two", "three", "three", "three")
    val wordPairsRDD = sc.sparkContext.parallelize(words).map(word => (word, 1))
    val wordCountsWithReduce = wordPairsRDD
      .reduceByKey(_ + _)

    val wordCountsWithGroup = wordPairsRDD
      .groupByKey()
      .map(t => (t._1, t._2.sum))

    println(wordCountsWithReduce.collect().mkString(","))
    println(wordCountsWithGroup.collect().mkString(","))

    println("############ MAPPARTITIONS ################")
    //MAPPARTITIONS ::Return a new RDD by applying a function to each partition of this RDD
    val x_mappartition = sc.sparkContext.parallelize(Array(1,2,3), 2)
    def f(i:Iterator[Int])={ (i.sum,42).productIterator }
    val y_mappartition = x_mappartition.mapPartitions(f)
    // glom() flattens elements on the same partition
    println(x_mappartition.collect().mkString(","))
    println(y_mappartition.collect().mkString(","))

    //MAPPARTITIONSWITHINDEX :: mapPartitionsWithIndex(f, preservesPartitioning=False)
    //Return a new RDD by applying a function to each partition of this RDD,
    //while tracking the index of the original partition.

    println("################## MAPPARTITIONSWITHINDEX #######################")
    val x_mpwi = sc.sparkContext.parallelize(Array(1,2,3), 2)
    def f_mpwi(partitionIndex:Int, i:Iterator[Int]) = {
      (partitionIndex, i.sum).productIterator
    }
    val y_mpwi = x_mpwi.mapPartitionsWithIndex(f_mpwi)
    // glom() flattens elements on the same partition
    val xOut = x_mpwi.glom().collect()
    val yOut = y_mpwi.glom().collect()
    println(xOut.mkString(","))
    println(yOut.mkString(","))

    //SAMPLE :: Return a new RDD containing a statistical sample of the original RDD
    println("########## SAMPLE ################")
    val x_sample = sc.sparkContext.parallelize(Array(1, 2, 3, 4, 5))
    val y_sample = x_sample.sample(false, 0.4)
    // omitting seed will yield different output
    println(y_sample.collect().mkString(", "))

    //UNION: Return a new RDD containing all items from two original RDDs. Duplicates are not culled.
    //union(otherRDD)
    println("################# UNION ############")
    val x_union = sc.sparkContext.parallelize(Array(1,2,3), 2)
    val y_union = sc.sparkContext.parallelize(Array(3,4), 1)
    val z_union = x_union.union(y_union)
    val zOut = z_union.glom().collect()
    println(z_union.collect().mkString(","))

    //JOIN: Return a new RDD containing all pairs of elements having the same key in the original RDDs
    //union(otherRDD, numPartitions=None)
    println("################## JOIN ###############")
    val x_join = sc.sparkContext.parallelize(Array(("a", 1), ("b", 2)))
    val y_join = sc.sparkContext.parallelize(Array(("a", 3), ("a", 4), ("b", 5)))
    val z_join = x_join.join(y_join)
    println(z_join.collect().mkString(", "))

    //DISTINCT:Return a new RDD containing distinct items from the original RDD (omitting all duplicates)
    //distinct(numPartitions=None)
    println("################### DISTINCT #################")
    val x_distinct = sc.sparkContext.parallelize(Array(1,2,3,3,4))
    val y_distinct = x_distinct.distinct()
    println(y_distinct.collect().mkString(", "))

    //COALESCE : Return a new RDD which is reduced to a smaller number of partitions
    //coalesce(numPartitions, shuffle=False)

    println("################# COALSECE ########################")
    val x_coalesce = sc.sparkContext.parallelize(Array(1, 2, 3, 4, 5), 3)
    val y_coalesce = x_coalesce.coalesce(2)
    val xOut_coalesce = x_coalesce.glom().collect()
    val yOut_coalesce = y_coalesce.glom().collect()
    println(xOut_coalesce.mkString(", "))
    println(yOut_coalesce.mkString(" ,"))

    println("######################## KEYBY ###################")
    //KEYBY:Create a Pair RDD, forming one pair for each item in the original RDD. The
    //pair’s key is calculated from the value via a user-supplied function.
    val x_keyby = sc.sparkContext.parallelize(
      Array("John", "Fred", "Anna", "James"))
    val y_keyby = x_keyby.keyBy(w => w.charAt(0))
    println(y_keyby.collect().mkString(", "))

    println(" ###################### PARTITION BY #########################")
    //PARTITIONBY: Return a new RDD with the specified number of partitions, placing original
    //items into the partition returned by a user supplied function
    //partitionBy(numPartitions, partitioner=portable_hash)
    val x_partitionBy = sc.sparkContext.parallelize(Array(('J',"James"),('F',"Fred"),
      ('A',"Anna"),('J',"John")), 3)
    val y_partitionBy = x_partitionBy.partitionBy(new Partitioner() {
      val numPartitions = 2
      def getPartition(k:Any) = {
        if (k.asInstanceOf[Char] < 'H') 0 else 1
      }
    })
    val yOut_partitionBy = y_partitionBy.glom().collect()
    println(yOut_partitionBy.mkString(","))

    /*x : Array(Array((A,Anna), (F,Fred)),
      Array((J,John), (J,James)))
    y : Array(Array((F,Fred), (A,Anna)),
      Array((J,John), (J,James)))*/

    println("################# ZIP ##################")
    //ZIP : Return a new RDD containing pairs whose key is the item in the original RDD, and whose
    //value is that item’s corresponding element (same partition, same index) in a second RDD
    //zip(otherRDD)
    val x_zip = sc.sparkContext.parallelize(Array(1,2,3))
    val y_zip = x_zip.map(n=>n*n)
    val z_zip = x_zip.zip(y_zip)
    println(z_zip.collect().mkString(", "))

    //-------------ACTIONS-----------
    //GETNUMPARTITIONS : Return the number of partitions in RDD
    println(" ############### GETNUMPARTITIONS #################")
    val x_getnumpar = sc.sparkContext.parallelize(Array(1,2,3), 2)
    val y_getnumpar = x_getnumpar.partitions.size
    val xOut_getnumpar = x_getnumpar.glom().collect()
    println(xOut_getnumpar)

    println(" ############# REDUCE ##################")
    val x_reduce = sc.sparkContext.parallelize(Array(1,2,3,4))
    val y_reduce = x_reduce.reduce((a,b) => a+b)
    println(x_reduce.collect.mkString(", "))
    println(y_reduce)


//OUTPUT
    /*######### MAP #################
    a,b,c
    (a,1),(b,1),(c,1)
    ############# FILTER ##############
    1,2,3
    1,3
    ############### FLATMAP ###############
    1,2,3
    1,100,42,2,200,42,3,300,42
    ############ GROUPBY ###################
    (s,CompactBuffer(steve)),(f,CompactBuffer(freed)),(j,CompactBuffer(james)),(r,CompactBuffer(ram))
    ############# GROUPBYKEY #################
    (B,CompactBuffer(5, 4)),(A,CompactBuffer(3, 2, 1))
    ############### REDUCEBYKEY VS GROUPBYKEY ###############
    (two,2),(one,1),(three,3)
    (two,2),(one,1),(three,3)
    ############ MAPPARTITIONS ################
    1,2,3
    1,42,5,42
    ################## MAPPARTITIONSWITHINDEX #######################
      [I@4bbb49b0,[I@f096f37
      [Ljava.lang.Object;@3effd4f3,[Ljava.lang.Object;@41f4fe5
      ########## SAMPLE ################
    1
    ################# UNION ############
    1,2,3,3,4
    ################## JOIN ###############
    (a,(1,3)), (a,(1,4)), (b,(2,5))
    ################### DISTINCT #################
    4, 1, 3, 2
    ################# COALSECE ########################
      [I@7a7cc52c, [I@5853495b, [I@524a2ffb
      [I@2f61d591 ,[I@332820f4
    ######################## KEYBY ###################
    (J,John), (F,Fred), (A,Anna), (J,James)
    ###################### PARTITION BY #########################
    [Lscala.Tuple2;@6c15e8c7,[Lscala.Tuple2;@56380231
    ################# ZIP ##################
    (1,1), (2,4), (3,9)
    ############### GETNUMPARTITIONS #################
    ParallelCollectionRDD[49] at parallelize at Transformation_Actions.scala:172
    ############# REDUCE ##################
    1, 2, 3, 4
    10*/














  }

}
