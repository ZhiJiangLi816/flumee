package com.bawei.sparkproject.test

import com.bawei.sparkproject.spark.SortKey
import org.apache.spark.{SparkConf, SparkContext}

/** *
 *
 * @author Zhi-jiang li
 * @date 2020/2/7 0007 13:34
 **/
object SortKeyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SortKeyTest")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val arr = Array(Tuple2(new SortKey(30, 45, 20), "1"),
      Tuple2(new SortKey(35, 50, 4), "2"),
      Tuple2(new SortKey(30, 44, 21), "3"))

    val rdd = sc.parallelize(arr,1);

    val sortRdd = rdd.sortByKey(false)

    for (tuple <- sortRdd.collect()){
      println(tuple._2)
    }

  }

}
