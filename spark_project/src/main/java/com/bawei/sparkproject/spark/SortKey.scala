package com.bawei.sparkproject.spark

/** *
 *
 * @author Zhi-jiang li
 * @date 2020/2/7 0007 13:31
 **/
class SortKey(val clickCount: Int,
              val orderCount: Int,
              val payCount: Int)
  extends Ordered[SortKey] with Serializable {
  override def compare(that: SortKey): Int = {
    if (clickCount - that.clickCount != 0) {
      clickCount - that.clickCount
    } else if (orderCount - that.orderCount != 0) {
      orderCount - that.orderCount
    } else if (payCount - that.payCount != 0) {
      payCount - that.payCount
    } else {
      0
    }
  }
}
