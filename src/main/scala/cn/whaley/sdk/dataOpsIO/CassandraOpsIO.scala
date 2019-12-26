package cn.whaley.sdk.dataopsio

import com.datastax.driver.core.Cluster


/**
 * Created by Administrator on 2016/5/19.
 */
trait CassandraOpsIO {

  def getCassandraCluster():Cluster
}
