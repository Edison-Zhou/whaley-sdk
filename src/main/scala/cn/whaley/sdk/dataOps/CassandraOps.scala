package cn.whaley.sdk.dataOps

import cn.whaley.sdk.customize.TotalConf
import cn.whaley.sdk.dataopsio.CassandraOpsIO
import com.datastax.driver.core.Cluster

/**
 * Created by Administrator on 2016/5/19.
 */
object CassandraOps extends CassandraOpsIO{


  private val props = TotalConf.setCassandraConf

  /**
   * get cassandra connection
   * @return
   */
  override def getCassandraCluster():Cluster={

    val host = "bigdata-appsvr-130-1,bigdata-appsvr-130-2," +
      "bigdata-appsvr-130-3,bigdata-appsvr-130-4,bigdata-appsvr-130-5"
    val cassandraHost = host.split(",")

    val clusterName = "Big Data Cluster"

    val cluster:Cluster = Cluster.builder().withClusterName(clusterName).
                          addContactPoints(cassandraHost:_*).build()

    cluster
  }

/*  def getVideoInfo(videSid: String,InfoName: String = "all")={
    val c =
  }*/

}
