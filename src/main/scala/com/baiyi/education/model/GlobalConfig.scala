package com.baiyi.education.model

object GlobalConfig {
  val HBASE_ZOOKEEPER_QUORUM = "192.168.155.179"
  val HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181"

  val BOOTSTRAP_SERVERS = "192.168.155.179:6667"
  val ACKS = "-1"

  val KUDU_MASTER = "hadoop201"
  val KUDU_TABLE_DWDBASEAD = "impala::education.dwd_base_ad"
  val KUDU_TABLE_DWDBASEWEBSITE = "impala::education.dwd_base_website"
  val KUDU_TABLE_DWDVIPLEVEL = "impala::education.dwd_vip_level"
  val KUDU_TABLE_DWSMEMBER = "impala::education.dws_member"
}
