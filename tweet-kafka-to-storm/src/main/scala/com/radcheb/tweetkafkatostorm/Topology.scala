package com.radcheb.tweetkafkatostorm

import java.util.Properties
import com.radcheb.tweetkafkatostorm.utils._;

import backtype.storm.generated.KillOptions
import backtype.storm.topology.TopologyBuilder
import backtype.storm.{Config, LocalCluster}
import kafka.admin.AdminUtils
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import storm.kafka.{KafkaSpout, SpoutConfig, ZkHosts}

// import the slf4j logger
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._

class Topology(kafkaZkConnect: String, topic: String, numTopicPartitions: Int = 1,
                     topologyName: String = "kafka-storm-starter", runtime: Duration = 1.hour) {

  def runTopologyLocally() {
    val zkHosts = new ZkHosts(kafkaZkConnect)
    val topic = "UnusualTopic"
    val zkRoot = "/kafka-spout"
    // The spout appends this id to zkRoot when composing its ZooKeeper path.  You don't need a leading `/`.
    val zkSpoutId = "kafka-storm-starter"
    // la config du kafka necessaire pour le spout de storm
    val kafkaConfig = new SpoutConfig(zkHosts, topic, zkRoot, zkSpoutId)
    // creation d'un spout kafka
    val kafkaSpout = new KafkaSpout(kafkaConfig)
    // un spout par topic
    val numSpoutExecutors = numTopicPartitions
    val builder = new TopologyBuilder
    val spoutId = "kafka-spout"
    builder.setSpout(spoutId, kafkaSpout, numSpoutExecutors)

    // Showcases how to customize the topology configuration
    val topologyConfiguration = {
      val c = new Config
      c.setDebug(false)
      c.setNumWorkers(4)
      c.setMaxSpoutPending(1000)
      c.setMessageTimeoutSecs(60)
      c.setNumAckers(0)
      c.setMaxTaskParallelism(50)
      c.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384: Integer)
      c.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384: Integer)
      c.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8: Integer)
      c.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32: Integer)
      c.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 0.05: java.lang.Double)
      c
    }

    // Now run the topology in a local, in-memory Storm cluster
    val cluster = new LocalCluster
    cluster.submitTopology(topologyName, topologyConfiguration, builder.createTopology())
    Thread.sleep(runtime.toMillis)
    val killOpts = new KillOptions()
    killOpts.set_wait_secs(1)
    cluster.killTopologyWithOpts(topologyName, killOpts)
    cluster.shutdown()
  }

}

object KafkaStormDemo {

  private var zookeeperEmbedded: Option[ZooKeeperEmbedded] = None
  private var zkClient: Option[ZkClient] = None
  private var kafkaEmbedded: Option[KafkaEmbedded] = None

  val log: Logger = LoggerFactory.getLogger(classOf[Topology])

  def main(args: Array[String]) {

    log.info("logger is running")
    val kafkaTopic = "UnusualTopic"
    log.info("startZooKeeperAndKafka, topic UnusualTopic")
    startZooKeeperAndKafka(kafkaTopic)
    // pour chaque instance de zookeeper embedded
    for {z <- zookeeperEmbedded} {
      // instantiation d'une topology avec tous ces composants
      val topology = new Topology(z.connectString, kafkaTopic)
      topology.runTopologyLocally()
    }
    stopZooKeeperAndKafka()
  }

  /**
   * Launches in-memory, embedded instances of ZooKeeper and Kafka so that our demo Storm topology can connect to and
   * read from Kafka.
   */
  private def startZooKeeperAndKafka(topic: String, numTopicPartitions: Int = 1, numTopicReplicationFactor: Int = 1,
                                     zookeeperPort: Int = 2182) {

    zookeeperEmbedded = Some(new ZooKeeperEmbedded(zookeeperPort))
    for {z <- zookeeperEmbedded} {
    log.info("connecting to zookeeper"+ z.connectString)
      // la conf du broker : la connection avec zookeeper
      val brokerConfig = new Properties
      brokerConfig.put("zookeeper.connect", z.connectString)
      // creation d'un kafka server en mémoire
      kafkaEmbedded = Some(new KafkaEmbedded(brokerConfig))
      // démarer tous les brokers
      for {k <- kafkaEmbedded} {
        k.start()
      }

      val sessionTimeout = 30.seconds
      val connectionTimeout = 30.seconds
      // un client de zookeeper pour faire l'dministration
      zkClient = Some(new ZkClient(z.connectString, sessionTimeout.toMillis.toInt, connectionTimeout.toMillis.toInt,
        ZKStringSerializer))
      for {
        zc <- zkClient
      } {
        // creation d'un topic
        val topicConfig = new Properties
        AdminUtils.createTopic(zc, topic, numTopicPartitions, numTopicReplicationFactor, topicConfig)
      }
    }
  }

  private def stopZooKeeperAndKafka() {
    for {k <- kafkaEmbedded} k.stop()
    for {zc <- zkClient} zc.close()
    for {z <- zookeeperEmbedded} z.stop()
  }

}