package com.radcheb.tweetkafkatostorm.utils;

import org.apache.curator.test.TestingServer

/**
 * Runs an in-memory, "embedded" instance of a ZooKeeper server.
 *
 * The ZooKeeper server instance is automatically started when you create a new instance of this class.
 *
 * @param port The port (aka `clientPort`) to listen to.  Default: 2181.
 */
class ZooKeeperEmbedded(val port: Int = 2182) extends LazyLogging {

  logger.debug(s"Starting embedded ZooKeeper server on port $port...")

  private val server = new TestingServer(port)

  /**
   * Stop the instance.
   */
  def stop() {
    logger.debug(s"Shutting down embedded ZooKeeper server on port $port...")
    server.close()
    logger.debug(s"Shutdown of embedded ZooKeeper server on port $port completed")
  }

  /**
   * The ZooKeeper connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
   * Example: `127.0.0.1:2181`.
   *
   * You can use this to e.g. tell Kafka and Storm how to connect to this instance.
   */
  val connectString: String = server.getConnectString

  /**
   * The hostname of the ZooKeeper instance.  Example: `127.0.0.1`
   */
  val hostname: String = connectString.splitAt(connectString lastIndexOf ':')._1 // "foo:1:2:3" -> ("foo:1:2", ":3)

}