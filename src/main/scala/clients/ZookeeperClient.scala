package clients

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper._
import org.apache.zookeeper.data.Stat
import org.slf4j.LoggerFactory

class ZookeeperClient(servers: String, basePath: String, sessionTimeout: Int = 3000, connectTimeout: Int = 3000) {
  private lazy val logger = LoggerFactory.getLogger(classOf[ZookeeperClient])
  private var zk: ZooKeeper = _
  if (Option(basePath).get.contains("/") || basePath == null || Option(basePath).isEmpty) {
    throw new IllegalArgumentException("basePath must not have a '/' or be empty or null")
  }
  connect()

  /**
    * @param path - the znode path to set data
    * @param data - the string data to set the path to
    */
  def setData(path: String, data: String): Unit = {
    val currentPath = makeZNodeString(path)
    val pathStat = Option(zk.exists(currentPath, false))
    if (pathStat.isEmpty) {
      // if path doesnt exist then create it. persistent = keep after disconnect
      zk.create(currentPath, data.getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    }
    else { zk.setData(currentPath, data.getBytes, -1) }
  }

  /**
    * @param path - path to the destination
    * @return - the data currently in zookeeper corresponding to the path
    */
  def getData(path: String): String = {
    // null is used for stat because the package only will ignore the stat field if it's a null
    val data: Array[Byte] = zk.getData(makeZNodeString(path), false, null)
    new String(data, "UTF-8")
  }

  /**
    * create zookeeper path
    * @param path - the path to be created
    */
  def createPath(path: String): Unit = {
    val currentPath = makeZNodeString(path)
    try {
      logger.debug("Creating Path in ZooKeeper: {}", currentPath)
      zk.create(currentPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    } catch {
      case _: KeeperException.NodeExistsException => logger.info(s"Path $currentPath already exists.")
    }
  }

  /**
    * @param path - the znode path to delete
    */
  def deletePath(path: String): Unit = {
    val currentPath = makeZNodeString(path)
    logger.info(s"Deleting Path $currentPath...")
    val statData = zk.exists(currentPath, false)
    zk.delete(currentPath, statData.getVersion)
    logger.info(s"Deleted path $currentPath.")
  }

  /**
    * connects to zookeeper and handles disconnects
    */
  private def connect(): Unit = {
    // used to block until we have received a successful connection or other work
    val latch = new CountDownLatch(1)
    // watcher gets notified of any state change
    val watcher = new Watcher {
      def process(event : WatchedEvent) { processEvent(latch, event) }
    }
    if (zk != null) { zk.close() }  // close any lingering sessions
    zk = new ZooKeeper(servers, sessionTimeout, watcher)
    latch.countDown() // allow time for assignments before releasing threads
    latch.await(sessionTimeout, TimeUnit.MILLISECONDS) // allow time for connection
    try {
      pokeZooKeeper
      logger.info("Successfully connected to Zookeeper.")
    } catch {
      case e: Exception => throw new RuntimeException("Unable to connect to ZooKeeper.", e)
    }
  }

  /**
    * test command to make sure we successfully connected
    * @return - whether or not test command was successful
    */
  private def pokeZooKeeper: Boolean = {
    val result: Stat = zk.exists("/", false)
    result.getVersion >= 0
  }

  /**
    * path creation and to prevent writing to root
    * @param path desired path under the basePath
    * @return - cleaned path attached to the base path
    */
  private def makeZNodeString(path: String): String = {
    if (path.endsWith("/")) {
      "/%s/%s".format(basePath, path.dropRight(1)).replaceAll("//", "/")
    } else {
      "/%s/%s".format(basePath, path).replaceAll("//", "/")
    }
  }

  /**
    * logic for the watcher. watchers are notified when any state change occurs in the client.
    * @param latch - a latch to give others time to catch up and get settled
    * @param event - event sent in zookeeper
    */
  private def processEvent(latch: CountDownLatch, event : WatchedEvent) {
    latch.await()
    event.getState match {
      case KeeperState.SyncConnected => latch.countDown()  // count down and release all waiting threads when done
      case KeeperState.Expired => connect()
      case _ => logger.info("Zookeeper has disconnected.")
    }
  }
}