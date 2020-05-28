package kafka.server

import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}

import kafka.utils.{Logging, ShutdownableThread}
import kafka.zk.{FeatureZNode,FeatureZNodeStatus, KafkaZkClient, ZkVersion}
import kafka.zookeeper.ZNodeChangeHandler
import org.apache.kafka.common.internals.FatalExitError

import scala.concurrent.TimeoutException

/**
 * Listens to changes in the ZK feature node, via the ZK client. Whenever a change notification
 * is received from ZK, the feature cache in FinalizedFeatureCache is asynchronously updated
 * to the latest features read from ZK. The cache updates are serialized through a single
 * notification processor thread.
 *
 * @param zkClient     the Zookeeper client
 */
class FinalizedFeatureChangeListener(zkClient: KafkaZkClient) extends Logging {

  /**
   * Helper class used to update the FinalizedFeatureCache.
   *
   * @param featureZkNodePath   the path to the ZK feature node to be read
   * @param maybeNotifyOnce     an optional latch that can be used to notify the caller when an
   *                            updateOrThrow() operation is over
   */
  private class FeatureCacheUpdater(featureZkNodePath: String, maybeNotifyOnce: Option[CountDownLatch]) {

    def this(featureZkNodePath: String) = this(featureZkNodePath, Option.empty)

    /**
     * Updates the feature cache in FinalizedFeatureCache with the latest features read from the
     * ZK node in featureZkNodePath. If the cache update is not successful, then, a suitable
     * exception is raised.
     *
     * NOTE: if a notifier was provided in the constructor, then, this method can be invoked exactly
     * once successfully. A subsequent invocation will raise an exception.
     *
     * @throws   IllegalStateException, if a non-empty notifier was provided in the constructor, and
     *           this method is called again after a successful previous invocation.
     * @throws   FeatureCacheUpdateException, if there was an error in updating the
     *           FinalizedFeatureCache.
     * @throws   RuntimeException, if there was a failure in reading/deserializing the
     *           contents of the feature ZK node.
     */
    def updateLatestOrThrow(): Unit = {
      maybeNotifyOnce.foreach(notifier => {
        if (notifier.getCount != 1) {
          throw new IllegalStateException(
            "Can not notify after updateLatestOrThrow was called more than once successfully.")
        }
      })

      debug(s"Reading feature ZK node at path: $featureZkNodePath")
      val (mayBeFeatureZNodeBytes, version) = zkClient.getDataAndVersion(featureZkNodePath)

      // There are 4 cases:
      //
      // (empty dataBytes, valid version)       => The empty dataBytes will fail FeatureZNode deserialization.
      //                                           FeatureZNode, when present in ZK, can not have empty contents.
      // (non-empty dataBytes, valid version)   => This is a valid case, and should pass FeatureZNode deserialization
      //                                           if dataBytes contains valid data.
      // (empty dataBytes, unknown version)     => This is a valid case, and this can happen if the FeatureZNode
      //                                           does not exist in ZK.
      // (non-empty dataBytes, unknown version) => This case is impossible, since, KafkaZkClient.getDataAndVersion
      //                                           API ensures that unknown version is returned only when the
      //                                           ZK node is absent. Therefore dataBytes should be empty in such
      //                                           a case.
      if (version == ZkVersion.UnknownVersion) {
        info(s"Feature ZK node at path: $featureZkNodePath does not exist")
        FinalizedFeatureCache.clear()
      } else {
        val featureZNode = FeatureZNode.decode(mayBeFeatureZNodeBytes.get)
        if (featureZNode.status == FeatureZNodeStatus.Disabled) {
          info(s"Feature ZK node at path: $featureZkNodePath is in disabled status, clearing feature cache.")
          FinalizedFeatureCache.clear()
        } else if (featureZNode.status == FeatureZNodeStatus.Enabled) {
          FinalizedFeatureCache.updateOrThrow(featureZNode.features, version)
        } else {
          throw new IllegalStateException(s"Unexpected FeatureZNodeStatus found in $featureZNode")
        }
      }

      maybeNotifyOnce.foreach(notifier => notifier.countDown())
    }

    /**
     * Waits until at least a single updateLatestOrThrow completes successfully. This method returns
     * immediately if an updateLatestOrThrow call had already completed successfully.
     *
     * @param waitTimeMs   the timeout for the wait operation
     *
     * @throws             RuntimeException if the thread was interrupted during wait
     *
     *                     TimeoutException if the wait can not be completed in waitTimeMs
     *                     milli seconds
     */
    def awaitUpdateOrThrow(waitTimeMs: Long): Unit = {
      maybeNotifyOnce.foreach(notifier => {
        var success = false
        try {
          success = notifier.await(waitTimeMs, TimeUnit.MILLISECONDS)
        } catch {
          case e: InterruptedException =>
            throw new RuntimeException(
              "Unable to wait for FinalizedFeatureCache update to finish.", e)
        }

        if (!success) {
          throw new TimeoutException(
            s"Timed out after waiting for ${waitTimeMs}ms for FeatureCache to be updated.")
        }
      })
    }
  }

  /**
   * A shutdownable thread to process feature node change notifications that are populated into the
   * queue. If any change notification can not be processed successfully (unless it is due to an
   * interrupt), the thread treats it as a fatal event and triggers Broker exit.
   *
   * @param name   name of the thread
   */
  private class ChangeNotificationProcessorThread(name: String) extends ShutdownableThread(name = name) {
    override def doWork(): Unit = {
      try {
        queue.take.updateLatestOrThrow()
      } catch {
        case e: InterruptedException => info(s"Change notification queue interrupted", e)
        case e: Exception => {
          error("Failed to process feature ZK node change event. The broker will exit.", e)
          throw new FatalExitError(1)
        }
      }
    }
  }

  // Feature ZK node change handler.
  object FeatureZNodeChangeHandler extends ZNodeChangeHandler {
    override val path: String = FeatureZNode.path

    override def handleCreation(): Unit = {
      info(s"Feature ZK node created at path: $path")
      queue.add(new FeatureCacheUpdater(path))
    }

    override def handleDataChange(): Unit = {
      info(s"Feature ZK node updated at path: $path")
      queue.add(new FeatureCacheUpdater(path))
    }

    override def handleDeletion(): Unit = {
      warn(s"Feature ZK node deleted at path: $path")
      // This event may happen, rarely (ex: ZK corruption or operational error).
      // In such a case, we prefer to just log a warning and treat the case as if the node is absent,
      // and populate the FinalizedFeatureCache with empty finalized features.
      queue.add(new FeatureCacheUpdater(path))
    }
  }

  private val queue = new LinkedBlockingQueue[FeatureCacheUpdater]

  private val thread = new ChangeNotificationProcessorThread("feature-zk-node-event-process-thread")

  /**
   * This method initializes the feature ZK node change listener. Optionally, it also ensures to
   * update the FinalizedFeatureCache once with the latest contents of the feature ZK node
   * (if the node exists). This step helps ensure that feature incompatibilities (if any) in brokers
   * are conveniently detected before the initOrThrow() method returns to the caller. If feature
   * incompatibilities are detected, this method will throw an Exception to the caller, and the Broker
   * will exit eventually.
   *
   * @param waitOnceForCacheUpdateMs   # of milli seconds to wait for feature cache to be updated once.
   *                                   If this parameter <= 0, no wait operation happens.
   *
   * @throws Exception if feature incompatibility check could not be finished in a timely manner
   */
  def initOrThrow(waitOnceForCacheUpdateMs: Long): Unit = {
    thread.start()
    zkClient.registerZNodeChangeHandlerAndCheckExistence(FeatureZNodeChangeHandler)

    if (waitOnceForCacheUpdateMs > 0) {
      val ensureCacheUpdateOnce = new FeatureCacheUpdater(
        FeatureZNodeChangeHandler.path, Some(new CountDownLatch(1)))
      queue.add(ensureCacheUpdateOnce)
      try {
        ensureCacheUpdateOnce.awaitUpdateOrThrow(waitOnceForCacheUpdateMs)
      } catch {
        case e: Exception => {
          close()
          throw e
        }
      }
    }
  }

  /**
   * Closes the feature ZK node change listener by unregistering the listener from ZK client,
   * clearing the queue and shutting down the ChangeNotificationProcessorThread.
   */
  def close(): Unit = {
    zkClient.unregisterZNodeChangeHandler(FeatureZNodeChangeHandler.path)
    queue.clear()
    thread.shutdown()
    thread.join()
  }

  // For testing only.
  def isListenerInitiated: Boolean = {
    thread.isRunning && thread.isAlive
  }

  // For testing only.
  def isListenerDead: Boolean = {
    !thread.isRunning && !thread.isAlive
  }
}
