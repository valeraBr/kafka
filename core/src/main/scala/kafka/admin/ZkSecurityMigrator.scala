/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.admin

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import javax.security.auth.login.LoginException
import joptsimple.OptionParser
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkException
import kafka.utils.{ToolsUtils, Logging, ZKGroupTopicDirs, ZkUtils, CommandLineUtils, ZkPath}
import org.apache.log4j.Level
import org.apache.kafka.common.security.JaasUtils
import org.apache.zookeeper.AsyncCallback.{ChildrenCallback, StatCallback}
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.ZooKeeper
import scala.collection.JavaConverters._
import scala.collection._
import scala.concurrent._
import scala.concurrent.duration._
import scala.util._

/**
 * This tool is to be used when making access to ZooKeeper authenticated or 
 * the other way around, when removing authenticated access. The exact steps
 * to migrate a Kafka cluster from unsecure to secure with respect to ZooKeeper
 * access are the following:
 * 
 * 1- Perform a rolling upgrade of Kafka servers, setting zookeeper.set.acl to false
 * and passing a valid JAAS login file via the system property 
 * java.security.auth.login.config
 * 2- Perform a second rolling upgrade keeping the system property for the login file
 * and now setting zookeeper.set.acl to true
 * 3- Finally run this tool. There is a script under ./bin. Run 
 *   ./bin/zookeeper-security-migration --help
 * to see the configuration parameters. An example of running it is the following:
 *  ./bin/zookeeper-security-migration --kafka.go=secure --zookeeper.connection=localhost:2181
 * 
 * To convert a cluster from secure to unsecure, we need to perform the following
 * steps:
 * 1- Perform a rolling upgrade setting zookeeper.set.acl to false for each server
 * 2- Run this migration tool, setting kafka.go to unsecure
 * 3- Perform another rolling upgrade to remove the system property setting the
 * login file (java.security.auth.login.config).
 */

object ZkSecurityMigrator extends Logging {
  val usageMessage = ("ZooKeeper Migration Tool Help. This tool updates the ACLs of "
                      + "znodes as part of the process of setting up ZooKeeper "
                      + "authentication.")
  val workQueue = new LinkedBlockingQueue[Runnable]
  val threadExecutionContext = 
    ExecutionContext.fromExecutor(new ThreadPoolExecutor(1,
                                                         Runtime.getRuntime().availableProcessors(),
                                                         5000,
                                                         TimeUnit.MILLISECONDS,
                                                         workQueue))
  var futures: List[Future[String]] = List()
  var exception: ZkException = null
  var zkUtils: ZkUtils = null
  
  def setAclsRecursively(path: String) {
    info("Setting ACL for path %s".format(path))
    val setPromise = Promise[String]
    val childrenPromise = Promise[String]
    futures = futures :+ setPromise.future
    futures = futures :+ childrenPromise.future
    zkUtils.zkConnection.getZookeeper.setACL(path, ZkUtils.DefaultAcls(zkUtils.isSecure), -1, SetACLCallback, setPromise)
    zkUtils.zkConnection.getZookeeper.getChildren(path, false, GetChildrenCallback, childrenPromise)
  }

  object GetChildrenCallback extends ChildrenCallback {
      def processResult(rc: Int,
                        path: String,
                        ctx: Object,
                        children: java.util.List[String]) {
        //val list = Option(children).map(_.asScala)
        val zkHandle = zkUtils.zkConnection.getZookeeper
        val promise = ctx.asInstanceOf[Promise[String]]
        Code.get(rc) match {
          case Code.OK =>
            // Set ACL for each child 
              Future {
                val childPathBuilder = new StringBuilder
                for(child <- children.asScala) {
                  childPathBuilder.clear
                  childPathBuilder.append(path)
                  childPathBuilder.append("/")
                  childPathBuilder.append(child.mkString)

                  val childPath = childPathBuilder.mkString
                  setAclsRecursively(childPath)
                }
                promise success "done"
              }(threadExecutionContext)
          case Code.CONNECTIONLOSS =>
            Future {
              zkHandle.getChildren(path, false, GetChildrenCallback, ctx)
            }(threadExecutionContext)
          case Code.NONODE =>
            warn("Node is gone, it could be have been legitimately deleted: %s".format(path))
            promise success "done"
          case Code.SESSIONEXPIRED =>
            // Starting a new session isn't really a problem, but it'd complicate
            // the logic of the tool, so we quit and let the user re-run it.
            error("ZooKeeper session expired while changing ACLs")
            promise failure ZkException.create(KeeperException.create(Code.get(rc)))
          case _ =>
            error("Unexpected return code: %d".format(rc))
            promise failure ZkException.create(KeeperException.create(Code.get(rc)))
        }
      }
  }
  
  object SetACLCallback extends StatCallback {
    def processResult(rc: Int,
                      path: String,
                      ctx: Object,
                      stat: Stat) {
      val zkHandle = zkUtils.zkConnection.getZookeeper
      val promise = ctx.asInstanceOf[Promise[String]]
      
      Code.get(rc) match {
          case Code.OK =>
            info("Successfully set ACLs for %s".format(path))
            promise success "done"
          case Code.CONNECTIONLOSS =>
            Future {
              zkHandle.setACL(path, ZkUtils.DefaultAcls(zkUtils.isSecure), -1, SetACLCallback, ctx)
            }(threadExecutionContext)
          case Code.NONODE =>
            warn("Node is gone, it could be have been legitimately deleted: %s".format(path))
            promise success "done"
          case Code.SESSIONEXPIRED =>
            // Starting a new session isn't really a problem, but it'd complicate
            // the logic of the tool, so we quit and let the user re-run it.
            error("ZooKeeper session expired while changing ACLs")
            promise failure ZkException.create(KeeperException.create(Code.get(rc)))
          case _ =>
            error("Unexpected return code: %d".format(rc))
            promise failure ZkException.create(KeeperException.create(Code.get(rc)))   
      }
    }
  }

  def run(args: Array[String]) {
    var jaasFile = System.getProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    val parser = new OptionParser()

    val goOpt = parser.accepts("zookeeper.acl", "Indicates whether to make the Kafka znodes in ZooKeeper secure or unsecure."
        + " The options are 'secure' and 'unsecure'").withRequiredArg().ofType(classOf[String])
    val jaasFileOpt = parser.accepts("jaas.file", "JAAS Config file.").withOptionalArg().ofType(classOf[String])
    val zkUrlOpt = parser.accepts("zookeeper.connect", "Sets the ZooKeeper connect string (ensemble). This parameter " +
      "takes a comma-separated list of host:port pairs.").withRequiredArg().defaultsTo("localhost:2181").
      ofType(classOf[String])
    val zkSessionTimeoutOpt = parser.accepts("zookeeper.session.timeout", "Sets the ZooKeeper session timeout.").
      withRequiredArg().defaultsTo("30000").ofType(classOf[java.lang.Integer])
    val zkConnectionTimeoutOpt = parser.accepts("zookeeper.connection.timeout", "Sets the ZooKeeper connection timeout.").
      withRequiredArg().defaultsTo("30000").ofType(classOf[java.lang.Integer])
    val helpOpt = parser.accepts("help", "Print usage information.")

    val options = parser.parse(args : _*)
    if (options.has(helpOpt))
      CommandLineUtils.printUsageAndDie(parser, usageMessage)

    if ((jaasFile == null) && !options.has(jaasFileOpt)) {
     val errorMsg = ("No JAAS configuration file has been specified. Please make sure that you have set either " + 
                    "the system property %s or the option %s".format(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, "--jaas.file")) 
     if(logger.isEnabledFor(Level.ERROR))
       error(errorMsg)
     System.err.println("ERROR: %s".format(errorMsg))
     throw new IllegalArgumentException("Incorrect configuration")
    }

    if (jaasFile == null) {
      jaasFile = options.valueOf(jaasFileOpt)
      System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasFile)
    }

    if (!JaasUtils.isZkSecurityEnabled(jaasFile)) {
      val errorMsg = "Security isn't enabled, most likely the file isn't set properly: %s".format(jaasFile)
      if(logger.isEnabledFor(Level.ERROR))
        error(errorMsg)
      System.err.println("ERROR: %s".format(errorMsg))
      throw new IllegalArgumentException("Incorrect configuration") 
    }

    val goSecure: Boolean = options.valueOf(goOpt) match {
      case "secure" =>
        info("Making it secure")
        true
      case "unsecure" =>
        info("Making it unsecure")
        false
      case _ =>
        CommandLineUtils.printUsageAndDie(parser, usageMessage)
    }
    val zkUrl = options.valueOf(zkUrlOpt)
    val zkSessionTimeout = 6000 //options.valueOf(zkSessionTimeoutOpt).intValue
    val zkConnectionTimeout = 6000 //options.valueOf(zkConnectionTimeoutOpt).intValue
    zkUtils = ZkUtils(zkUrl, zkSessionTimeout, zkConnectionTimeout, goSecure)
    
    for (path <- zkUtils.securePersistentZkPaths) {
      info("Securing " + path)
      zkUtils.makeSurePersistentPathExists(path)
      setAclsRecursively(path)
    }

    try {
      while(futures.size > 0) {
        val head::tail = futures
        Await.result(head, Duration.Inf)
        head.value match {
          case Some(Success(v)) => // nothing to do
          case Some(Failure(e)) => throw e
        }
        futures = tail
        info("Size of future list is %d".format(futures.size))
      }
    } finally {
      zkUtils.close
    }
  }

  def main(args: Array[String]) {
    try {
      run(args)
    } catch {
        case e: Exception =>
          e.printStackTrace()
    }
  }
}