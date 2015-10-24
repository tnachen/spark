/*
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

package org.apache.spark.scheduler.cluster.mesos

import java.io.File
import java.util
import java.util.concurrent.locks.ReentrantLock
import java.util.{Collections, List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, HashSet}

import org.apache.mesos.Protos.{TaskInfo => MesosTaskInfo, _}
import org.apache.mesos.{Scheduler => MScheduler, SchedulerDriver}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.mesos.MesosExternalShuffleClient
import org.apache.spark.rpc.RpcAddress
import org.apache.spark.scheduler.{SlaveLost, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.Utils
import org.apache.spark.{SecurityManager, SparkContext, SparkEnv, SparkException, TaskState}

/**
 * A SchedulerBackend that runs tasks on Mesos, but uses "coarse-grained" tasks, where it holds
 * onto each Mesos node for the duration of the Spark job instead of relinquishing cores whenever
 * a task is done. It launches Spark tasks within the coarse-grained Mesos tasks using the
 * CoarseGrainedSchedulerBackend mechanism. This class is useful for lower and more predictable
 * latency.
 *
 * Unfortunately this has a bit of duplication from MesosSchedulerBackend, but it seems hard to
 * remove this.
 */
private[spark] class CoarseMesosSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    master: String,
    securityManager: SecurityManager)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv)
  with MScheduler
  with MesosSchedulerUtils {

  val MAX_SLAVE_FAILURES = 2     // Blacklist a slave after this many failures

  // Maximum number of cores to acquire (TODO: we'll need more flexible controls here)
  val maxCores = conf.get("spark.cores.max", Int.MaxValue.toString).toInt

  // If shuffle service is enabled, the Spark driver will register with the shuffle service.
  // This is for cleaning up shuffle files reliably.
  private val shuffleServiceEnabled = conf.getBoolean("spark.shuffle.service.enabled", false)

  private val maxExecutorsPerSlave = conf.getInt("spark.mesos.coarse.executors.max", 1)

  private val maxCpusPerExecutor =
    conf.getOption("spark.mesos.coarse.coresPerExecutor.max").map { m => m.toInt }

  if (conf.getOption("spark.mesos.coarse.executors.max").isDefined && maxCpusPerExecutor.isEmpty) {
    throw new IllegalArgumentException(
      "Must configure spark.mesos.coarse.coresPerExecutor.max when " +
      "spark.mesos.coarse.executors.max is set")
  }

  // Cores we have acquired with each Mesos task ID
  val coresByTaskId = new HashMap[String, Int]
  var totalCoresAcquired = 0

  // Maping from slave Id to hostname
  private val slaveIdToHost = new HashMap[String, String]

  // Contains the list of slave ids that we have connect shuffle service to
  private val existingSlaveShuffleConnections = new HashSet[String]

  // Contains a mapping of slave ids to the number of executors launched.
  val slaveIdsWithExecutors = new HashMap[String, Int]

  val taskIdToSlaveId: HashMap[String, String] = new HashMap[String, String]
  // How many times tasks on each slave failed
  val failuresBySlaveId: HashMap[String, Int] = new HashMap[String, Int]

  /**
   * The total number of executors we aim to have. Undefined when not using dynamic allocation
   * and before the ExecutorAllocatorManager calls [[doRequestTotalExecutors]].
   */
  private var executorLimitOption: Option[Int] = None

  /**
   *  Return the current executor limit, which may be [[Int.MaxValue]]
   *  before properly initialized.
   */
  private[mesos] def executorLimit: Int = executorLimitOption.getOrElse(Int.MaxValue)

  // private lock object protecting mutable state above. Using the intrinsic lock
  // may lead to deadlocks since the superclass might also try to lock
  private val stateLock = new ReentrantLock

  val extraCoresPerSlave = conf.getInt("spark.mesos.extra.cores", 0)

  // Offer constraints
  private val slaveOfferConstraints =
    parseConstraintString(sc.conf.get("spark.mesos.constraints", ""))

  // A client for talking to the external shuffle service, if it is a
  private val mesosExternalShuffleClient: Option[MesosExternalShuffleClient] = {
    if (shuffleServiceEnabled) {
      Some(new MesosExternalShuffleClient(
        SparkTransportConf.fromSparkConf(conf),
        securityManager,
        securityManager.isAuthenticationEnabled(),
        securityManager.isSaslEncryptionEnabled()))
    } else {
      None
    }
  }

  var nextMesosTaskId = 0

  @volatile var appId: String = _

  def newMesosTaskId(slaveId: String): String = {
    val id = nextMesosTaskId
    nextMesosTaskId += 1
    slaveId + "/" + id
  }

  override def start() {
    super.start()
    val driver = createSchedulerDriver(
      master,
      CoarseMesosSchedulerBackend.this,
      sc.sparkUser,
      sc.appName,
      sc.conf,
      sc.ui.map(_.appUIAddress))
    startScheduler(driver)
  }

  def createCommand(offer: Offer, numCores: Int, taskId: String): CommandInfo = {
    val executorSparkHome = conf.getOption("spark.mesos.executor.home")
      .orElse(sc.getSparkHome())
      .getOrElse {
        throw new SparkException("Executor Spark home `spark.mesos.executor.home` is not set!")
      }
    val environment = Environment.newBuilder()
    val extraClassPath = conf.getOption("spark.executor.extraClassPath")
    extraClassPath.foreach { cp =>
      environment.addVariables(
        Environment.Variable.newBuilder().setName("SPARK_CLASSPATH").setValue(cp).build())
    }
    val extraJavaOpts = conf.get("spark.executor.extraJavaOptions", "")

    // Set the environment variable through a command prefix
    // to append to the existing value of the variable
    val prefixEnv = conf.getOption("spark.executor.extraLibraryPath").map { p =>
      Utils.libraryPathEnvPrefix(Seq(p))
    }.getOrElse("")

    environment.addVariables(
      Environment.Variable.newBuilder()
        .setName("SPARK_EXECUTOR_OPTS")
        .setValue(extraJavaOpts)
        .build())

    sc.executorEnvs.foreach { case (key, value) =>
      environment.addVariables(Environment.Variable.newBuilder()
        .setName(key)
        .setValue(value)
        .build())
    }
    val command = CommandInfo.newBuilder()
      .setEnvironment(environment)

    val uri = conf.getOption("spark.executor.uri")
      .orElse(Option(System.getenv("SPARK_EXECUTOR_URI")))

    if (uri.isEmpty) {
      val runScript = new File(executorSparkHome, "./bin/spark-class").getCanonicalPath
      command.setValue(
        "%s \"%s\" org.apache.spark.executor.CoarseGrainedExecutorBackend"
          .format(prefixEnv, runScript) +
        s" --driver-url $driverURL" +
        s" --executor-id $taskId" +
        s" --hostname ${offer.getHostname}" +
        s" --cores $numCores" +
        s" --app-id $appId")
    } else {
      // Grab everything to the first '.'. We'll use that and '*' to
      // glob the directory "correctly".
      val basename = uri.get.split('/').last.split('.').head

      command.setValue(
        s"cd $basename*; $prefixEnv " +
         "./bin/spark-class org.apache.spark.executor.CoarseGrainedExecutorBackend" +
        s" --driver-url $driverURL" +
        s" --executor-id $taskId" +
        s" --hostname ${offer.getHostname}" +
        s" --cores $numCores" +
        s" --app-id $appId")
      command.addUris(CommandInfo.URI.newBuilder().setValue(uri.get))
    }

    conf.getOption("spark.mesos.uris").map { uris =>
      setupUris(uris, command)
    }

    command.build()
  }

  protected def driverURL: String = {
    if (conf.contains("spark.testing")) {
      "driverURL"
    } else {
      sc.env.rpcEnv.uriOf(
        SparkEnv.driverActorSystemName,
        RpcAddress(conf.get("spark.driver.host"), conf.get("spark.driver.port").toInt),
        CoarseGrainedSchedulerBackend.ENDPOINT_NAME)
    }
  }

  override def offerRescinded(d: SchedulerDriver, o: OfferID) {}

  override def registered(d: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo) {
    appId = frameworkId.getValue
    mesosExternalShuffleClient.foreach(_.init(appId))
    logInfo("Registered as framework ID " + appId)
    markRegistered()
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalCoresAcquired >= maxCores * minRegisteredRatio
  }

  override def disconnected(d: SchedulerDriver) {}

  override def reregistered(d: SchedulerDriver, masterInfo: MasterInfo) {}

  /**
   * Method called by Mesos to offer resources on slaves. We respond by launching an executor,
   * unless we've already launched more than we wanted to.
   */
  override def resourceOffers(d: SchedulerDriver, offers: JList[Offer]) {
    val memoryPerExecutor = calculateTotalMemory(sc)
    stateLock.synchronized {
      val filters = Filters.newBuilder().setRefuseSeconds(5).build()
      for (offer <- offers.asScala) {
        val mem = getResource(offer.getResourcesList, "mem")
        val cpus = getResource(offer.getResourcesList, "cpus").toInt
        var remainingMem = mem
        var remainingCores = cpus
        val tasks = new util.ArrayList[MesosTaskInfo]()
        val offerAttributes = toAttributeMap(offer.getAttributesList)
        val meetsConstraints = matchesAttributeRequirements(slaveOfferConstraints, offerAttributes)
        val slaveId = offer.getSlaveId.getValue
        val id = offer.getId.getValue
        var executorCount = slaveIdsWithExecutors.getOrElse(slaveId, 0)
        while (taskIdToSlaveId.size < executorLimit &&
            totalCoresAcquired < maxCores &&
            meetsConstraints &&
            remainingMem >= calculateTotalMemory(sc) &&
            remainingCores >= 1 &&
            failuresBySlaveId.getOrElse(slaveId, 0) < MAX_SLAVE_FAILURES &&
            executorCount < maxExecutorsPerSlave) {
          val coresToUse =
            math.min(maxCpusPerExecutor.getOrElse(Int.MaxValue),
              math.min(remainingCores, maxCores - totalCoresAcquired))
          totalCoresAcquired += coresToUse
          remainingCores -= coresToUse
          remainingMem -= memoryPerExecutor
          val taskId = newMesosTaskId(slaveId)
          taskIdToSlaveId(taskId) = slaveId
          executorCount += 1
          slaveIdsWithExecutors(slaveId) = executorCount
          coresByTaskId(taskId) = coresToUse
          // Gather cpu resources from the available resources and use them in the task.
          val (remainingResources, cpuResourcesToUse) =
            partitionResources(offer.getResourcesList, "cpus", coresToUse)
          val (_, memResourcesToUse) =
            partitionResources(remainingResources.asJava, "mem", calculateTotalMemory(sc))
          val taskBuilder = MesosTaskInfo.newBuilder()
            .setTaskId(TaskID.newBuilder().setValue(taskId.toString).build())
            .setSlaveId(offer.getSlaveId)
            .setCommand(createCommand(offer, coresToUse + extraCoresPerSlave, taskId))
            .setName("Task " + taskId)
            .addAllResources(cpuResourcesToUse.asJava)
            .addAllResources(memResourcesToUse.asJava)

          sc.conf.getOption("spark.mesos.executor.docker.image").foreach { image =>
            MesosSchedulerBackendUtil
              .setupContainerBuilderDockerInfo(image, sc.conf, taskBuilder.getContainerBuilder())
          }
          tasks.add(taskBuilder.build())
        }

        if (!tasks.isEmpty) {
          // accept the offer and launch the task
          logDebug(s"Accepting offer: $id with attributes: $offerAttributes mem: $mem cpu: $cpus")
          slaveIdToHost(offer.getSlaveId.getValue) = offer.getHostname
          d.launchTasks(Collections.singleton(offer.getId()), tasks, filters)
        } else {
          // Decline the offer
          logDebug(s"Declining offer: $id with attributes: $offerAttributes mem: $mem cpu: $cpus")
          d.declineOffer(offer.getId)
        }
      }
    }
  }


  override def statusUpdate(d: SchedulerDriver, status: TaskStatus) {
    val taskId = status.getTaskId.getValue
    val state = status.getState
    logInfo(s"Mesos task $taskId is now $state")
    val slaveId: String = status.getSlaveId.getValue
    stateLock.synchronized {
      // If the shuffle service is enabled, have the driver register with each one of the
      // shuffle services. This allows the shuffle services to clean up state associated with
      // this application when the driver exits. There is currently not a great way to detect
      // this through Mesos, since the shuffle services are set up independently.
      if (TaskState.fromMesos(state).equals(TaskState.RUNNING) &&
          slaveIdToHost.contains(slaveId) &&
          shuffleServiceEnabled &&
          !existingSlaveShuffleConnections.contains(slaveId)) {
        assume(mesosExternalShuffleClient.isDefined,
          "External shuffle client was not instantiated even though shuffle service is enabled.")
        // TODO: Remove this and allow the MesosExternalShuffleService to detect
        // framework termination when new Mesos Framework HTTP API is available.
        val externalShufflePort = conf.getInt("spark.shuffle.service.port", 7337)
        val hostname = slaveIdToHost.remove(slaveId).get
        logDebug(s"Connecting to shuffle service on slave $slaveId, " +
            s"host $hostname, port $externalShufflePort for app ${conf.getAppId}")
        mesosExternalShuffleClient.get
          .registerDriverWithShuffleService(hostname, externalShufflePort)
        existingSlaveShuffleConnections += slaveId
      } else if (TaskState.isFinished(TaskState.fromMesos(state))) {
        val slaveId = taskIdToSlaveId(taskId)

        // Remove the cores we have remembered for this task, if it's in the hashmap
        for (cores <- coresByTaskId.get(taskId)) {
          totalCoresAcquired -= cores
          coresByTaskId -= taskId
        }
        // If it was a failure, mark the slave as failed for blacklisting purposes
        if (TaskState.isFailed(TaskState.fromMesos(state))) {
          failuresBySlaveId(slaveId) = failuresBySlaveId.getOrElse(slaveId, 0) + 1
          if (failuresBySlaveId(slaveId) >= MAX_SLAVE_FAILURES) {
            logInfo(s"Blacklisting Mesos slave $slaveId due to too many failures; " +
                "is Spark installed on it?")
          }
        }
        executorTerminated(d, taskId, slaveId, s"Executor finished with state $state")
        // In case we'd rejected everything before but have now lost a node
        d.reviveOffers()
      }
    }
  }

  override def error(d: SchedulerDriver, message: String) {
    logError(s"Mesos error: $message")
    scheduler.error(message)
  }

  override def stop() {
    super.stop()
    if (mesosDriver != null) {
      mesosDriver.stop()
    }
  }

  override def frameworkMessage(d: SchedulerDriver, e: ExecutorID, s: SlaveID, b: Array[Byte]) {}

  /**
   * Called when a slave is lost or a Mesos task finished. Update local view on
   * what tasks are running and remove the terminated slave from the list of pending
   * slave IDs that we might have asked to be killed. It also notifies the driver
   * that an executor was removed.
   */
  private def executorTerminated(
      d: SchedulerDriver,
      executorId: String,
      slaveId: String,
      reason: String): Unit = {
    stateLock.synchronized {
      if (slaveIdsWithExecutors.contains(slaveId) && taskIdToSlaveId.contains(executorId)) {
        taskIdToSlaveId.remove(executorId)
        removeExecutor(executorId, SlaveLost(reason))
        val newCount = slaveIdsWithExecutors(slaveId) - 1
        if (newCount == 0) {
          slaveIdsWithExecutors.remove(slaveId)
        } else {
          slaveIdsWithExecutors(slaveId) = newCount
        }
      }
    }
  }

  override def slaveLost(d: SchedulerDriver, slaveId: SlaveID): Unit = {
    logInfo(s"Mesos slave lost: ${slaveId.getValue}")
    // Terminate all executors in the slave
    stateLock.synchronized {
      val lostExecutors = taskIdToSlaveId.filter(_._2.equals(slaveId.getValue)).map(_._1)
      lostExecutors.foreach { taskId =>
        executorTerminated(d, taskId, slaveId.getValue, "Mesos slave lost: " + slaveId.getValue)
      }
    }
  }

  override def executorLost(d: SchedulerDriver, e: ExecutorID, s: SlaveID, status: Int): Unit = {
    logInfo("Executor lost: %s".format(e.getValue))
    executorTerminated(d, e.getValue, s.getValue, "Mesos Executor lost: " + e.getValue)
  }

  override def applicationId(): String =
    Option(appId).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
    }

  override def doRequestTotalExecutors(requestedTotal: Int): Boolean = {
    // We don't truly know if we can fulfill the full amount of executors
    // since at coarse grain it depends on the amount of slaves available.
    logInfo("Capping the total amount of executors to " + requestedTotal)
    executorLimitOption = Some(requestedTotal)
    true
  }

  override def doKillExecutors(executorIds: Seq[String]): Boolean = {
    if (mesosDriver == null) {
      logWarning("Asked to kill executors before the Mesos driver was started.")
      return false
    }

    for (executorId <- executorIds) {
      if (taskIdToSlaveId.contains(executorId)) {
        mesosDriver.killTask(TaskID.newBuilder().setValue(executorId).build())
      } else {
        logWarning("Unable to find executor Id '" + executorId + "' in Mesos scheduler")
      }
    }
    // no need to adjust `executorLimitOption` since the AllocationManager already communicated
    // the desired limit through a call to `doRequestTotalExecutors`.
    // See [[o.a.s.scheduler.cluster.CoarseGrainedSchedulerBackend.killExecutors]]
    true
  }
}
