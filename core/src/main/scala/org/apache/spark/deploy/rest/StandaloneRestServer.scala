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

package org.apache.spark.deploy.rest

import java.io.File
import javax.servlet.http.HttpServletResponse

import akka.actor.ActorRef

import org.apache.spark.{SparkConf, SPARK_VERSION => sparkVersion}
import org.apache.spark.util.{AkkaUtils, Utils}
import org.apache.spark.deploy.{Command, DeployMessages, DriverDescription}
import org.apache.spark.deploy.ClientArguments._

/**
 * A server that responds to requests submitted by the [[RestClient]].
 * This is intended to be embedded in the standalone Master and used in cluster mode only.
 *
 * This server responds with different HTTP codes depending on the situation:
 *   200 OK - Request was processed successfully
 *   400 BAD REQUEST - Request was malformed, not successfully validated, or of unexpected type
 *   468 UNKNOWN PROTOCOL VERSION - Request specified a protocol this server does not understand
 *   500 INTERNAL SERVER ERROR - Server throws an exception internally while processing the request
 *
 * The server always includes a JSON representation of the relevant [[SubmitRestProtocolResponse]]
 * in the HTTP body. If an error occurs, however, the server will include an [[ErrorResponse]]
 * instead of the one expected by the client. If the construction of this error response itself
 * fails, the response will consist of an empty body with a response code that indicates internal
 * server error.
 *
 * @param host the address this server should bind to
 * @param requestedPort the port this server will attempt to bind to
 * @param masterActor reference to the Master actor to which requests can be sent
 * @param masterUrl the URL of the Master new drivers will attempt to connect to
 * @param masterConf the conf used by the Master
 */
private[spark] class StandaloneRestServer(
    host: String,
    requestedPort: Int,
    masterActor: ActorRef,
    masterUrl: String,
    masterConf: SparkConf)
  extends RestServer(
    host,
    requestedPort,
    masterConf,
    new StandaloneSubmitRequestServlet(masterActor, masterUrl, masterConf),
    new StandaloneKillRequestServlet(masterActor, masterConf),
    new StandaloneStatusRequestServlet(masterActor, masterConf)) {}

/**
 * A servlet for handling kill requests passed to the [[StandaloneRestServer]].
 */
private[rest] class StandaloneKillRequestServlet(masterActor: ActorRef, conf: SparkConf)
  extends KillRequestServlet {

  protected override def handleKill(submissionId: String): KillSubmissionResponse = {
    val askTimeout = AkkaUtils.askTimeout(conf)
    val response = AkkaUtils.askWithReply[DeployMessages.KillDriverResponse](
      DeployMessages.RequestKillDriver(submissionId), masterActor, askTimeout)
    val k = new KillSubmissionResponse
    k.serverSparkVersion = sparkVersion
    k.message = response.message
    k.submissionId = submissionId
    k.success = response.success
    k
  }
}

/**
 * A servlet for handling status requests passed to the [[StandaloneRestServer]].
 */
private[rest] class StandaloneStatusRequestServlet(masterActor: ActorRef, conf: SparkConf)
  extends StatusRequestServlet {

  protected override def handleStatus(submissionId: String): SubmissionStatusResponse = {
    val askTimeout = AkkaUtils.askTimeout(conf)
    val response = AkkaUtils.askWithReply[DeployMessages.DriverStatusResponse](
      DeployMessages.RequestDriverStatus(submissionId), masterActor, askTimeout)
    val message = response.exception.map { s"Exception from the cluster:\n" + formatException(_) }
    val d = new SubmissionStatusResponse
    d.serverSparkVersion = sparkVersion
    d.submissionId = submissionId
    d.success = response.found
    d.driverState = response.state.map(_.toString).orNull
    d.workerId = response.workerId.orNull
    d.workerHostPort = response.workerHostPort.orNull
    d.message = message.orNull
    d
  }
}

/**
 * A servlet for handling submit requests passed to the [[StandaloneRestServer]].
 */
private[rest] class StandaloneSubmitRequestServlet(
    masterActor: ActorRef,
    masterUrl: String,
    conf: SparkConf)
  extends SubmitRequestServlet {

  /**
   * Build a driver description from the fields specified in the submit request.
   *
   * This involves constructing a command that takes into account memory, java options,
   * classpath and other settings to launch the driver. This does not currently consider
   * fields used by python applications since python is not supported in standalone
   * cluster mode yet.
   */
  private def buildDriverDescription(request: CreateSubmissionRequest): DriverDescription = {
    // Required fields, including the main class because python is not yet supported
    val appResource = Option(request.appResource).getOrElse {
      throw new SubmitRestMissingFieldException("Application jar is missing.")
    }
    val mainClass = Option(request.mainClass).getOrElse {
      throw new SubmitRestMissingFieldException("Main class is missing.")
    }

    // Optional fields
    val sparkProperties = request.sparkProperties
    val driverMemory = sparkProperties.get("spark.driver.memory")
    val driverCores = sparkProperties.get("spark.driver.cores")
    val driverExtraJavaOptions = sparkProperties.get("spark.driver.extraJavaOptions")
    val driverExtraClassPath = sparkProperties.get("spark.driver.extraClassPath")
    val driverExtraLibraryPath = sparkProperties.get("spark.driver.extraLibraryPath")
    val superviseDriver = sparkProperties.get("spark.driver.supervise")
    val appArgs = request.appArgs
    val environmentVariables = request.environmentVariables

    // Construct driver description
    val conf = new SparkConf(false)
      .setAll(sparkProperties)
      .set("spark.master", masterUrl)
    val extraClassPath = driverExtraClassPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraLibraryPath = driverExtraLibraryPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraJavaOpts = driverExtraJavaOptions.map(Utils.splitCommandString).getOrElse(Seq.empty)
    val sparkJavaOpts = Utils.sparkJavaOpts(conf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    val command = new Command(
      "org.apache.spark.deploy.worker.DriverWrapper",
      Seq("{{WORKER_URL}}", "{{USER_JAR}}", mainClass) ++ appArgs, // args to the DriverWrapper
      environmentVariables, extraClassPath, extraLibraryPath, javaOpts)
    val actualDriverMemory = driverMemory.map(Utils.memoryStringToMb).getOrElse(DEFAULT_MEMORY)
    val actualDriverCores = driverCores.map(_.toInt).getOrElse(DEFAULT_CORES)
    val actualSuperviseDriver = superviseDriver.map(_.toBoolean).getOrElse(DEFAULT_SUPERVISE)
    new DriverDescription(
      appResource, actualDriverMemory, actualDriverCores, actualSuperviseDriver, command)
  }

  /**
   * Handle the submit request and construct an appropriate response to return to the client.
   *
   * This assumes that the request message is already successfully validated.
   * If the request message is not of the expected type, return error to the client.
   */
  protected override def handleSubmit(
      requestMessageJson: String,
      requestMessage: SubmitRestProtocolMessage,
      responseServlet: HttpServletResponse): SubmitRestProtocolResponse = {
    requestMessage match {
      case submitRequest: CreateSubmissionRequest =>
        val askTimeout = AkkaUtils.askTimeout(conf)
        val driverDescription = buildDriverDescription(submitRequest)
        val response = AkkaUtils.askWithReply[DeployMessages.SubmitDriverResponse](
          DeployMessages.RequestSubmitDriver(driverDescription), masterActor, askTimeout)
        val submitResponse = new CreateSubmissionResponse
        submitResponse.serverSparkVersion = sparkVersion
        submitResponse.message = response.message
        submitResponse.success = response.success
        submitResponse.submissionId = response.driverId.orNull
        val unknownFields = findUnknownFields(requestMessageJson, requestMessage)
        if (unknownFields.nonEmpty) {
          // If there are fields that the server does not know about, warn the client
          submitResponse.unknownFields = unknownFields
        }
        submitResponse
      case unexpected =>
        responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        handleError(s"Received message of unexpected type ${unexpected.messageType}.")
    }
  }
}
