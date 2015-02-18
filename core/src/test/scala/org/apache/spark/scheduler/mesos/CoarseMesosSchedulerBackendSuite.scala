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

package org.apache.spark.scheduler.cluster

import org.scalatest.FunSuite
import org.apache.spark.{SparkEnv, SparkConf, SparkContext, LocalSparkContext}
import org.apache.spark.scheduler.cluster.mesos.{MemoryUtils, CoarseMesosSchedulerBackend}
import org.apache.mesos.Protos._
import org.apache.mesos.Protos.Value.Scalar
import org.apache.mesos.SchedulerDriver
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

import scala.collection.mutable
import akka.actor.ActorSystem
import java.util.Collections
import org.mockito.Matchers


class CoarseMesosSchedulerBackendSuite extends FunSuite with LocalSparkContext with MockitoSugar {
  def createOffer(offerId: String, slaveId: String, mem: Int, cpu: Int) = {
    val builder = Offer.newBuilder()
    builder.addResourcesBuilder()
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(mem))
    builder.addResourcesBuilder()
      .setName("cpus")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(cpu))
    builder.setId(OfferID.newBuilder().setValue(offerId).build()).setFrameworkId(FrameworkID.newBuilder().setValue("f1"))
      .setSlaveId(SlaveID.newBuilder().setValue(slaveId)).setHostname(s"host${slaveId}").build()
  }

  test("mesos supports killing and limiting executors") {
    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]

    val se = mock[SparkEnv]
    val sparkConf = new SparkConf
    when(se.actorSystem).thenReturn(ActorSystem.create())
    val sc = mock[SparkContext]
    when(sc.executorMemory).thenReturn(100)
    when(sc.getSparkHome()).thenReturn(Option("/path"))
    when(sc.executorEnvs).thenReturn(new mutable.HashMap[String, String])
    when(sc.conf).thenReturn(sparkConf)
    when(sc.env).thenReturn(se)

    when(taskScheduler.sc).thenReturn(sc)

    sparkConf.set("spark.driver.host", "driverHost")
    sparkConf.set("spark.driver.port", "1234")

    val minMem = MemoryUtils.calculateTotalMemory(sc).toInt
    val minCpu = 4

    val mesosOffers = new java.util.ArrayList[Offer]
    mesosOffers.add(createOffer("o1", "s1", minMem, minCpu))

    when(
      driver.launchTasks(
        Matchers.eq(Collections.singleton(mesosOffers.get(0).getId)),
        Matchers.any(),
        Matchers.any[Filters]()
      )
    ).thenReturn(Status.valueOf(1))

    when(
      driver.killTask(TaskID.newBuilder().setValue("0").build())
    ).thenReturn(Status.valueOf(1))

    when(
      driver.declineOffer(OfferID.newBuilder().setValue("o2").build())
    ).thenReturn(Status.valueOf(1))

    val backend = new CoarseMesosSchedulerBackend(taskScheduler, sc, "master")
    backend.driver = driver
    backend.resourceOffers(driver, mesosOffers)

    assert(backend.doKillExecutors(Seq("s1/0")))
    assert(backend.executorLimit.get.equals(0))

    val mesosOffers2 = new java.util.ArrayList[Offer]
    mesosOffers2.add(createOffer("o2", "s2", minMem, minCpu))
    backend.resourceOffers(driver, mesosOffers2)
    // Verify we didn't launch any new executor
    assert(backend.pendingRemovedSlaveIds.size.equals(1))

    when(
      driver.launchTasks(
        Matchers.eq(Collections.singleton(mesosOffers2.get(0).getId)),
        Matchers.any(),
        Matchers.any[Filters]()
      )
    ).thenReturn(Status.valueOf(1))

    backend.doRequestTotalExecutors(2)
    backend.resourceOffers(driver, mesosOffers2)
    backend.slaveLost(driver, SlaveID.newBuilder().setValue("s1").build())
    assert(backend.pendingRemovedSlaveIds.size.equals(0))
  }

  test("mesos supports killing and relaunching tasks with executors") {
    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]

    val se = mock[SparkEnv]
    val sparkConf = new SparkConf
    when(se.actorSystem).thenReturn(ActorSystem.create())
    val sc = mock[SparkContext]
    when(sc.executorMemory).thenReturn(100)
    when(sc.getSparkHome()).thenReturn(Option("/path"))
    when(sc.executorEnvs).thenReturn(new mutable.HashMap[String, String])
    when(sc.conf).thenReturn(sparkConf)
    when(sc.env).thenReturn(se)

    when(taskScheduler.sc).thenReturn(sc)

    // Enable shuffle service so it will require extra resources
    sparkConf.set("spark.shuffle.service.enabled", "true")
    sparkConf.set("spark.driver.host", "driverHost")
    sparkConf.set("spark.driver.port", "1234")

    val minMem = MemoryUtils.calculateTotalMemory(sc).toInt + 1024
    val minCpu = 4

    val mesosOffers = new java.util.ArrayList[Offer]
    mesosOffers.add(createOffer("o1", "s1", minMem, minCpu))

    when(
      driver.launchTasks(
        Matchers.eq(Collections.singleton(mesosOffers.get(0).getId)),
        Matchers.any(),
        Matchers.any[Filters]
      )
    ).thenReturn(Status.valueOf(1))

    val offer2 = createOffer("o2", "s1", minMem, 1);

    when(
      driver.launchTasks(
        Matchers.eq(Collections.singleton(offer2.getId)),
        Matchers.any(),
        Matchers.any[Filters]
      )
    ).thenReturn(Status.valueOf(1))

    when(driver.reviveOffers()).thenReturn(Status.valueOf(1))

    val backend = new CoarseMesosSchedulerBackend(taskScheduler, sc, "master")
    backend.driver = driver
    backend.resourceOffers(driver, mesosOffers)

    // Simulate task killed, but executor is still running
    val status = TaskStatus.newBuilder()
      .setTaskId(TaskID.newBuilder().setValue("0").build())
      .setSlaveId(SlaveID.newBuilder().setValue("s1").build())
      .setState(TaskState.TASK_KILLED)
      .build

    backend.statusUpdate(driver, status)
    assert(backend.slaveIdsWithExecutors.contains("s1").equals(false))

    mesosOffers.clear()
    mesosOffers.add(offer2)
    backend.resourceOffers(driver, mesosOffers)
    assert(backend.slaveIdsWithExecutors.contains("s1").equals(true))
  }
}
