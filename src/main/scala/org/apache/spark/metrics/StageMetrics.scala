package org.apache.spark.metrics

import java.util.{Map => JMap}
import scala.collection.JavaConverters._

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.SparkContext
import org.apache.spark.status.api.v1.{StageData, StageStatus}
import org.apache.spark.status.AppStatusStore
import org.apache.spark.internal.Logging

import com.codahale.metrics.{Gauge, MetricRegistry}

class StageMetrics extends DriverPlugin with Logging {
  private var sparkContext: SparkContext = _
  private var pluginContext: PluginContext = _

  private val metricStagePrefix: String = "stage"

  def get_numTasks(currentStage: StageData): Int = {
    return currentStage.numTasks
  }

  def get_numActiveTasks(currentStage: StageData): Int = {
    return currentStage.numActiveTasks
  }

  def get_numCompleteTasks(currentStage: StageData): Int = {
    return currentStage.numCompleteTasks
  }

  // numTasks, numActiveTasks, numCompleteTasks
  def get_metric_value(metricName: String, currentStageId: Int): Int = {
    val currentStage: StageData = sparkContext.statusStore.stageList(null).find(_.stageId == currentStageId).head
    return metricName match {
      case "numTasks" => get_numTasks(currentStage)
      case "numActiveTasks" => get_numActiveTasks(currentStage)
      case "numCompleteTasks" => get_numCompleteTasks(currentStage)
      case _ =>
        logWarning(s"$metricName with id ${currentStageId.toString} is not found")
        0
    }
  }

  def is_metric_registred(currentStageId: Int, metricName: String): Boolean = {
    val metricRegistryName: String = s"$metricStagePrefix.${currentStageId.toString}.$metricName"
    return pluginContext.metricRegistry.getNames().contains(metricRegistryName)
  }

  // TODO: can be many active stages
  def registerMetrics(): Int = {
    val metricNames: Seq[String] = Seq("numTasks", "numActiveTasks", "numCompleteTasks")
    val metricRegistry = pluginContext.metricRegistry
    val currentStageId: Int = sparkContext.statusStore.stageList(null).head.stageId

    metricNames.foreach(x =>
      if(!is_metric_registred(currentStageId, x)) {
        metricRegistry.register(MetricRegistry.name(metricStagePrefix, currentStageId.toString, x), new Gauge[Int] {
          override def getValue: Int = {
            val metricName = x
            val metricStageId = currentStageId
            get_metric_value(metricName, currentStageId)
          }
        })
      })
      return currentStageId
  }

  private[spark] override def init(sc: SparkContext, pc: PluginContext): JMap[String, String] = {
    val metricRegistry = pc.metricRegistry
    //val sourceName = "DAGScheduler"

    sparkContext = sc
    pluginContext = pc

    metricRegistry.register(MetricRegistry.name(metricStagePrefix, "currentStageId"), new Gauge[Int] {
      override def getValue: Int = registerMetrics()
    })
    Map.empty[String, String].asJava
  }
}
