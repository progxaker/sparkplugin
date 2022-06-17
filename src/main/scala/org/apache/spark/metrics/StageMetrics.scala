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

  def get_numTasks(metricStage: StageData): Int = {
    val currentStage: StageData = sparkContext.statusStore.stageList(null).head
    if(metricStage.stageId == currentStage.stageId) return 1
    else return 0
  }

  def get_numTasks(metricStage: StageData): Int = {
    return metricStage.numTasks
  }

  def get_numActiveTasks(metricStage: StageData): Int = {
    return metricStage.numActiveTasks
  }

  def get_numCompleteTasks(metricStage: StageData): Int = {
    return metricStage.numCompleteTasks
  }

  // numTasks, numActiveTasks, numCompleteTasks
  def get_metric_value(metricName: String, metricStageId: Int): Int = {
    val metricStage: StageData = sparkContext.statusStore.stageList(null).find(_.stageId == metricStageId).head
    return metricName match {
      case "status" => get_stageStatus(metricStage)
      case "numTasks" => get_numTasks(metricStage)
      case "numActiveTasks" => get_numActiveTasks(metricStage)
      case "numCompleteTasks" => get_numCompleteTasks(metricStage)
      case _ =>
        logWarning(s"$metricName with id ${metricStageId.toString} is not found")
        0
    }
  }

  def is_metric_registred(metricStageId: Int, metricName: String): Boolean = {
    val metricRegistryName: String = s"$metricStagePrefix.${metricStageId.toString}.$metricName"
    return pluginContext.metricRegistry.getNames().contains(metricRegistryName)
  }

  // TODO: can be many active stages
  def registerMetrics(): Int = {
    val metricNames: Seq[String] = Seq("status", "numTasks", "numActiveTasks", "numCompleteTasks")
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
