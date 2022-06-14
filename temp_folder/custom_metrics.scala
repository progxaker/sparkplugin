package custom_plugin

import java.util.{Map => JMap}
import java.util.Arrays
import scala.collection.JavaConverters._

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.SparkContext
import org.apache.spark.status.api.v1.{StageData, StageStatus}
import org.apache.spark.status.AppStatusStore

private[spark] class DemoMetricsPlugin extends SparkPlugin {

  // Return the plugin's driver-side component.
  override def driverPlugin(): DriverPlugin = {
    new DriverPlugin() {
      override def init(sc: SparkContext, myContext: PluginContext): JMap[String, String] = {
        val metricRegistry = myContext.metricRegistry
        //val sourceName = "DAGScheduler"
        // Gauge for testing
        val conf = sc.getConf
          var _statusStore: AppStatusStore = _
          val store = status_store.createLiveStore(conf, AppStatusSource.createSource(conf))
          val stageList: Array[Int] = store.stageList(Arrays.asList(StageStatus.ACTIVE)).map(_.stageId).toArray
            metricRegistry.register(MetricRegistry.name("totalTasks"), new Gauge[Int] {
            override def getValue: Int = stageList.numTasks.sum
          })
        Map.empty[String, String].asJava
      }
    }
  }

  // Return the plugin's executor-side component.
  override def executorPlugin(): ExecutorPlugin = {
    new ExecutorPlugin {
      override def init(myContext:PluginContext, extraConf:JMap[String, String]) = {
        // Gauge for testing
        val metricRegistry = myContext.metricRegistry
        //val sourceName = "DAGScheduler"
        metricRegistry.register(MetricRegistry.name("totalTasks"), new Gauge[Int] {
          override def getValue: Int = 1
        })
      }
    }
  }

}
