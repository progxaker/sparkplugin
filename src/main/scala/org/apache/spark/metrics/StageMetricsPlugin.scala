package org.apache.spark.metrics

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}

class StageMetricsPlugin extends SparkPlugin {

  // Return the plugin's driver-side component.
  override def driverPlugin(): DriverPlugin = new StageMetrics()

  // Return the plugin's executor-side component.
  override def executorPlugin(): ExecutorPlugin = null

}
