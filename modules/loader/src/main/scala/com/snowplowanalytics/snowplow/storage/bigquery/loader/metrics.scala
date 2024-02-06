/*
 * Copyright (c) 2018-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.storage.bigquery.loader

import com.snowplowanalytics.snowplow.storage.bigquery.common.config.Environment.LoaderEnvironment
import com.snowplowanalytics.snowplow.storage.bigquery.common.config.model.Monitoring.Dropwizard

import com.codahale.metrics.{MetricRegistry, Slf4jReporter}
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit

import org.apache.beam.sdk.metrics.Distribution
import org.apache.beam.sdk.metrics.Metrics

object metrics {
  private val metrics = new MetricRegistry()
  private val logger  = LoggerFactory.getLogger("bigquery.loader.metrics")
  val reporter        = Slf4jReporter.forRegistry(metrics).outputTo(logger).build()

  val latencyDistribution: Distribution = Metrics.distribution("pipeline", "latency");

  // Take the latest value every 'period' second.
  def startReporter(reporter: Slf4jReporter, env: LoaderEnvironment): Unit = env.monitoring.dropwizard match {
    case Some(Dropwizard(period)) => reporter.start(period.toSeconds, TimeUnit.SECONDS)
    case _                        => ()
  }
}
