/*
 * Copyright (c) 2020-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.external.analytics.scalasdk.encode

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate}
import java.util.UUID
import io.circe.syntax._
import com.snowplowanalytics.snowplow.external.analytics.scalasdk.SnowplowEvent._
import com.snowplowanalytics.snowplow.external.analytics.scalasdk.Event

object TsvEncoder {
  sealed trait FieldEncoder[T] {
    def encodeField(t: T): String
  }

  implicit object StringEncoder extends FieldEncoder[String] {
    def encodeField(str: String) = str
  }

  implicit object InstantEncoder extends FieldEncoder[Instant] {
    def encodeField(inst: Instant): String =
      DateTimeFormatter.ISO_INSTANT
        .format(inst)
        .replace("T", " ")
        .dropRight(1) // remove trailing 'Z'
  }

  implicit object UuidEncoder extends FieldEncoder[UUID] {
    def encodeField(uuid: UUID): String = uuid.toString
  }

  implicit object IntEncoder extends FieldEncoder[Int] {
    def encodeField(int: Int): String = int.toString
  }

  implicit object DoubleEncoder extends FieldEncoder[Double] {
    def encodeField(doub: Double): String = doub.toString
  }

  implicit object BooleanEncoder extends FieldEncoder[Boolean] {
    def encodeField(bool: Boolean): String = if (bool) "1" else "0"
  }

  implicit object LocalDateEncoder extends FieldEncoder[LocalDate] {
    def encodeField(date: LocalDate): String =
      date.toString
  }

  implicit object ContextsEncoder extends FieldEncoder[Contexts] {
    def encodeField(ctxts: Contexts): String =
      if (ctxts.data.isEmpty)
        ""
      else
        ctxts.asJson.noSpaces
  }

  implicit object UnstructEncoder extends FieldEncoder[UnstructEvent] {
    def encodeField(unstruct: UnstructEvent): String =
      if (unstruct.data.isDefined)
        unstruct.asJson.noSpaces
      else
        ""
  }

  def encode[A](a: A)(implicit ev: FieldEncoder[A]): String =
    ev.encodeField(a)

  def encode[A](optA: Option[A])(implicit ev: FieldEncoder[A]): String =
    optA.map(a => ev.encodeField(a)).getOrElse("")

  def encode(event: Event): String =
      encode(event.api_key) + "\t" +
      encode(event.app_id) + "\t" +
      encode(event.platform) + "\t" +
      encode(event.enricher_tstamp) + "\t" +
      encode(event.collector_tstamp) + "\t" +
      encode(event.dvce_created_tstamp) + "\t" +
      encode(event.event) + "\t" +
      encode(event.event_id) + "\t" +
      encode(event.name_tracker) + "\t" +
      encode(event.v_tracker) + "\t" +
      encode(event.v_collector) + "\t" +
      encode(event.v_etl) + "\t" +
      encode(event.user_id) + "\t" +
      encode(event.installation_id) + "\t" +
      encode(event.unique_id) + "\t" +
      encode(event.user_ipaddress) + "\t" +
      encode(event.network_userid) + "\t" +
      encode(event.geo_country) + "\t" +
      encode(event.geo_country_name) + "\t" +
      encode(event.geo_region) + "\t" +
      encode(event.geo_city) + "\t" +
      encode(event.geo_zipcode) + "\t" +
      encode(event.geo_latitude) + "\t" +
      encode(event.geo_longitude) + "\t" +
      encode(event.geo_region_name) + "\t" +
      encode(event.mkt_medium) + "\t" +
      encode(event.mkt_source) + "\t" +
      encode(event.mkt_term) + "\t" +
      encode(event.mkt_content) + "\t" +
      encode(event.mkt_campaign) + "\t" +
      encode(event.contexts) + "\t" +
      encode(event.unstruct_event) + "\t" +
      encode(event.useragent) + "\t" +
      encode(event.geo_timezone) + "\t" +
      encode(event.etl_tags) + "\t" +
      encode(event.dvce_sent_tstamp) + "\t" +
      encode(event.derived_contexts) + "\t" +
      encode(event.derived_tstamp) + "\t" +
      encode(event.event_vendor) + "\t" +
      encode(event.event_name) + "\t" +
      encode(event.event_format) + "\t" +
      encode(event.event_version) + "\t" +
      encode(event.event_fingerprint) + "\t" +
      encode(event.true_tstamp) + "\t" +
      encode(event.event_tstamp) + "\t" +
      encode(event.event_quality) + "\t" +
      encode(event.sandbox_mode) + "\t" +
      encode(event.backfill_mode) + "\t" +
      encode(event.date_)
}
