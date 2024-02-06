/*
 * Copyright (c) 2016-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.external.analytics.scalasdk

// java
import java.time.{Instant, LocalDate}
import java.util.UUID

// circe
import io.circe.{Decoder, Encoder, Json, JsonObject}
import io.circe.generic.semiauto._
import io.circe.syntax._

// iglu
import com.snowplowanalytics.iglu.core.SelfDescribingData
//import com.snowplowanalytics.iglu.core.circe.implicits._

// This library
import com.snowplowanalytics.snowplow.external.analytics.scalasdk.decode.{DecodeResult, Parser}
import com.snowplowanalytics.snowplow.external.analytics.scalasdk.SnowplowEvent.{Contexts, UnstructEvent}
//import com.snowplowanalytics.snowplow.external.analytics.scalasdk.SnowplowEvent._
import com.snowplowanalytics.snowplow.external.analytics.scalasdk.encode.TsvEncoder

/**
 * Case class representing a canonical Snowplow event.
 *
 * @see https://docs.snowplowanalytics.com/docs/understanding-your-pipeline/canonical-event/
 */
// format: off
case class Event(
  api_key:                Option[String],
  app_id:                 Option[String],
  platform:               Option[String],
  enricher_tstamp:        Option[Instant],
  collector_tstamp:       Instant,
  dvce_created_tstamp:    Option[Instant],
  event:                  Option[String],
  event_id:               UUID,
  name_tracker:           Option[String],
  v_tracker:              Option[String],
  v_collector:            String,
  v_etl:                  String,
  user_id:                Option[String],
  installation_id:        Option[String],
  unique_id:              Option[String],
  user_ipaddress:         Option[String],
  network_userid:         Option[String],
  geo_country:            Option[String],
  geo_country_name:       Option[String],
  geo_region:             Option[String],
  geo_city:               Option[String],
  geo_zipcode:            Option[String],
  geo_latitude:           Option[Double],
  geo_longitude:          Option[Double],
  geo_region_name:        Option[String],
  mkt_medium:             Option[String],
  mkt_source:             Option[String],
  mkt_term:               Option[String],
  mkt_content:            Option[String],
  mkt_campaign:           Option[String],
  contexts:               Contexts,
  unstruct_event:         UnstructEvent,
  useragent:              Option[String],
  geo_timezone:           Option[String],
  etl_tags:               Option[String],
  dvce_sent_tstamp:       Option[Instant],
  derived_contexts:       Contexts,
  derived_tstamp:         Option[Instant],
  event_vendor:           Option[String],
  event_name:             Option[String],
  event_format:           Option[String],
  event_version:          Option[String],
  event_fingerprint:      Option[String],
  true_tstamp:            Option[Instant],
  event_tstamp:           Option[Instant],
  event_quality:          Option[Int],
  sandbox_mode:           Option[Boolean],
  backfill_mode:          Option[Boolean],
  `date_`:                Option[LocalDate]
) {
  // format: on

  /**
   * Extracts metadata from the event containing information about the types and Iglu URIs of its shred properties
   */
  def inventory: Set[Data.ShreddedType] = {
    val unstructEvent = unstruct_event.data.toSet
      .map((ue: SelfDescribingData[Json]) => Data.ShreddedType(Data.UnstructEvent, ue.schema))

    val derivedContexts = derived_contexts.data.toSet
      .map((ctx: SelfDescribingData[Json]) => Data.ShreddedType(Data.Contexts(Data.DerivedContexts), ctx.schema))

    val customContexts = contexts.data.toSet
      .map((ctx: SelfDescribingData[Json]) => Data.ShreddedType(Data.Contexts(Data.CustomContexts), ctx.schema))

    customContexts ++ derivedContexts ++ unstructEvent
  }

  /**
   * Returns the event as a map of keys to Circe JSON values, while dropping inventory fields
   */
  def atomic: Map[String, Json] = jsonMap - "contexts" - "unstruct_event" - "derived_contexts"

  /**
   * Returns the event as a list of key/Circe JSON value pairs.
   * Unlike `jsonMap` and `atomic`, these keys use the ordering of the canonical event model
   */
  def ordered: List[(String, Option[Json])] =
    Event.parser.knownKeys.map(key => (key.name, jsonMap.get(key.name)))

  /**
   * Returns a compound JSON field containing information about an event's latitude and longitude,
   * or None if one of these fields doesn't exist
   */
  def geoLocation: Option[(String, Json)] =
    for {
      lat <- geo_latitude
      lon <- geo_longitude
    } yield "geo_location" -> s"$lat,$lon".asJson

  /**
   * Transforms the event to a validated JSON whose keys are the field names corresponding to the
   * EnrichedEvent POJO of the Scala Common Enrich project. If the lossy argument is true, any
   * self-describing events in the fields (unstruct_event, contexts and derived_contexts) are returned
   * in a "shredded" format (e.g. "unstruct_event_com_acme_1_myField": "value"), otherwise a standard
   * self-describing format is used.
   *
   * @param lossy Whether unstruct_event, contexts and derived_contexts should be flattened
   */
  def toJson(lossy: Boolean): Json =
    if (lossy)
      JsonObject
        .fromMap(
          atomic ++ contexts.toShreddedJson.toMap ++ derived_contexts.toShreddedJson.toMap ++ unstruct_event.toShreddedJson.toMap ++ geoLocation
        )
        .asJson
    else
      this.asJson

  /** Create the TSV representation of this event. */
  def toTsv: String = TsvEncoder.encode(this)

  /**
   * This event as a map of keys to Circe JSON values
   */
  private lazy val jsonMap: Map[String, Json] = this.asJsonObject.toMap
}

object Event {

  object unsafe {
    implicit def unsafeEventDecoder: Decoder[Event] = deriveDecoder[Event]
  }

  /**
   * Automatically derived Circe encoder
   */
  implicit val jsonEncoder: Encoder.AsObject[Event] = deriveEncoder[Event]

  implicit def eventDecoder: Decoder[Event] = unsafe.unsafeEventDecoder.ensure(validate.validator)

  /**
   * Derived TSV parser for the Event class
   */
  private val parser: Parser[Event] = Parser.deriveFor[Event].get

  /**
   * Converts a string with an enriched event TSV to an Event instance,
   * or a ValidatedNel containing information about errors
   *
   * @param line Enriched event TSV line
   */
  def parse(line: String): DecodeResult[Event] =
    parser.parse(line)

  def minimal(
    id: UUID,
    collectorTstamp: Instant,
    vCollector: String,
    vEtl: String
  ): Event =
    Event(
      None,
      None,
      None,
      None,
      collectorTstamp,
      None,
      None,
      id,
      None,
      None,
      vCollector,
      vEtl,
      None, None, None, None, None, None, None, None, None, None,
      None, None, None, None, None, None, None, None,
      Contexts(Nil),
      UnstructEvent(None),
      None,
      None,
      None,
      None,
      Contexts(Nil),
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None
    )
}
