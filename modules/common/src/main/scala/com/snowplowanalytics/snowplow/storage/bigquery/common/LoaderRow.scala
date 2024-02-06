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
package com.snowplowanalytics.snowplow.storage.bigquery.common

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.schemaddl.bigquery._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Schema => DdlSchema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.snowplow.external.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.external.badrows.{BadRow, Failure, FailureDetails, Payload, Processor}
import cats.Monad
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.effect.Clock
import cats.implicits._
import com.google.api.services.bigquery.model.TableRow
import io.circe.generic.semiauto.deriveEncoder
import io.circe.{Encoder, Json}
import io.circe.syntax._
import org.joda.time.Instant

/** Row ready to be passed into Loader stream and Mutator topic */
case class LoaderRow(collectorTstamp: Instant, tableName: String, data: TableRow)

object LoaderRow {

  type Transformed = ValidatedNel[FailureDetails.LoaderIgluError, List[(String, AnyRef)]]

  val LoadTstampField = Field("load_tstamp", Type.Timestamp, Mode.Nullable)

  /**
    * Parse the enriched TSV line into a loader row, that can be loaded into BigQuery.
    * If Loader is able to figure out that row cannot be loaded into BQ,
    * it will return a BadRow with a detailed error message.
    * If this preliminary check was passed, but the row cannot be loaded for other reasons,
    * it will be forwarded to a "failed inserts" topic, without additional information.
    * @param igluClient A Resolver to be used for schema lookups.
    * @param record An enriched TSV line.
    * @return Either a BadRow with error messages or a row that is ready to be loaded.
    */
  def parse[F[_]: Monad: RegistryLookup: Clock](igluClient: Resolver[F], processor: Processor)(
    projectId: String,
    record: String
  ): List[F[Either[BadRow, LoaderRow]]] =
    Event.parse(record) match {
      case Validated.Valid(event) =>
        fromEvent[F](igluClient, processor)(projectId, event)
      case Validated.Invalid(error) =>
        val badRowError = BadRow.LoaderParsingError(processor, error, Payload.RawPayload(record))
        List(Monad[F].pure(badRowError.asLeft))
    }

  def filterContext[F[_]: Monad: RegistryLookup: Clock] (igluClient: Resolver[F], event: Event, contextName: String) : F[Validated[NonEmptyList[FailureDetails.LoaderIgluError], List[(String, AnyRef)]]] = {
    var foundContext: F[Validated[NonEmptyList[FailureDetails.LoaderIgluError], List[(String, AnyRef)]]] = Monad[F].pure(List.empty[(String, AnyRef)].validNel[FailureDetails.LoaderIgluError])

    if (event.contexts.data.nonEmpty) {
      val selfDescribingContextFilter = event.contexts.data.filter(x => x.schema.name == contextName)
      if (selfDescribingContextFilter.nonEmpty) {
        val selfDescribingDeviceContext = selfDescribingContextFilter.apply(0)

        foundContext = transformJson[F](igluClient, selfDescribingDeviceContext.schema)(selfDescribingDeviceContext.data).map(_.map { row =>
          List(("ctx_" + contextName, Adapter.adaptRow(row)))
        })
      }
    }

    foundContext
  }

  def eventLoaderRow[F[_]: Monad: RegistryLookup: Clock](igluClient: Resolver[F], processor: Processor)(
    projectId: String, event: Event, payload: Payload.LoaderPayload
  ):  F[Either[BadRow, LoaderRow]] = {
    val atomic = transformAtomic(event)
    val selfDescribingEvent = event
      .unstruct_event
      .data
      .map {
        case SelfDescribingData(schema, data) =>
          transformJson[F](igluClient, schema)(data).map(_.map { row => {
            row match {
              case Row.Record(_) => List(("params", Adapter.adaptRow(row)))
              case _ => List(("params", null))
            }
          }
        })
      }
      .getOrElse(Monad[F].pure(List.empty[(String, AnyRef)].validNel[FailureDetails.LoaderIgluError]))

    var transformedEvent: F[Either[BadRow.LoaderIgluError, List[(String, AnyRef)]]] = Monad[F].pure(Right(List.empty[(String, AnyRef)]))

    val sessionContext = filterContext[F](igluClient, event, "session_context")
    val deviceContext = filterContext[F](igluClient, event, "device_context")

    transformedEvent = (selfDescribingEvent, deviceContext, sessionContext).mapN { (e, d, s) =>
      (e, d, s)
        .mapN { (e, d, s) =>
          e ++ d ++ s
        }
        .leftMap(details => Failure.LoaderIgluErrors(details))
        .leftMap(failure => BadRow.LoaderIgluError(processor, failure, payload))
        .toEither
    }

    transformedEvent.map { badRowsOrJsons =>
      for {
        jsons <- badRowsOrJsons
        atomic <- atomic.leftMap(failure => BadRow.LoaderRuntimeError(processor, failure, payload))
        timestamp = new Instant(event.collector_tstamp.toEpochMilli)
        dataset = if (event.backfill_mode.getOrElse(false)) "backfill" else "load"
        tableName = s"${projectId}.${event.app_id.get}_$dataset.${event.event_name.get}"
      } yield LoaderRow(
        collectorTstamp = timestamp,
        tableName = tableName,
        data = (atomic ++ jsons).foldLeft(new TableRow())((r, kv) => r.set(kv._1, kv._2))
      )
    }
  }

  def contextLoaderRow[F[_] : Monad : RegistryLookup : Clock](igluClient: Resolver[F], processor: Processor)(
    projectId: String, event: Event, contextJson: SelfDescribingData[Json], payload: Payload.LoaderPayload
  ): F[Either[BadRow, LoaderRow]] = {
    val atomic = transformAtomic(event)
    val selfDescribingContext = Some(contextJson)
      .map {

        case SelfDescribingData(schema, data) =>
          transformJson[F](igluClient, schema)(data).map(_.map { row =>
            List(("params", Adapter.adaptRow(row)))
          })
      }
      .getOrElse(Monad[F].pure(List.empty[(String, AnyRef)].validNel[FailureDetails.LoaderIgluError]))

    val transformedJson = selfDescribingContext.map { c =>
      c
        .leftMap(details => Failure.LoaderIgluErrors(details))
        .leftMap(failure => BadRow.LoaderIgluError(processor, failure, payload))
        .toEither
    }

    transformedJson.map { badRowsOrJsons =>
      for {
        jsons <- badRowsOrJsons
        atomic <- atomic.leftMap(failure => BadRow.LoaderRuntimeError(processor, failure, payload))
        timestamp = new Instant(event.collector_tstamp.toEpochMilli)
        dataset = if (event.backfill_mode.getOrElse(false)) "backfill" else "load"
        tableName = s"${projectId}.${event.app_id.get}_$dataset.ctx_${contextJson.schema.name}"
      } yield LoaderRow(
        collectorTstamp = timestamp,
        tableName = tableName,
        data = (atomic ++ jsons).foldLeft(new TableRow())((r, kv) => r.set(kv._1, kv._2))
      )
    }
  }

  /** Parse JSON object provided by Snowplow Analytics SDK */
  def fromEvent[F[_]: Monad: RegistryLookup: Clock](igluClient: Resolver[F], processor: Processor)(
    projectId: String,
    event: Event
  ): List[F[Either[BadRow, LoaderRow]]] = {
    val payload: Payload.LoaderPayload = Payload.LoaderPayload(event)

    val contextLoaderRows = event.contexts.data
      .filter(_.schema.name == "event_context")
      .map(contextLoaderRow[F](igluClient, processor)(projectId, event, _, payload))
    val eventRow = eventLoaderRow[F](igluClient, processor)(projectId, event, payload)
    List(eventRow) ++ contextLoaderRows
  }

  /** Extract all set canonical properties in BQ-compatible format */
  def transformAtomic(event: Event): Either[String, List[(String, Any)]] = {
    val aggregated =
      event.atomic
        .filter { case (key, value) => !value.isNull && key != "api_key" }
        .toList.traverse[ValidatedNel[String, *], (String, Any)] {
        case (key, value) =>
          value
            .fold(
              s"null in $key".invalidNel,
              b => b.validNel,
              i => i.toInt.orElse(i.toBigDecimal).getOrElse(i.toDouble).validNel,
              s => s.validNel,
              _ => s"array ${value.noSpaces} in $key".invalidNel,
              _ => s"object ${value.noSpaces} in $key".invalidNel
            )
            .map { v =>
              (key, v)
            }
      }

    aggregated
      .map((LoadTstampField.name, "AUTO") :: _)
      .leftMap(errors => s"Unexpected types in transformed event: ${errors.mkString_(",")}")
      .toEither
  }

  /**
    * Get BigQuery-compatible table rows from data-only JSON payload
    * Can be transformed to contexts (via Repeated) later only remain ue-compatible
    */
  def transformJson[F[_]: Monad: RegistryLookup: Clock](igluClient: Resolver[F], schemaKey: SchemaKey)(
    data: Json
  ): F[ValidatedNel[FailureDetails.LoaderIgluError, Row]] =
    igluClient
      .lookupSchema(schemaKey)
      .map(
        _.leftMap { e =>
          NonEmptyList.one(FailureDetails.LoaderIgluError.IgluError(schemaKey, e))
        }.flatMap(schema => DdlSchema.parse(schema).toRight(invalidSchema(schemaKey)))
          .map(schema => Field.build("", schema, false))
          .flatMap(field => Row.cast(field)(data).leftMap(e => e.map(castError(schemaKey))).toEither)
          .toValidated
      )

  def castError(schemaKey: SchemaKey)(error: CastError): FailureDetails.LoaderIgluError =
    error match {
      case CastError.MissingInValue(key, value) =>
        FailureDetails.LoaderIgluError.MissingInValue(schemaKey, key, value)
      case CastError.NotAnArray(value, expected) =>
        FailureDetails.LoaderIgluError.NotAnArray(schemaKey, value, expected.asJson.noSpaces)
      case CastError.WrongType(value, expected) =>
        FailureDetails.LoaderIgluError.WrongType(schemaKey, value, expected.asJson.noSpaces)
    }

  implicit val bqModeEncoder: Encoder[Mode] = deriveEncoder[Mode]

  implicit val bqTypeEncoder: Encoder[Type] = deriveEncoder[Type]

  implicit val bqFieldEncoder: Encoder[Field] = deriveEncoder[Field]

  private def invalidSchema(schemaKey: SchemaKey): NonEmptyList[FailureDetails.LoaderIgluError] = {
    val error = FailureDetails.LoaderIgluError.InvalidSchema(schemaKey, "Cannot be parsed as JSON Schema AST")
    NonEmptyList.one(error)
  }
}
