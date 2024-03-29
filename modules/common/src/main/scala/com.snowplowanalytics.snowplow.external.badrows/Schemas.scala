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
package com.snowplowanalytics.snowplow.external.badrows

import com.snowplowanalytics.iglu.core.{SchemaVer, SchemaKey}

object Schemas {
  /** @see [[BadRow.SizeViolation]] */
  val SizeViolation = SchemaKey("com.snowplowanalytics.snowplow.external.badrows", "size_violation", "jsonschema", SchemaVer.Full(1, 0, 0))

  /** @see [[BadRow.CPFormatViolation]] */
  val CPFormatViolation = SchemaKey("com.snowplowanalytics.snowplow.external.badrows", "collector_payload_format_violation", "jsonschema", SchemaVer.Full(1, 0, 0))

  /** @see [[BadRow.AdapterFailures]] */
  val AdapterFailures = SchemaKey("com.snowplowanalytics.snowplow.external.badrows", "adapter_failures", "jsonschema", SchemaVer.Full(1, 0, 0))

  /** @see [[BadRow.TrackerProtocolViolations]] */
  val TrackerProtocolViolations = SchemaKey("com.snowplowanalytics.snowplow.external.badrows", "tracker_protocol_violations", "jsonschema", SchemaVer.Full(1, 0, 0))

  /** @see [[BadRow.SchemaViolations]] */
  val SchemaViolations = SchemaKey("com.snowplowanalytics.snowplow.external.badrows", "schema_violations", "jsonschema", SchemaVer.Full(2, 0, 0))

  /** @see [[BadRow.EnrichmentFailures]] */
  val EnrichmentFailures = SchemaKey("com.snowplowanalytics.snowplow.external.badrows", "enrichment_failures", "jsonschema", SchemaVer.Full(2, 0, 0))

  /** @see [[BadRow.LoaderParsingError]] */
  val LoaderParsingError = SchemaKey("com.snowplowanalytics.snowplow.external.badrows", "loader_parsing_error", "jsonschema", SchemaVer.Full(2, 0, 0))

  /** @see [[BadRow.LoaderIgluError]] */
  val LoaderIgluError = SchemaKey("com.snowplowanalytics.snowplow.external.badrows", "loader_iglu_error", "jsonschema", SchemaVer.Full(2, 0, 0))

  /** @see [[BadRow.LoaderRuntimeError]] */
  val LoaderRuntimeError = SchemaKey("com.snowplowanalytics.snowplow.external.badrows", "loader_runtime_error", "jsonschema", SchemaVer.Full(1, 0, 1))

  /** @see [[BadRow.LoaderRecoveryError]] */
  val LoaderRecoveryError = SchemaKey("com.snowplowanalytics.snowplow.external.badrows", "loader_recovery_error", "jsonschema", SchemaVer.Full(1, 0, 0))

  /** @see [[BadRow.RecoveryError]] */
  val RecoveryError = SchemaKey("com.snowplowanalytics.snowplow.external.badrows", "recovery_error", "jsonschema", SchemaVer.Full(1, 0, 0))

  /** @see [[BadRow.GenericError]] */
  val GenericError = SchemaKey("com.snowplowanalytics.snowplow.external.badrows", "generic_error", "jsonschema", SchemaVer.Full(1, 0, 0))
}
