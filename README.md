# GameTuner BigQuery Loader

## Overview

GameTuner BigQuery Loader is a part of the GameTuner project. It is responsible for loading data from the PubSub topic with enriched events into BigQuery. It is written in Scala and uses the [Google Cloud Dataflow][dataflow] to load data into BigQuery. This project is a fork of the [Snowplow BigQuery Loader project][snowplow-bigquery-loader]. 

## Requirements

For building and running the application you need:

- sbt >= 2.6
- scala = 2.12.10
- jdk = 11

## Installation

Since application should be run in the Google Cloud Dataflow environment, it is required to build a flex template. The flex template is a container image that contains the application code and dependencies. It is used to run the application in the Google Cloud Dataflow environment. BigQuery Loader is deployed using [GameTuner Terraform module][gametuner-terraform].

Image is submiteed using the following command:

```bash
gcloud builds submit --config=cloudbuild.yaml .
```

## Licence

This project is fork of [Snowplow BigQuery Loader version 1.4.3][snowplow-bigquery-loader-1.4.3], that is licenced under Apache 2.0 Licence.

The GameTuner BigQuery Loader is copyright 2022-2024 AlgebraAI.

GameTuner BigQuery Loader is released under the [Apache 2.0 License][license].


[dataflow]:https://cloud.google.com/dataflow
[snowplow-bigquery-loader]:https://github.com/snowplow-incubator/snowplow-bigquery-loader
[gametuner-terraform]:https://github.com/GameTuner/gametuner-terraform-gcp.git
[snowplow-bigquery-loader-1.4.3]:https://github.com/snowplow-incubator/snowplow-bigquery-loader/releases/tag/1.4.3