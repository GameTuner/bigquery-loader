package com.snowplowanalytics.snowplow.external.analytics.scalasdk

package object validate {

  val FIELD_SIZES: Map[String, Int] = Map(
    "api_key" -> 255,
    "app_id" -> 255,
    "platform" -> 255,
    "event" -> 128,
    "event_id" -> 36,
    "name_tracker" -> 128,
    "v_tracker" -> 100,
    "v_collector" -> 100,
    "v_etl" -> 1000,
    "user_id" -> 255,
    "installation_id" -> 255,
    "unique_id" -> 255,
    "user_ipaddress" -> 128,
    "user_fingerprint" -> 128,
    "domain_userid" -> 128,
    "network_userid" -> 128,
    "geo_country" -> 2,
    "geo_country_name" -> 75,
    "geo_region" -> 3,
    "geo_city" -> 75,
    "geo_zipcode" -> 15,
    "geo_region_name" -> 100,
    "ip_isp" -> 100,
    "ip_organization" -> 128,
    "ip_domain" -> 128,
    "ip_netspeed" -> 100,
    "page_url" -> 4096,
    "page_title" -> 2000,
    "page_referrer" -> 4096,
    "page_urlscheme" -> 16,
    "page_urlhost" -> 255,
    "page_urlpath" -> 3000,
    "page_urlquery" -> 6000,
    "page_urlfragment" -> 3000,
    "refr_urlscheme" -> 16,
    "refr_urlhost" -> 255,
    "refr_urlpath" -> 6000,
    "refr_urlquery" -> 6000,
    "refr_urlfragment" -> 3000,
    "refr_medium" -> 25,
    "refr_source" -> 50,
    "refr_term" -> 255,
    "mkt_medium" -> 255,
    "mkt_source" -> 255,
    "mkt_term" -> 255,
    "mkt_content" -> 500,
    "mkt_campaign" -> 255,
    "se_category" -> 1000,
    "se_action" -> 1000,
    "se_label" -> 4096,
    "se_property" -> 1000,
    "tr_orderid" -> 255,
    "tr_affiliation" -> 255,
    "tr_city" -> 255,
    "tr_state" -> 255,
    "tr_country" -> 255,
    "ti_orderid" -> 255,
    "ti_sku" -> 255,
    "ti_name" -> 255,
    "ti_category" -> 255,
    "useragent" -> 1000,
    "br_name" -> 50,
    "br_family" -> 50,
    "br_version" -> 50,
    "br_type" -> 50,
    "br_renderengine" -> 50,
    "br_lang" -> 255,
    "br_colordepth" -> 12,
    "os_name" -> 50,
    "os_family" -> 50,
    "os_manufacturer" -> 50,
    "os_timezone" -> 255,
    "dvce_type" -> 50,
    "doc_charset" -> 128,
    "tr_currency" -> 3,
    "ti_currency" -> 3,
    "base_currency" -> 3,
    "geo_timezone" -> 64,
    "mkt_clickid" -> 128,
    "mkt_network" -> 64,
    "etl_tags" -> 500,
    "refr_domain_userid" -> 128,
    "domain_sessionid" -> 128,
    "event_vendor" -> 1000,
    "event_name" -> 1000,
    "event_format" -> 128,
    "event_version" -> 128,
    "event_fingerprint" -> 128,
    "sandbox_mode" -> 5,
    "backfill_mode" -> 5,
  )

  private def validateStr(
    k: String,
    value: String
  ): List[String] =
    if (value.length > FIELD_SIZES.getOrElse(k, Int.MaxValue))
      List(s"Field $k longer than maximum allowed size ${FIELD_SIZES.getOrElse(k, Int.MaxValue)}")
    else
      List.empty[String]

  private def validateStr(
    k: String,
    v: Option[String]
  ): List[String] =
    v match {
      case Some(value) => validateStr(k, value)
      case None => List.empty[String]
    }

  def validator(e: Event): List[String] =
      validateStr("api_key", e.api_key) ++
      validateStr("app_id", e.app_id) ++
      validateStr("platform", e.platform) ++
      validateStr("event", e.event) ++
      validateStr("name_tracker", e.name_tracker) ++
      validateStr("v_tracker", e.v_tracker) ++
      validateStr("v_collector", e.v_collector) ++
      validateStr("v_etl", e.v_etl) ++
      validateStr("user_id", e.user_id) ++
      validateStr("installation_id", e.installation_id) ++
      validateStr("unique_id", e.unique_id) ++
      validateStr("user_ipaddress", e.user_ipaddress) ++
      validateStr("network_userid", e.network_userid) ++
      validateStr("geo_country", e.geo_country) ++
      validateStr("geo_country_name", e.geo_country_name) ++
      validateStr("geo_region", e.geo_region) ++
      validateStr("geo_city", e.geo_city) ++
      validateStr("geo_zipcode", e.geo_zipcode) ++
      validateStr("geo_region_name", e.geo_region_name) ++
      validateStr("mkt_medium", e.mkt_medium) ++
      validateStr("mkt_source", e.mkt_source) ++
      validateStr("mkt_term", e.mkt_term) ++
      validateStr("mkt_content", e.mkt_content) ++
      validateStr("mkt_campaign", e.mkt_campaign) ++
      validateStr("useragent", e.useragent) ++
      validateStr("geo_timezone", e.geo_timezone) ++
      validateStr("etl_tags", e.etl_tags) ++
      validateStr("event_vendor", e.event_vendor) ++
      validateStr("event_name", e.event_name) ++
      validateStr("event_format", e.event_format) ++
      validateStr("event_version", e.event_version) ++
      validateStr("event_fingerprint", e.event_fingerprint)

}
