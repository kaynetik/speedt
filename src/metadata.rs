use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::config;

/// Connection metadata gathered before/after a session.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Metadata {
    pub client_ip: Option<String>,
    pub country: Option<String>,
    pub region: Option<String>,
    pub city: Option<String>,
    pub postal_code: Option<String>,
    pub latitude: Option<String>,
    pub longitude: Option<String>,
    pub asn: Option<u32>,
    pub as_organization: Option<String>,
    pub colo: Option<String>,
    pub http_protocol: Option<String>,
    pub tls: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MetaJson {
    #[serde(rename = "asOrganization")]
    as_organization: Option<String>,
    asn: Option<u32>,
    #[serde(default)]
    colo: ColoField,
    country: Option<String>,
    city: Option<String>,
    region: Option<String>,
    #[serde(rename = "postalCode")]
    postal_code: Option<String>,
    latitude: Option<String>,
    longitude: Option<String>,
    #[serde(rename = "clientIp")]
    client_ip: Option<String>,
    #[serde(rename = "httpProtocol")]
    http_protocol: Option<String>,
}

/// Cloudflare's `/meta` returns `colo` either as a bare IATA string or as a
/// nested object `{ "iata": "BEG", "city": "Belgrade", ... }` depending on
/// the call site. Accept both.
#[derive(Debug, Default, Deserialize)]
#[serde(untagged)]
enum ColoField {
    Object {
        iata: Option<String>,
    },
    Code(String),
    #[default]
    None,
}

impl ColoField {
    fn into_iata(self) -> Option<String> {
        match self {
            Self::Object { iata } => iata,
            Self::Code(s) => Some(s),
            Self::None => None,
        }
    }
}

pub async fn fetch(client: &reqwest::Client) -> Result<Metadata> {
    let mut md = Metadata::default();

    if let Ok(resp) = client
        .get(config::meta_url())
        .header("accept", "application/json")
        .header("origin", config::HOST)
        .header("referer", format!("{}/", config::HOST))
        .send()
        .await
    {
        md.http_protocol = Some(format!("{:?}", resp.version()));
        if let Ok(meta) = resp.json::<MetaJson>().await {
            md.as_organization = meta.as_organization;
            md.asn = meta.asn;
            md.colo = meta.colo.into_iata();
            md.country = meta.country;
            md.city = meta.city;
            md.region = meta.region;
            md.postal_code = meta.postal_code;
            md.latitude = meta.latitude;
            md.longitude = meta.longitude;
            md.client_ip = meta.client_ip;
            if md.http_protocol.is_none() {
                md.http_protocol = meta.http_protocol;
            }
        }
    }

    let trace = client
        .get(config::trace_url())
        .send()
        .await
        .context("trace request failed")?
        .text()
        .await
        .context("trace body read failed")?;
    parse_trace_into(&trace, &mut md);

    Ok(md)
}

fn parse_trace_into(trace: &str, md: &mut Metadata) {
    for line in trace.lines() {
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        match k {
            "ip" => md.client_ip.get_or_insert_with(|| v.to_string()),
            "loc" => md.country.get_or_insert_with(|| v.to_string()),
            "colo" => md.colo.get_or_insert_with(|| v.to_string()),
            "tls" => md.tls.get_or_insert_with(|| v.to_string()),
            "http" => md.http_protocol.get_or_insert_with(|| v.to_string()),
            _ => continue,
        };
    }
}
