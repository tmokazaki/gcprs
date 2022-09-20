use crate::auth;
use google_sheets4 as sheets4;
use hyper;
use hyper_rustls;
use sheets4::api::ValueRange;
use sheets4::Sheets;

use anyhow;
use anyhow::Result;

pub struct SpreadSheet {
    api: Sheets<hyper_rustls::HttpsConnector<hyper::client::connect::HttpConnector>>,
}

#[derive(Clone, Debug)]
pub struct ValuesGetParam {
    _sheet_id: String,
    _sheet_name: String,
    _range_notation: Option<String>,
    _value_render_option: Option<ValueRenderOption>,
    _major_dimention: Option<String>,
    _date_time_render_option: Option<String>,
}

impl ValuesGetParam {
    pub fn new(sheet_id: String, sheet_name: String) -> Self {
        ValuesGetParam {
            _sheet_id: sheet_id,
            _sheet_name: sheet_name,
            _range_notation: Default::default(),
            _value_render_option: Default::default(),
            _major_dimention: Default::default(),
            _date_time_render_option: Default::default(),
        }
    }

    pub fn range_notaion(&mut self, p: &str) -> &mut Self {
        self._range_notation = Some(p.to_string());
        self
    }
}

#[derive(Clone, Debug)]
pub enum ValueRenderOption {
    FormattedValue,
    UnformattedValue,
    Formula,
}

impl ValueRenderOption {
    fn as_str(&self) -> &'static str {
        match self {
            ValueRenderOption::FormattedValue => "FORMATTED_VALUE",
            ValueRenderOption::UnformattedValue => "UNFORMATTED_VALUE",
            ValueRenderOption::Formula => "FORMULA",
        }
    }
}

impl SpreadSheet {
    pub fn new(auth: auth::GcpAuth) -> Result<SpreadSheet> {
        let client = hyper::Client::builder().build(
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()
                .https_only()
                .enable_http1()
                .enable_http2()
                .build(),
        );
        let hub = Sheets::new(client, auth.authenticator());
        Ok(SpreadSheet { api: hub })
    }

    pub async fn get_values(&self, p: &ValuesGetParam) -> Result<ValueRange> {
        // https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/get
        let mut s = self
            .api
            .spreadsheets()
            .values_get(&p._sheet_id, &p._sheet_name);
        if let Some(ro) = &p._value_render_option {
            s = s.value_render_option(ro.as_str());
        }
        //.major_dimension("sed")
        //.date_time_render_option("duo")
        let result = s.doit().await?;

        Ok(result.1)
    }
}
