use reqwest::{Client, Response};
use serde::Deserialize;

use crate::{prelude::*, BaseUrl, Error};

#[derive(Deserialize, Debug)]
struct ErrorData {
    data: String,
    code: u16,
    msg: String,
}

#[derive(Debug)]
pub struct HttpClient {
    pub client: Client,
    pub base_url: String,
    pub base_url_config: BaseUrl,
}

async fn parse_response(response: Response) -> Result<String> {
    let status_code = response.status().as_u16();
    let text = response
        .text()
        .await
        .map_err(|e| Error::GenericRequest(e.to_string()))?;

    if status_code < 400 {
        return Ok(text);
    }
    let error_data = serde_json::from_str::<ErrorData>(&text);
    if (400..500).contains(&status_code) {
        let client_error = match error_data {
            Ok(error_data) => Error::ClientRequest {
                status_code,
                error_code: Some(error_data.code),
                error_message: error_data.msg,
                error_data: Some(error_data.data),
            },
            Err(err) => Error::ClientRequest {
                status_code,
                error_message: text,
                error_code: None,
                error_data: Some(err.to_string()),
            },
        };
        return Err(client_error);
    }

    Err(Error::ServerRequest {
        status_code,
        error_message: text,
    })
}

impl HttpClient {
    pub async fn post(&self, url_path: &'static str, data: String) -> Result<String> {
        let full_url = if let Some(query_pos) = self.base_url.find('?') {
            // Split URL and query parameters, insert path before query
            let base_part = &self.base_url[..query_pos];
            let query_part = &self.base_url[query_pos..];
            format!("{}{}{}", base_part, url_path, query_part)
        } else {
            format!("{}{}", self.base_url, url_path)
        };

        // Don't build() the request separately - send it directly to preserve default headers
        let result = self
            .client
            .post(full_url)
            .header("Content-Type", "application/json")
            .body(data)
            .send()
            .await
            .map_err(|e| Error::GenericRequest(e.to_string()))?;
        parse_response(result).await
    }

    pub fn is_mainnet(&self) -> bool {
        self.base_url_config.is_mainnet()
    }
}
