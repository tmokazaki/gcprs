use std::error::Error;
use std::fmt;
use serde::{Deserialize, Serialize};

/// Google API response BadRequest
///
/// Err(Bad Request: {"error":{"code":404,"errors":[{"domain":"global","message":"The specified bucket does not exist.","reason":"notFound"}],"message":"The specified bucket does not exist."}}
///
#[derive(Debug, Deserialize, Serialize)]
pub enum RequestError {
    /// 404 notfound
    NotFound {
        code: u16,
        message: String
    },
    /// 403 forbidden
    Forbidden {
        code: u16,
        message: String
    },
    Undefined {
        code: u16,
        message: String,
    }
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RequestError::NotFound { code, message }
             | RequestError::Forbidden { code, message }
             | RequestError::Undefined { code, message } => write!(f, "code: {}, {}", code, message) ,
        }
    }
}

impl Error for RequestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}


const UNKNOWN_CODE: u16 = 500;
const UNKNOWN_MESSAGE: &str = "Unknown error";

impl BadRequest {

    fn code_message(&self) -> (u16, String) {
        self.error.as_ref()
            .map(|e| (e.code.unwrap_or(UNKNOWN_CODE), e.message.as_ref().map(|m| m.clone()).unwrap_or(String::from(UNKNOWN_MESSAGE))))
            .unwrap_or((UNKNOWN_CODE, String::from(UNKNOWN_MESSAGE)))
    }

    pub fn request_error(&self) -> RequestError {
        let (code, message) = self.code_message();
        match code {
            404 => RequestError::NotFound { code, message, },
            403 => RequestError::Forbidden { code, message, },
            _ => RequestError::Undefined { code, message, }
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BadRequest {
    pub error: Option<GoogleError>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GoogleError {
    pub code: Option<u16>,
    pub errors: Option<Vec<ErrorDetail>>,
    pub message: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ErrorDetail {
    pub domain: Option<String>,
    pub message: Option<String>,
    pub reason: Option<String>,
}
