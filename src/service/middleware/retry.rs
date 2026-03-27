use futures_util::{future, FutureExt};
use http::header::AsHeaderName;
use http::{HeaderMap, HeaderValue, Request, Response};
use hyper_util::client::legacy::Error;
use std::time::Duration;
use tower::retry::Policy;

use crate::body::OctoBody;

fn header_as_u64(headers: &HeaderMap<HeaderValue>, header: impl AsHeaderName) -> Option<u64> {
    headers.get(header)?.to_str().ok()?.parse().ok()
}
fn header_as_i64(headers: &HeaderMap<HeaderValue>, header: impl AsHeaderName) -> Option<i64> {
    headers.get(header)?.to_str().ok()?.parse().ok()
}

#[derive(Clone)]
pub enum RetryConfig {
    None,
    Simple(usize),
    /// Retry [`self.0`] times if the status is a 5XX or if the status code is in the list of statuses
    SimpleWithStatuses(usize, &'static [u16]),
    /// Handle github's retry headers, up to [`self.0`] times.
    /// Per the rate limit documentation here: https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api?apiVersion=2022-11-28
    /// If we get a 429 and none of the headers are present, wait 60 seconds.
    /// If we get a 403, and neither of those headers are present, do not retry.
    /// It's not clear whether it's actually forbidden, or if it's a rate limit.
    /// For server errors (5xx), retry immediately
    /// For any other errors do not retry.
    HandleRateLimits(usize),
}

impl<B> Policy<Request<OctoBody>, Response<B>, Error> for RetryConfig {
    type Future = future::BoxFuture<'static, ()>;

    fn retry(
        &mut self,
        _req: &mut Request<OctoBody>,
        result: &mut Result<Response<B>, Error>,
    ) -> Option<Self::Future> {
        match self {
            RetryConfig::None => None,
            RetryConfig::Simple(count) => match result {
                Ok(response) => {
                    if response.status().is_server_error() || response.status() == 429 {
                        if *count > 0 {
                            *count -= 1;
                            Some(future::ready(()).boxed())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                Err(_) => {
                    if *count > 0 {
                        *count -= 1;
                        Some(future::ready(()).boxed())
                    } else {
                        None
                    }
                }
            },
            RetryConfig::SimpleWithStatuses(count, statuses) => match result {
                Ok(response) => {
                    if response.status().is_server_error()
                        || statuses.contains(&response.status().as_u16())
                    {
                        if *count > 0 {
                            *count -= 1;
                            Some(future::ready(()).boxed())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                Err(_) => {
                    if *count > 0 {
                        *count -= 1;
                        Some(future::ready(()).boxed())
                    } else {
                        None
                    }
                }
            },
            RetryConfig::HandleRateLimits(max_retries) => {
                if *max_retries > 0 {
                    let response = result.as_ref().ok()?;

                    if matches!(
                        response.status(),
                        http::StatusCode::TOO_MANY_REQUESTS | http::StatusCode::FORBIDDEN
                    ) {
                        let headers = response.headers();
                        let wait_secs = match (
                            header_as_u64(headers, "retry-after"),
                            header_as_u64(headers, "x-ratelimit-remaining"),
                            header_as_i64(headers, "x-ratelimit-reset"),
                        ) {
                            (Some(secs), _, _) => Some(secs),
                            (None, Some(remaining), Some(reset_ts)) if remaining == 0 => {
                                Some(std::cmp::max(5, reset_ts - chrono::Utc::now().timestamp())
                                    as u64)
                            }
                            (None, _, _)
                                if response.status() == http::StatusCode::TOO_MANY_REQUESTS =>
                            {
                                Some(60)
                            }
                            _ => None,
                        }?;

                        *max_retries -= 1;
                        Some(
                            tokio::time::sleep(Duration::from_secs(wait_secs))
                                .then(move |_| {
                                    future::ready(())
                                })
                                .boxed(),
                        )
                    } else if response.status().is_server_error() {
                        *max_retries -= 1;
                        Some(future::ready(()).boxed())
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }
    }

    fn clone_request(&mut self, req: &Request<OctoBody>) -> Option<Request<OctoBody>> {
        match self {
            RetryConfig::None => None,
            _ => {
                // This returns none if the body is empty. Just return an empty body
                // instead so that we retry GET requests.
                let body = match req.body().try_clone() {
                    Some(b) => b,
                    None => OctoBody::empty()
                };

                // `Request` can't be cloned
                let mut new_req = Request::builder()
                    .uri(req.uri())
                    .method(req.method())
                    .version(req.version());
                for (name, value) in req.headers() {
                    new_req = new_req.header(name, value);
                }

                let new_req = new_req.body(body).expect(
                    "This should never panic, as we are cloning a components from existing request",
                );
                Some(new_req)
            }
        }
    }
}
