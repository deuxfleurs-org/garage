use http::header::{CONTENT_TYPE, LOCATION};
use hyper::{body::Incoming as IncomingBody, Request, Response, StatusCode};
use rust_embed::Embed;

use garage_api_common::helpers::*;

use crate::api_server::ResBody;
use crate::error::*;

#[derive(Embed)]
#[folder = "$GARAGE_WEBADMIN_DIST"]
struct Asset;

const DEFAULT_FILE: &str = "index.html";

pub const ROOT_ASSETS: &[&str] = &["/favicon.ico", "/favicon.svg"];

pub fn is_webadmin_path(path: &str) -> bool {
	path == "/" || ROOT_ASSETS.contains(&path) || path.starts_with("/ui/")
}

pub async fn handle(req: Request<IncomingBody>) -> Result<Response<ResBody>, Error> {
	let path = req.uri().path();
	let path = match path.strip_prefix("/ui/") {
		Some(f) if Asset::get(f).is_some() => f,
		Some(_) => DEFAULT_FILE,
		None if ROOT_ASSETS.contains(&path) => path.strip_prefix("/").unwrap(),
		None if path == "/" => {
			return Ok(Response::builder()
				.status(StatusCode::FOUND)
				.header(LOCATION, "/ui/")
				.body(empty_body())?);
		}
		_ => unreachable!(),
	};

	let file = Asset::get(path).expect("No index.html in webadmin embedded static files");

	let mime = mime_guess::MimeGuess::from_path(path)
		.first_or_octet_stream()
		.to_string();

	Ok(Response::builder()
		.status(StatusCode::OK)
		.header(CONTENT_TYPE, &mime)
		.body(bytes_body(file.data.into_owned().into()))?)
}
