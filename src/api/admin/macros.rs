macro_rules! admin_endpoints {
    [
        $(@special $special_endpoint:ident,)*
        $($endpoint:ident,)*
    ] => {
        paste! {
            pub enum AdminApiRequest {
                $(
                    $special_endpoint( [<$special_endpoint Request>] ),
                )*
                $(
                    $endpoint( [<$endpoint Request>] ),
                )*
            }

            #[derive(Serialize)]
            #[serde(untagged)]
            pub enum AdminApiResponse {
                $(
                    $endpoint( [<$endpoint Response>] ),
                )*
            }

            impl AdminApiRequest {
                pub fn name(&self) -> &'static str {
                    match self {
                        $(
                            Self::$special_endpoint(_) => stringify!($special_endpoint),
                        )*
                        $(
                            Self::$endpoint(_) => stringify!($endpoint),
                        )*
                    }
                }
            }

            #[async_trait]
            impl EndpointHandler for AdminApiRequest {
                type Response = AdminApiResponse;

                async fn handle(self, garage: &Arc<Garage>) -> Result<AdminApiResponse, Error> {
                    Ok(match self {
                        $(
                            AdminApiRequest::$special_endpoint(_) => panic!(
                                concat!(stringify!($special_endpoint), " needs to go through a special handler")
                            ),
                        )*
                        $(
                            AdminApiRequest::$endpoint(req) => AdminApiResponse::$endpoint(req.handle(garage).await?),
                        )*
                    })
                }
            }
        }
    };
}

pub(crate) use admin_endpoints;
