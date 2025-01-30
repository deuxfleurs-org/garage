macro_rules! admin_endpoints {
    [
        $(@special $special_endpoint:ident,)*
        $($endpoint:ident,)*
    ] => {
        paste! {
            #[derive(Debug, Clone, Serialize, Deserialize)]
            pub enum AdminApiRequest {
                $(
                    $special_endpoint( [<$special_endpoint Request>] ),
                )*
                $(
                    $endpoint( [<$endpoint Request>] ),
                )*
            }

            #[derive(Debug, Clone, Serialize)]
            #[serde(untagged)]
            pub enum AdminApiResponse {
                $(
                    $endpoint( [<$endpoint Response>] ),
                )*
            }

            #[derive(Debug, Clone, Serialize, Deserialize)]
            pub enum TaggedAdminApiResponse {
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

            impl AdminApiResponse {
                pub fn tagged(self) -> TaggedAdminApiResponse {
                    match self {
                        $(
                            Self::$endpoint(res) => TaggedAdminApiResponse::$endpoint(res),
                        )*
                    }
                }
            }

            $(
                impl From< [< $endpoint Request >] > for AdminApiRequest {
                    fn from(req: [< $endpoint Request >]) -> AdminApiRequest {
                        AdminApiRequest::$endpoint(req)
                    }
                }

                impl TryFrom<TaggedAdminApiResponse> for [< $endpoint Response >] {
                    type Error = TaggedAdminApiResponse;
                    fn try_from(resp: TaggedAdminApiResponse) -> Result< [< $endpoint Response >], TaggedAdminApiResponse> {
                        match resp {
                            TaggedAdminApiResponse::$endpoint(v) => Ok(v),
                            x => Err(x),
                        }
                    }
                }
            )*

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
