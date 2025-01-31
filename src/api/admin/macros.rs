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
            impl RequestHandler for AdminApiRequest {
                type Response = AdminApiResponse;

                async fn handle(self, garage: &Arc<Garage>, admin: &Admin) -> Result<AdminApiResponse, Error> {
                    Ok(match self {
                        $(
                            AdminApiRequest::$special_endpoint(_) => panic!(
                                concat!(stringify!($special_endpoint), " needs to go through a special handler")
                            ),
                        )*
                        $(
                            AdminApiRequest::$endpoint(req) => AdminApiResponse::$endpoint(req.handle(garage, admin).await?),
                        )*
                    })
                }
            }
        }
    };
}

macro_rules! local_admin_endpoints {
    [
        $($endpoint:ident,)*
    ] => {
        paste! {
            #[derive(Debug, Clone, Serialize, Deserialize)]
            pub enum LocalAdminApiRequest {
                $(
                    $endpoint( [<Local $endpoint Request>] ),
                )*
            }

            #[derive(Debug, Clone, Serialize, Deserialize)]
            pub enum LocalAdminApiResponse {
                $(
                    $endpoint( [<Local $endpoint Response>] ),
                )*
            }

            $(
                pub type [< $endpoint Request >] = MultiRequest< [< Local $endpoint Request >] >;

                pub type [< $endpoint RequestBody >] = [< Local $endpoint Request >];

                pub type [< $endpoint Response >] = MultiResponse< [< Local $endpoint Response >] >;

                impl From< [< Local $endpoint Request >] > for LocalAdminApiRequest {
                    fn from(req: [< Local $endpoint Request >]) -> LocalAdminApiRequest {
                        LocalAdminApiRequest::$endpoint(req)
                    }
                }

                impl TryFrom<LocalAdminApiResponse> for [< Local $endpoint Response >] {
                    type Error = LocalAdminApiResponse;
                    fn try_from(resp: LocalAdminApiResponse) -> Result< [< Local $endpoint Response >], LocalAdminApiResponse> {
                        match resp {
                            LocalAdminApiResponse::$endpoint(v) => Ok(v),
                            x => Err(x),
                        }
                    }
                }

                #[async_trait]
                impl RequestHandler for [< $endpoint Request >] {
                    type Response = [< $endpoint Response >];

	                async fn handle(self, garage: &Arc<Garage>, admin: &Admin) -> Result<Self::Response, Error> {
				        let to = match self.node.as_str() {
                            "*" => garage.system.cluster_layout().all_nodes().to_vec(),
                            id => {
                                let nodes = garage.system.cluster_layout().all_nodes()
                                    .iter()
                                    .filter(|x| hex::encode(x).starts_with(id))
                                    .cloned()
                                    .collect::<Vec<_>>();
                                if nodes.len() != 1 {
                                    return Err(Error::bad_request(format!("Zero or multiple nodes matching {}: {:?}", id, nodes)));
                                }
                                nodes
                            }
                        };

                        let resps = garage.system.rpc_helper().call_many(&admin.endpoint,
                            &to,
                            AdminRpc::Internal(self.body.into()),
                            RequestStrategy::with_priority(PRIO_NORMAL),
                        ).await?;

                        let mut ret = [< $endpoint Response >] {
                            success: HashMap::new(),
                            error: HashMap::new(),
                        };
                        for (node, resp) in resps {
                            match resp {
                                Ok(AdminRpcResponse::InternalApiOkResponse(r)) => {
                                    match [< Local $endpoint Response >]::try_from(r) {
                                        Ok(r) => {
                                            ret.success.insert(hex::encode(node), r);
                                        }
                                        Err(_) => {
                                            ret.error.insert(hex::encode(node), "returned invalid value".to_string());
                                        }
                                    }
                                }
                                Ok(AdminRpcResponse::ApiErrorResponse{error_code, http_code, message}) => {
                                    ret.error.insert(hex::encode(node), format!("{} ({}): {}", error_code, http_code, message));
                                }
                                Ok(_) => {
                                    ret.error.insert(hex::encode(node), "returned invalid value".to_string());
                                }
                                Err(e) => {
                                    ret.error.insert(hex::encode(node), e.to_string());
                                }
                            }
                        }

                        Ok(ret)
                    }
                }
            )*

            impl LocalAdminApiRequest {
                pub fn name(&self) -> &'static str {
                    match self {
                        $(
                            Self::$endpoint(_) => stringify!($endpoint),
                        )*
                    }
                }
            }

            #[async_trait]
            impl RequestHandler for LocalAdminApiRequest {
                type Response = LocalAdminApiResponse;

                async fn handle(self, garage: &Arc<Garage>, admin: &Admin) -> Result<LocalAdminApiResponse, Error> {
                    Ok(match self {
                        $(
                            LocalAdminApiRequest::$endpoint(req) => LocalAdminApiResponse::$endpoint(req.handle(garage, admin).await?),
                        )*
                    })
                }
            }
        }
    };
}

pub(crate) use admin_endpoints;
pub(crate) use local_admin_endpoints;
