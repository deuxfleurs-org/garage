/// This macro is used to generate very repetitive match {} blocks in this module
/// It is _not_ made to be used anywhere else
macro_rules! router_match {
    (@match $enum:expr , [ $($endpoint:ident,)* ]) => {{
        // usage: router_match {@match my_enum, [ VariantWithField1, VariantWithField2 ..] }
        // returns true if the variant was one of the listed variants, false otherwise.
        use Endpoint::*;
        match $enum {
            $(
            $endpoint { .. } => true,
            )*
            _ => false
        }
    }};
    (@extract $enum:expr , $param:ident, [ $($endpoint:ident,)* ]) => {{
        // usage: router_match {@extract my_enum, field_name, [ VariantWithField1, VariantWithField2 ..] }
        // returns Some(field_value), or None if the variant was not one of the listed variants.
        use Endpoint::*;
        match $enum {
            $(
            $endpoint {$param, ..} => Some($param),
            )*
            _ => None
        }
    }};
    (@gen_parser ($keyword:expr, $key:ident, $query:expr, $header:expr),
        key: [$($kw_k:ident $(if $required_k:ident)? $(header $header_k:expr)? => $api_k:ident $(($($conv_k:ident :: $param_k:ident),*))?,)*],
        no_key: [$($kw_nk:ident $(if $required_nk:ident)? $(if_header $header_nk:expr)? => $api_nk:ident $(($($conv_nk:ident :: $param_nk:ident),*))?,)*]) => {{
        // usage: router_match {@gen_parser (keyword, key, query, header),
        //   key: [
        //      SOME_KEYWORD => VariantWithKey,
        //      ...
        //   ],
        //   no_key: [
        //      SOME_KEYWORD => VariantWithoutKey,
        //      ...
        //   ]
        // }
        // See in from_{method} for more detailed usage.
        use Endpoint::*;
        use keywords::*;
        match ($keyword, !$key.is_empty()){
            $(
            ($kw_k, true) if true $(&& $query.$required_k.is_some())? $(&& $header.contains_key($header_k))? => Ok($api_k {
                $key,
                $($(
                    $param_k: router_match!(@@parse_param $query, $conv_k, $param_k),
                )*)?
            }),
            )*
            $(
            ($kw_nk, false) $(if $query.$required_nk.is_some())? $(if $header.contains($header_nk))? => Ok($api_nk {
                $($(
                    $param_nk: router_match!(@@parse_param $query, $conv_nk, $param_nk),
                )*)?
            }),
            )*
            (kw, _) => Err(Error::BadRequest(format!("Invalid endpoint: {}", kw)))
        }
    }};

    (@@parse_param $query:expr, query_opt, $param:ident) => {{
        // extract optional query parameter
		$query.$param.take().map(|param| param.into_owned())
    }};
    (@@parse_param $query:expr, query, $param:ident) => {{
        // extract mendatory query parameter
        $query.$param.take().ok_or_bad_request("Missing argument for endpoint")?.into_owned()
    }};
    (@@parse_param $query:expr, opt_parse, $param:ident) => {{
        // extract and parse optional query parameter
        // missing parameter is file, however parse error is reported as an error
		$query.$param
            .take()
            .map(|param| param.parse())
            .transpose()
            .map_err(|_| Error::BadRequest("Failed to parse query parameter".to_owned()))?
    }};
    (@@parse_param $query:expr, parse, $param:ident) => {{
        // extract and parse mandatory query parameter
        // both missing and un-parseable parameters are reported as errors
        $query.$param.take().ok_or_bad_request("Missing argument for endpoint")?
            .parse()
            .map_err(|_| Error::BadRequest("Failed to parse query parameter".to_owned()))?
    }};
    (@func
    $(#[$doc:meta])*
     pub enum Endpoint {
        $(
            $(#[$outer:meta])*
            $variant:ident $({
                $($name:ident: $ty:ty,)*
            })?,
        )*
    }) => {
    $(#[$doc])*
        pub enum Endpoint {
            $(
                $(#[$outer])*
                $variant $({
                    $($name: $ty, )*
                })?,
            )*
        }
        impl Endpoint {
            pub fn name(&self) -> &'static str {
                match self {
                    $(Endpoint::$variant $({ $($name: _,)* .. })? => stringify!($variant),)*
                }
            }
        }
    };
    (@if ($($cond:tt)+) then ($($then:tt)*) else ($($else:tt)*)) => {
        $($then)*
    };
    (@if () then ($($then:tt)*) else ($($else:tt)*)) => {
        $($else)*
    };
}

/// This macro is used to generate part of the code in this module. It must be called only one, and
/// is useless outside of this module.
macro_rules! generateQueryParameters {
    ( $($rest:expr => $name:ident),* ) => {
        /// Struct containing all query parameters used in endpoints. Think of it as an HashMap,
        /// but with keys statically known.
        #[derive(Debug, Default)]
        struct QueryParameters<'a> {
            keyword: Option<Cow<'a, str>>,
            $(
            $name: Option<Cow<'a, str>>,
            )*
        }

        impl<'a> QueryParameters<'a> {
            /// Build this struct from the query part of an URI.
            fn from_query(query: &'a str) -> Result<Self, Error> {
                let mut res: Self = Default::default();
                for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
                    let repeated = match k.as_ref() {
                        $(
                            $rest => if !v.is_empty() {
                                res.$name.replace(v).is_some()
                            } else {
                                false
                            },
                        )*
                        _ => {
                            if k.starts_with("response-") || k.starts_with("X-Amz-") {
                                false
                            } else if v.as_ref().is_empty() {
                                if res.keyword.replace(k).is_some() {
                                    return Err(Error::BadRequest("Multiple keywords".to_owned()));
                                }
                                continue;
                            } else {
                                debug!("Received an unknown query parameter: '{}'", k);
                                false
                            }
                        }
                    };
                    if repeated {
                        return Err(Error::BadRequest(format!(
                            "Query parameter repeated: '{}'",
                            k
                        )));
                    }
                }
                Ok(res)
            }

            /// Get an error message in case not all parameters where used when extracting them to
            /// build an Enpoint variant
            fn nonempty_message(&self) -> Option<&str> {
                if self.keyword.is_some() {
                    Some("Keyword not used")
                } $(
                    else if self.$name.is_some() {
                        Some(concat!("'", $rest, "'"))
                    }
                )* else {
                    None
                }
            }
        }
    }
}

pub(crate) use generateQueryParameters;
pub(crate) use router_match;
