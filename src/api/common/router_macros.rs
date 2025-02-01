/// This macro is used to generate very repetitive match {} blocks in this module
/// It is _not_ made to be used anywhere else
#[macro_export]
macro_rules! router_match {
    (@match $enum:expr , [ $($endpoint:ident,)* ]) => {{
        // usage: router_match {@match my_enum, [ VariantWithField1, VariantWithField2 ..] }
        // returns true if the variant was one of the listed variants, false otherwise.
        match $enum {
            $(
            Endpoint::$endpoint { .. } => true,
            )*
            _ => false
        }
    }};
    (@extract $enum:expr , $param:ident, [ $($endpoint:ident,)* ]) => {{
        // usage: router_match {@extract my_enum, field_name, [ VariantWithField1, VariantWithField2 ..] }
        // returns Some(field_value), or None if the variant was not one of the listed variants.
        match $enum {
            $(
            Endpoint::$endpoint {$param, ..} => Some($param),
            )*
            _ => None
        }
    }};
    (@gen_path_parser ($method:expr, $reqpath:expr, $query:expr)
     [
     $($meth:ident $path:pat $(if $required:ident)? => $api:ident $(($($conv:ident :: $param:ident),*))?,)*
     ]) => {{
        {
            #[allow(unused_parens)]
            match ($method, $reqpath) {
                $(
                    (&Method::$meth, $path) if true $(&& $query.$required.is_some())? => Endpoint::$api {
                        $($(
                            $param: router_match!(@@parse_param $query, $conv, $param),
                        )*)?
                    },
                )*
                (m, p) => {
                    return Err(Error::bad_request(format!(
                        "Unknown API endpoint: {} {}",
                        m, p
                    )))
                }
            }
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
        match ($keyword, !$key.is_empty()){
            $(
            (Keyword::$kw_k, true) if true $(&& $query.$required_k.is_some())? $(&& $header.contains_key($header_k))? => Ok(Endpoint::$api_k {
                $key,
                $($(
                    $param_k: router_match!(@@parse_param $query, $conv_k, $param_k),
                )*)?
            }),
            )*
            $(
            (Keyword::$kw_nk, false) $(if $query.$required_nk.is_some())? $(if $header.contains($header_nk))? => Ok(Endpoint::$api_nk {
                $($(
                    $param_nk: router_match!(@@parse_param $query, $conv_nk, $param_nk),
                )*)?
            }),
            )*
            (kw, _) => Err(Error::bad_request(format!("Invalid endpoint: {}", kw)))
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
            .map_err(|_| Error::bad_request("Failed to parse query parameter"))?
    }};
    (@@parse_param $query:expr, parse, $param:ident) => {{
        // extract and parse mandatory query parameter
        // both missing and un-parseable parameters are reported as errors
        $query.$param.take().ok_or_bad_request("Missing argument for endpoint")?
            .parse()
            .map_err(|_| Error::bad_request("Failed to parse query parameter"))?
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
}

/// This macro is used to generate part of the code in this module. It must be called only one, and
/// is useless outside of this module.
#[macro_export]
macro_rules! generateQueryParameters {
    (
        keywords: [ $($kw_param:expr => $kw_name: ident),* ],
        fields: [ $($f_param:expr => $f_name:ident),* ]
    ) => {
        #[derive(Debug)]
        #[allow(non_camel_case_types)]
        #[allow(clippy::upper_case_acronyms)]
        enum Keyword {
            EMPTY,
            $( $kw_name, )*
        }

        impl std::fmt::Display for Keyword {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                match self {
                    Keyword::EMPTY => write!(f, "``"),
                    $( Keyword::$kw_name => write!(f, "`{}`", $kw_param), )*
                }
            }
        }

        impl Default for Keyword {
            fn default() -> Self {
                Keyword::EMPTY
            }
        }

        /// Struct containing all query parameters used in endpoints. Think of it as an HashMap,
        /// but with keys statically known.
        #[derive(Debug, Default)]
        struct QueryParameters<'a> {
            keyword: Option<Keyword>,
            $(
            $f_name: Option<Cow<'a, str>>,
            )*
        }

        impl<'a> QueryParameters<'a> {
            /// Build this struct from the query part of an URI.
            fn from_query(query: &'a str) -> Result<Self, Error> {
                let mut res: Self = Default::default();
                for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
                    match k.as_ref() {
                        $(
                            $kw_param => if let Some(prev_kw) = res.keyword.replace(Keyword::$kw_name) {
                                return Err(Error::bad_request(format!(
                                    "Multiple keywords: '{}' and '{}'", prev_kw, $kw_param
                                )));
                            },
                        )*
                        $(
                            $f_param => if !v.is_empty() {
                                if res.$f_name.replace(v).is_some() {
                                    return Err(Error::bad_request(format!(
                                        "Query parameter repeated: '{}'", k
                                    )));
                                }
                            },
                        )*
                        _ => {
                            if !(k.starts_with("response-") || k.starts_with("X-Amz-")) {
                                debug!("Received an unknown query parameter: '{}'", k);
                            }
                        }
                    };
                }
                Ok(res)
            }

            /// Get an error message in case not all parameters where used when extracting them to
            /// build an Endpoint variant
            fn nonempty_message(&self) -> Option<&str> {
                if self.keyword.is_some() {
                    Some("Keyword not used")
                } $(
                    else if self.$f_name.is_some() {
                        Some(concat!("'", $f_param, "'"))
                    }
                )* else {
                    None
                }
            }
        }
    }
}

pub use generateQueryParameters;
pub use router_match;
