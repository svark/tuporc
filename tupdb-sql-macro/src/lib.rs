use proc_macro::TokenStream;
use quote::{format_ident, quote};
use std::fs;
use std::path::PathBuf;

#[derive(Debug)]
enum SqlFunctionType {
    QueryMap, // ends with &
    Query,    // ends with ?
    Mutation, // ends with !
    InsertId, // ends with ->
}

#[derive(Debug)]
struct SqlQuery {
    name: String,
    query: String,
    params: Vec<(String, String)>,
    #[allow(unused)]
    query_returns: String,
    fn_type: SqlFunctionType,
}

fn parse_sql_file(content: &str) -> Vec<SqlQuery> {
    let mut queries = Vec::new();
    let blocks = content.split("-- <eos>").filter(|block| !block.is_empty());

    for block in blocks {
        let mut lines = block.lines().filter(|l| !l.trim().is_empty());
        let mut name = String::new();
        let mut params = Vec::new();
        let mut query_returns = String::new();
        let mut in_params = false;
        let mut query = String::new();

        while let Some(line) = lines.next() {
            let line = line.trim();
            if line.starts_with("-- name:") {
                name = line.trim_start_matches("-- name:").trim().to_string();
            } else if line.starts_with("-- # Parameters") {
                in_params = true;
            } else if line.starts_with("-- param:") && in_params {
                let param_str = line.trim_start_matches("-- param:").trim();
                let parts: Vec<&str> = param_str.split(':').collect();
                if parts.len() == 2 {
                    params.push((parts[0].trim().to_string(), parts[1].trim().to_string()));
                }
            } else if line.starts_with("-- returns:") && in_params {
                let param_str = line.trim_start_matches("-- returns:").trim();
                query_returns = param_str.to_string();
            } else if !line.starts_with("--") {
                in_params = false;
                query.push_str(line);
                query.push('\n');
            }
        }

        if !name.is_empty() && !query.is_empty() {
            let fn_type = if name.ends_with('?') {
                SqlFunctionType::Query
            } else if name.ends_with("->") {
                SqlFunctionType::InsertId
            } else if name.ends_with('!') {
                SqlFunctionType::Mutation
            } else if name.ends_with("&") {
                SqlFunctionType::QueryMap
            } else {
                SqlFunctionType::Mutation // default to mutation if no suffix
            };

            // Remove the suffix from the name and any whitespace
            name = name
                .trim_end_matches('?')
                .trim_end_matches("->")
                .trim_end_matches('!')
                .trim_end_matches("&")
                .trim()
                .to_string();

            queries.push(SqlQuery {
                name,
                query: query.trim().to_string(),
                params,
                query_returns,
                fn_type,
            });
        }
    }
    queries
}

#[proc_macro]
pub fn generate_prepared_statements(input: TokenStream) -> TokenStream {
    let sql_file = input.to_string().trim_matches('"').to_string();

    // Get the path to Cargo.toml
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");

    // Combine manifest dir with the provided path
    let full_path = PathBuf::from(manifest_dir).join("src").join(&sql_file);

    let content = fs::read_to_string(&full_path)
        .unwrap_or_else(|_| panic!("Failed to read SQL file: {}", full_path.display()));

    // Extract filename without extension for trait name
    let file_name = std::path::Path::new(&sql_file)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("Unknown")
        .to_string();

    let trait_name = format_ident!("Tup{}Sql", file_name);
    let queries = parse_sql_file(&content);

    let mut trait_functions = Vec::new();
    let mut impl_functions = Vec::new();

    for query in queries {
        let fn_name = format_ident!("{}", query.name);
        let query_str = query.query.clone();
        let return_str = if query.query_returns.is_empty() {
            "()".to_string()
        } else {
            query.query_returns.clone()
        };
        use syn::parse_str;

        let return_type: syn::Type = parse_str(&return_str).expect("Failed to parse return type");

        // Generate parameter types for trait
        let param_types: Vec<_> = query
            .params
            .iter()
            .map(|(name, type_str)| {
                let param_name = format_ident!("{}", name);
                let param_type = match type_str.trim() {
                    t if t.contains("i64") => quote!(i64),
                    t if t.contains("i32") => quote!(i32),
                    t if t.contains("u8") => quote!(u8),
                    t if t.contains("&str") => quote!(&str),
                    _ => quote!(String),
                };
                quote! { #param_name: #param_type }
            })
            .collect();

        let param_names: Vec<_> = query
            .params
            .iter()
            .map(|(name, _)| format_ident!("{}", name))
            .collect();

        // Generate trait function signature and implementation
        match query.fn_type {
            SqlFunctionType::Query => {
                trait_functions.push(quote! {
                    fn #fn_name<F>(&self, #(#param_types,)* f: F) -> rusqlite::Result<#return_type>
                    where
                        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<#return_type>;
                });

                impl_functions.push(quote! {
                    fn #fn_name<F>(&self, #(#param_types,)* mut f: F) -> rusqlite::Result<#return_type>
                    where
                        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<#return_type>,
                    {
                        let mut stmt = self.prepare(#query_str)?;
                        stmt.query_row(
                        dynamic_named_params! {#(
                              #param_names
                           ),*
                        }, f)
                    }
                });
            }

            SqlFunctionType::QueryMap => {
                trait_functions.push(quote! {
                    fn #fn_name<F>(&self, #(#param_types,)* f: F) -> rusqlite::Result<()>
                    where
                        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<()>;
                });

                impl_functions.push(quote! {
                    fn #fn_name<F>(&self, #(#param_types,)* mut f: F) -> rusqlite::Result<()>
                    where
                        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<()>,
                    {
                        let mut stmt = self.prepare_cached(#query_str)?;
                        let mut rows = stmt.query(
                            dynamic_named_params! {#(
                                  #param_names
                               ),*
                            }
                        );
                        match rows {
                            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(()),
                            Err(e) => Err(e),
                            Ok(mut rows) => {
                                while let Some(row) = rows.next()? {
                                    f(&row)?;
                                }
                                Ok(())
                            }
                        }
                    }
                });
            }

            SqlFunctionType::InsertId => {
                trait_functions.push(quote! {
                    fn #fn_name(&self, #(#param_types),*) -> rusqlite::Result<i64>;
                });

                impl_functions.push(quote! {
                    fn #fn_name(&self, #(#param_types),*) -> rusqlite::Result<i64> {
                        let mut stmt = self.prepare_cached(#query_str)?;
                        stmt.insert(
                            dynamic_named_params!{#(
                                  #param_names
                               ),*
                            }
                        )?;
                        Ok(self.last_insert_rowid())
                    }
                });
            }
            SqlFunctionType::Mutation => {
                trait_functions.push(quote! {
                    fn #fn_name(&self, #(#param_types),*) -> rusqlite::Result<usize>;
                });

                impl_functions.push(quote! {
                    fn #fn_name(&self, #(#param_types),*) -> rusqlite::Result<usize> {
                        let mut stmt = self.prepare_cached(#query_str)?;
                        let sz: usize = stmt.execute(
                            dynamic_named_params! {#(
                                  #param_names
                               ),*
                            }
                        )?;
                        Ok(sz)
                    }
                });
            }
        }
    }

    // Generate trait and implementation for this SQL file
    let trait_def = quote! {
        pub trait #trait_name {
            #(#trait_functions)*
        }

        impl #trait_name for rusqlite::Connection {
            #(#impl_functions)*
        }
    };

    // Combine all traits into final output
    let output = quote! {
        #trait_def
    };

    output.into()
}
