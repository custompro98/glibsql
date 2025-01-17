//// glibsql/libsql helps construct and eexecute a libSQL request.

import decode
import gleam/dynamic
import gleam/javascript/promise.{type Promise, await}
import gleam/option.{type Option, None, Some}
import gleam/result

/// Config encapsulates everything needed to construct a libSQL client.
///
/// see `new_client()` to construct this record.
pub opaque type Config {
  Builder(url: Option(String), token: Option(String))
}

/// LibsqlClient encapsulates everything needed to interact with a libSQL server.
///
/// see `build()` to construct this record.
pub type LibsqlClient

/// Error type for all possible errors returned by glibsql/http.
pub opaque type GlibsqlError {
  /// Raised when a required property is not provided.
  MissingPropertyError(String)
  ParseError(String)
}

/// Create a new libSQL client.
///
/// Uses the builder pattern to construct everything necessary to start a client.
pub fn new_client() -> Config {
  Builder(url: None, token: None)
}

/// Set the target database URL.
/// Calling this function multiple times will override the previous value.
pub fn with_url(client: Config, url: String) -> Config {
  Builder(..client, url: Some(url))
}

/// Set the Bearer token to access the database. Do not include `Bearer `.
/// Calling this function multiple times will override the previous value.
pub fn with_token(client: Config, token: String) -> Config {
  Builder(..client, token: Some(token))
}

/// Build the libSQL client using the previously provided values.
pub fn build(config: Config) {
  use url <- result.try(option.to_result(
    config.url,
    MissingPropertyError("url"),
  ))

  use token <- result.try(option.to_result(
    config.token,
    MissingPropertyError("token"),
  ))

  do_build(url, token)
}

@external(javascript, "../libsql_ffi.mjs", "do_build")
fn do_build(_url: String, _token: String) -> Result(LibsqlClient, GlibsqlError)

/// Represents a response from a libSQL query.
///
/// The `records` parameter is the type of the rows returned by the query.
pub opaque type LibsqlResponse(records) {
  LibsqlResponse(
    rows_affected: Int,
    last_insert_rowid: Option(Int),
    rows: records,
  )
}

/// Execute a query against the database.
///
/// Returns a `LibsqlResponse` containing
/// - the number of rows affected
/// - the last insert rowid
/// - the rows returned by the query (decoded using the provided decoder)
pub fn execute(
  query: String,
  on client: LibsqlClient,
  returning decoder: decode.Decoder(records),
) {
  use resp <- await(do_execute(client, query))

  case resp {
    Ok(resp) -> {
      decode.into({
        use rows_affected <- decode.parameter
        use last_insert_rowid <- decode.parameter
        use rows <- decode.parameter

        LibsqlResponse(rows_affected, last_insert_rowid, rows)
      })
      |> decode.field("rows_affected", decode.int)
      |> decode.field("last_insert_rowid", decode.optional(decode.int))
      |> decode.field("rows", decoder)
      |> decode.from(dynamic.from(resp))
      |> result.map_error(fn(_) {
        ParseError(
          "Failed to decode rows. Did you mean to provide a `decode.list(decoder)`?",
        )
      })
      |> promise.resolve
    }
    Error(error) -> {
      promise.resolve(Error(error))
    }
  }
}

@external(javascript, "../libsql_ffi.mjs", "do_execute")
fn do_execute(
  _client: LibsqlClient,
  _query: String,
) -> Promise(Result(String, GlibsqlError))
