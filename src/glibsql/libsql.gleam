//// glibsql/libsql helps construct and eexecute a libSQL request.

import gleam/dynamic
import gleam/dynamic/decode
import gleam/javascript/promise.{type Promise}
import gleam/option.{type Option, None, Some}

@target(javascript)
/// LibsqlClient encapsulates everything needed to interact with a libSQL server.
pub opaque type LibsqlClient {
  LibsqlClient(internal: LibsqlClientInternal)
}

@target(javascript)
type LibsqlClientInternal

/// Connection type contains database connection info
pub type Connection {
  Connection(url: String, token: String, database: String, organization: String)
}

/// Config type for building connections
pub type Config {
  Config(url: String, token: Option(String))
}

/// QueryResult type containing query results
pub type QueryResult {
  QueryResult(rows: List(Row))
}

/// Row type representing a database row
pub type Row {
  Row(data: decode.Dynamic)
}

/// Error type for all possible errors returned by glibsql/http.
pub opaque type GlibsqlError {
  /// Raised when a required property is not provided.
  MissingPropertyError(String)
  ParseError(String)
}

@target(javascript)
/// External function to create a libSQL client using JavaScript FFI
@external(javascript, "../libsql_ffi.mjs", "do_build")
fn do_build(url: String, token: String) -> Result(LibsqlClientInternal, String)

@target(javascript)
/// External function to execute a query using JavaScript FFI
@external(javascript, "../libsql_ffi.mjs", "do_execute")
fn do_execute(
  client: LibsqlClientInternal,
  query: String,
) -> Promise(Result(ExecuteResult, String))

@target(javascript)
/// Result type from the JavaScript FFI
pub type ExecuteResult {
  ExecuteResult(
    rows_affected: Int,
    last_insert_rowid: String,
    rows: List(dynamic.Dynamic),
  )
}

/// Create a new config for connecting to libSQL
pub fn new_config(url: String) -> Config {
  Config(url: url, token: None)
}

/// Add token to config
pub fn with_auth_token(config: Config, token: String) -> Config {
  Config(..config, token: Some(token))
}

@target(javascript)
/// Connect to the database using config
pub fn connect(config: Config) -> Result(LibsqlClient, GlibsqlError) {
  case config.token {
    Some(token) -> {
      case do_build(config.url, token) {
        Ok(client) -> Ok(LibsqlClient(internal: client))
        Error(err) -> Error(ParseError("Failed to create client: " <> err))
      }
    }
    None -> Error(MissingPropertyError("token"))
  }
}

@target(erlang)
/// Connect to the database using config (Erlang target)
pub fn connect(config: Config) -> Result(Connection, GlibsqlError) {
  case config.token {
    Some(token) -> {
      // Simplified implementation for now
      Ok(Connection(
        url: config.url,
        token: token,
        database: "default",
        organization: "default",
      ))
    }
    None -> Error(MissingPropertyError("token"))
  }
}

@target(javascript)
/// Close a database connection (JavaScript target)
pub fn close(_client: LibsqlClient) -> Result(Nil, GlibsqlError) {
  Ok(Nil)
}

@target(erlang)
/// Close a database connection (Erlang target)
pub fn close(_conn: Connection) -> Result(Nil, GlibsqlError) {
  Ok(Nil)
}

@target(javascript)
/// Execute a query and return structured results (JavaScript target)
pub fn query(
  _client: LibsqlClient,
  _query: String,
  _params: List(dynamic.Dynamic),
) -> Result(QueryResult, GlibsqlError) {
  // This is a synchronous wrapper that will need to be implemented differently
  // For now, return an error indicating this should use the async execute function
  Error(ParseError("Use execute() function for async query execution"))
}

@target(erlang)
/// Execute a query and return structured results (Erlang target)
pub fn query(
  conn: Connection,
  query: String,
  _params: List(dynamic.Dynamic),
) -> Result(QueryResult, GlibsqlError) {
  // Placeholder implementation for Erlang target
  let _ = conn
  let _ = query
  Ok(QueryResult(rows: []))
}

/// Get integer value from row
pub fn get_int(_row: Row, _field: String) -> Result(Int, GlibsqlError) {
  // Simplified implementation
  Error(ParseError("Field extraction not implemented"))
}

/// Get string value from row
pub fn get_string(_row: Row, _field: String) -> Result(String, GlibsqlError) {
  // Simplified implementation
  Error(ParseError("Field extraction not implemented"))
}

/// Get float value from row
pub fn get_float(_row: Row, _field: String) -> Result(Float, GlibsqlError) {
  // Simplified implementation
  Error(ParseError("Field extraction not implemented"))
}

/// Get null value from row
pub fn get_null(_row: Row, _field: String) -> Result(Nil, GlibsqlError) {
  // Simplified implementation
  Error(ParseError("Field extraction not implemented"))
}

@target(javascript)
/// Execute a query against the database (JavaScript target).
///
/// Returns the raw response data as an ExecuteResult that can be used directly.
pub fn execute(
  query: String,
  on client: LibsqlClient,
) -> Promise(Result(ExecuteResult, GlibsqlError)) {
  use result <- promise.map(do_execute(client.internal, query))

  case result {
    Ok(execute_result) -> {
      Ok(execute_result)
    }
    Error(err) -> Error(ParseError("Query execution failed: " <> err))
  }
}

@target(erlang)
/// Execute a query against the database (Erlang target).
///
/// Returns the raw response data as a dynamic value that can be decoded as needed.
pub fn execute(
  sql_query: String,
  on _client: Connection,
) -> Promise(Result(decode.Dynamic, GlibsqlError)) {
  // For Erlang target, return an error indicating this needs proper implementation
  let _ = sql_query
  promise.resolve(
    Error(ParseError("Erlang target execute not yet implemented")),
  )
}
