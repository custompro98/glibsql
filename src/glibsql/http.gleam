//// glibsql/http helps construct a `gleam/http/request` for use with the [Hrana over HTTP](https://docs.turso.tech/sdk/http/reference) variant of libSQL,
//// simply pass the constructed HTTP request into your http client of choice.

import gleam/http
import gleam/http/request as http_request
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string

/// Statement wraps the supported types of requests.
/// A series of `ExecuteStatement(String)`s can be applied and
/// be conditionally followed with a `CloseStatement` to close
/// a connection when you are done with it.
pub type Statement {
  /// `ExecuteStatement` contains a query that will be executed as written.
  /// There is no SQL-injection protection provided, this type of statement
  /// should be used with a query builder that can render the built query
  /// to a prepared string.
  ExecuteStatement(sql: String)
  /// `CloseStatment` will either close the connection used in the current 
  /// pipeline or will close the connection referenced by the request baton.
  /// Note: connections will be automatically closed by Turso after a 10s timeout.
  CloseStatement
}

/// HttpRequest encapsulates everything needed to execute
/// a Hrana over HTTP libSQL request.
///
/// see `new_request()` to construct this record.
pub opaque type HttpRequest {
  HttpRequest(
    database: Option(String),
    organization: Option(String),
    host: Option(String),
    path: Option(String),
    token: Option(String),
    statements: List(Statement),
    baton: Option(String),
  )
}

/// Error type for all possible errors returned by glibsql/http.
pub opaque type GlibsqlError {
  /// Raised when a required property is not provided.
  MissingPropertyError(String)
}

/// Create a new Hrana over HTTP libSQL request.
///
/// Uses the builder pattern to construct everything necessary to send a request.
pub fn new_request() -> HttpRequest {
  HttpRequest(
    database: None,
    organization: None,
    host: None,
    path: None,
    token: None,
    statements: [],
    baton: None,
  )
}

/// Set the target database name.
/// Calling this function multiple times will override the previous value.
///
/// Given a Turso databse URL like libsql://example-database-myorganization.turso.io
/// The database name is "example-database"
pub fn with_database(request: HttpRequest, database: String) -> HttpRequest {
  HttpRequest(..request, database: Some(database))
}

/// Set the target database organization.
/// Calling this function multiple times will override the previous value.
///
/// Given a Turso databse URL like libsql://example-database-myorganization.turso.io
/// The database name is "myorganization"
pub fn with_organization(
  request: HttpRequest,
  organization: String,
) -> HttpRequest {
  HttpRequest(..request, organization: Some(organization))
}

/// Set the target database host.
/// NOTE: this defaults to Turso's turso.io
/// Calling this function multiple times will override the previous value.
///
/// Given a Turso databse URL like libsql://example-database-myorganization.turso.io
/// The host name is "turso.io"
pub fn with_host(request: HttpRequest, host: String) -> HttpRequest {
  HttpRequest(..request, host: Some(host))
}

/// Set the target database path on the host.
/// NOTE: this defaults to Turso's /v2/pipeline
/// Calling this function multiple times will override the previous value.
pub fn with_path(request: HttpRequest, path: String) -> HttpRequest {
  HttpRequest(..request, path: Some(path))
}

/// Set the Bearer token to access the database. Do not include `Bearer `.
/// Calling this function multiple times will override the previous value.
pub fn with_token(request: HttpRequest, token: String) -> HttpRequest {
  HttpRequest(..request, token: Some(token))
}

/// Set a statement on the request.
/// This function may be called multiple times, additional statements will be
/// executed in order.
pub fn with_statement(request: HttpRequest, statement: Statement) -> HttpRequest {
  HttpRequest(..request, statements: [statement, ..request.statements])
}

/// Clear all statements from the request.
pub fn clear_statements(request: HttpRequest) -> HttpRequest {
  HttpRequest(..request, statements: [])
}

/// Set the baton from a previous connection to be reused.
pub fn with_baton(request: HttpRequest, baton: String) -> HttpRequest {
  HttpRequest(..request, baton: Some(baton))
}

/// Build the request using the previously provided values.
/// Returns a gleam/http request suitable to be used in your HTTP client of choice.
pub fn build(
  request: HttpRequest,
) -> Result(http_request.Request(String), GlibsqlError) {
  use database <- result.try(option.to_result(
    request.database,
    MissingPropertyError("database"),
  ))

  use organization <- result.try(option.to_result(
    request.organization,
    MissingPropertyError("organization"),
  ))

  let host = option.unwrap(request.host, "turso.io")
  let path = option.unwrap(request.path, "/v2/pipeline")

  use token <- result.try(option.to_result(
    request.token,
    MissingPropertyError("token"),
  ))

  Ok(
    http_request.new()
    |> http_request.set_method(http.Post)
    |> http_request.set_scheme(http.Https)
    |> http_request.set_host(database <> "-" <> organization <> "." <> host)
    |> http_request.set_path(path)
    |> http_request.set_header(
      "Authorization",
      string.join(["Bearer", token], " "),
    )
    |> http_request.set_header("Content-Type", "application/json")
    |> http_request.set_header("Accept", "application/json")
    |> http_request.set_header("User-Agent", "glibsql/0.4.0")
    |> http_request.set_body(build_json(request)),
  )
}

fn build_json(req: HttpRequest) {
  let statements =
    list.reverse(req.statements)
    |> list.map(fn(stmt) {
      case stmt {
        ExecuteStatement(sql: sql) -> {
          json.object([
            #("type", json.string("execute")),
            #("stmt", json.object([#("sql", json.string(sql))])),
          ])
        }
        CloseStatement -> {
          json.object([#("type", json.string("close"))])
        }
      }
    })

  json.object([
    #("baton", json.nullable(req.baton, of: json.string)),
    #("requests", json.preprocessed_array(statements)),
  ])
  |> json.to_string
}
