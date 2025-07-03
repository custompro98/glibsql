//// glibsql/http helps construct a `gleam/http/request` for use with the 
//// [Hrana over HTTP](https://docs.turso.tech/sdk/http/reference) variant of libSQL,
//// simply pass the constructed HTTP request into your http client of choice.

import gleam/dynamic
import gleam/dynamic/decode
import gleam/float
import gleam/http
import gleam/http/request as http_request
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string

// Request side

/// Arguments are the arguments to a query, either anonymous or named.
/// Only one of the types may be used per statement.
pub type Argument {
  /// AnonymousArguments are the anonymous arguments to a query specified using the `?` syntax.
  AnonymousArgument(value: Value)
  /// NamedArguments are the named arguments to a query specified using either 
  /// the `:name`, the `@name`, or the `$name` syntax.
  NamedArgument(name: String, value: Value)
}

fn encode_argument(argument: Argument) -> json.Json {
  case argument {
    AnonymousArgument(value:) -> encode_value(value)
    NamedArgument(name:, value:) ->
      json.object([
        #("name", json.string(name)),
        #("value", encode_value(value)),
      ])
  }
}

fn encode_anonymous_arguments(arguments: Option(List(Argument))) -> json.Json {
  case arguments {
    Some(arguments) -> {
      arguments
      |> list.filter(fn(arg) {
        case arg {
          AnonymousArgument(_) -> True
          NamedArgument(_, _) -> False
        }
      })
      |> list.map(encode_argument)
      |> list.reverse
      |> json.preprocessed_array
    }
    None -> json.preprocessed_array([])
  }
}

fn encode_named_arguments(arguments: Option(List(Argument))) -> json.Json {
  case arguments {
    Some(arguments) -> {
      arguments
      |> list.filter(fn(arg) {
        case arg {
          AnonymousArgument(_) -> False
          NamedArgument(_, _) -> True
        }
      })
      |> list.map(encode_argument)
      |> list.reverse
      |> json.preprocessed_array
    }
    None -> json.preprocessed_array([])
  }
}

/// Statement wraps the supported types of requests.
/// A series of `ExecuteStatement(String)`s can be applied and
/// be conditionally followed with a `CloseStatement` to close
/// a connection when you are done with it.
///
/// See `new_statement()` to construct this record.
pub type Statement {
  /// `ExecuteStatement` contains a query that will be executed as written.
  /// There is no SQL-injection protection provided, this type of statement
  /// should be used with a query builder that can render the built query
  /// to a prepared string.
  ExecuteStatement(query: String, arguments: Option(List(Argument)))
  /// `CloseStatment` will either close the connection used in the current 
  /// pipeline or will close the connection referenced by the request baton.
  /// Note: connections will be automatically closed by Turso after a 10s timeout.
  CloseStatement
}

fn encode_statement(statement: Statement) -> json.Json {
  case statement {
    ExecuteStatement(query:, arguments:) ->
      json.object([
        #("type", json.string("execute")),
        #(
          "stmt",
          json.object([
            #("sql", json.string(query)),
            #("args", encode_anonymous_arguments(arguments)),
            #("named_args", encode_named_arguments(arguments)),
          ]),
        ),
      ])
    CloseStatement -> json.object([#("type", json.string("close"))])
  }
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

/// Create a new ExecuteStatement.
///
/// Uses the builder pattern to construct everything necessary to send a request.
pub fn new_statement() -> Statement {
  ExecuteStatement("", None)
}

/// Set a query on the ExecuteStatement.
/// Calling this function multiple times will override the previous value.
pub fn with_query(statement: Statement, query: String) -> Statement {
  case statement {
    ExecuteStatement(_, arguments) -> {
      ExecuteStatement(query, arguments)
    }
    CloseStatement -> CloseStatement
  }
}

/// Set an argument on the ExecuteStatement.
/// This function may be called multiple times, additional arguments will be
/// applied in order.
pub fn with_argument(statement: Statement, argument: Argument) -> Statement {
  case statement {
    ExecuteStatement(query, Some(arguments)) -> {
      ExecuteStatement(query, Some([argument, ..arguments]))
    }
    ExecuteStatement(query, None) -> {
      ExecuteStatement(query, Some([argument]))
    }
    CloseStatement -> CloseStatement
  }
}

/// Clear all arguments from the ExecuteStatement.
pub fn clear_arguments(statement: Statement) -> Statement {
  case statement {
    ExecuteStatement(query, _) -> {
      ExecuteStatement(query, None)
    }
    CloseStatement -> CloseStatement
  }
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
    |> http_request.set_header("User-Agent", "glibsql/0.7.1")
    |> http_request.set_body(build_json(request)),
  )
}

fn build_json(req: HttpRequest) {
  let statements =
    req.statements
    |> list.map(encode_statement)
    |> list.reverse

  json.object([
    #("baton", json.nullable(req.baton, of: json.string)),
    #("requests", json.preprocessed_array(statements)),
  ])
  |> json.to_string
}

// Response side

/// Values are actual column values.
pub type Value {
  /// Integers are integer values.
  Integer(value: Int)
  /// Reals are float values.
  Real(value: Float)
  /// Booleans are boolean values.
  Boolean(value: Bool)
  /// Texts are text values.
  Text(value: String)
  /// Datetimes are datetime values.
  Datetime(value: String)
  /// Blobs are blob values.
  Blob(value: String)
  /// Nulls are null values.
  Null
}

fn encode_value(value: Value) -> json.Json {
  case value {
    Integer(value:) ->
      json.object([
        #("type", json.string("integer")),
        #("value", json.string(int.to_string(value))),
      ])
    Real(value:) ->
      json.object([
        #("type", json.string("float")),
        #("value", json.string(float.to_string(value))),
      ])
    Boolean(value:) ->
      json.object([
        #("type", json.string("integer")),
        #(
          "value",
          json.string(case value {
            True -> "1"
            False -> "0"
          }),
        ),
      ])
    Text(value:) ->
      json.object([
        #("type", json.string("text")),
        #("value", json.string(value)),
      ])
    Datetime(value:) ->
      json.object([
        #("type", json.string("string")),
        #("value", json.string(value)),
      ])
    Blob(value:) ->
      json.object([
        #("type", json.string("blob")),
        #("value", json.string(value)),
      ])
    Null -> json.object([#("type", json.string("null"))])
  }
}

/// Columns are the columns returned from a query, specifying the name and type.
pub type Column {
  Column(name: String, type_: String)
}

fn column_decoder() -> decode.Decoder(Column) {
  use name <- decode.field("name", decode.string)
  use type_ <- decode.field("decltype", decode.string)
  decode.success(Column(name:, type_:))
}

/// Rows are the rows returned from a query, containing the values as a list.
pub type Row {
  Row(values: List(Value))
}

pub type Response {
  ExecuteResponse(columns: List(Column), rows: List(Row))
  CloseResponse
}

/// HttpResponses are the response from a query, containing the columns, rows, and other metadata.
pub type HttpResponse {
  HttpResponse(baton: Option(String), results: List(Response))
}

// Break down the response.

type IRowColumn {
  IRowColumn(type_: String, value: Option(String), base64: Option(String))
}

fn i_row_column_decoder() -> decode.Decoder(IRowColumn) {
  use type_ <- decode.field("type", decode.string)
  use value <- decode.optional_field(
    "value",
    None,
    decode.optional(
      decode.one_of(decode.string, or: [
        decode.float |> decode.map(float.to_string),
      ]),
    ),
  )
  // use base64 <- decode.field("base64", decode.optional(decode.string))
  decode.success(IRowColumn(type_:, value:, base64: None))
}

type IInnerResult {
  IInnerResult(rows: List(List(IRowColumn)), columns: List(Column))
}

fn i_inner_result_decoder() -> decode.Decoder(IInnerResult) {
  use rows <- decode.field(
    "rows",
    decode.list(decode.list(i_row_column_decoder())),
  )
  use columns <- decode.field("cols", decode.list(column_decoder()))
  decode.success(IInnerResult(rows:, columns:))
}

type IResponse {
  IResponse(type_: String, inner_result: Option(IInnerResult))
}

fn i_response_decoder() -> decode.Decoder(IResponse) {
  use type_ <- decode.field("type", decode.string)
  use inner_result <- decode.optional_field(
    "result",
    None,
    decode.optional(i_inner_result_decoder()),
  )
  decode.success(IResponse(type_:, inner_result:))
}

type IResult {
  IResult(type_: String, response: IResponse)
}

fn i_result_decoder() -> decode.Decoder(IResult) {
  use type_ <- decode.field("type", decode.string)
  use response <- decode.field("response", i_response_decoder())
  decode.success(IResult(type_:, response:))
}

type GlibsqlHttpResponse {
  GlibsqlHttpResponse(baton: Option(String), results: List(IResult))
}

fn glibsql_http_response_decoder() -> decode.Decoder(GlibsqlHttpResponse) {
  use baton <- decode.field("baton", decode.optional(decode.string))
  use results <- decode.field("results", decode.list(i_result_decoder()))
  decode.success(GlibsqlHttpResponse(baton:, results:))
}

// Turn the response into a Gleam data structure.

@target(erlang)
fn json_parse(json: String) {
  case json.parse(from: json, using: decode.dynamic) {
    Ok(dynamic_value) -> Ok(dynamic_value)
    Error(_err) -> Error(Nil)
  }
}



@target(javascript)
fn json_parse(json: String) {
  use dynamic_value <- result.try(decode_string(json))

  Ok(dynamic_value)
}

@external(javascript, "../glibsql_http_ffi.mjs", "decode")
fn decode_string(json: String) -> Result(dynamic.Dynamic, Nil)

pub fn decode_response(response: String) -> Result(HttpResponse, Nil) {
  use object <- result.try(json_parse(response))

  decode.run(object, glibsql_http_response_decoder())
  |> result.map(fn(resp) {
    let responses =
      list.map(resp.results, fn(res) {
        case res.response.inner_result {
          Some(inner_result) -> {
            let columns =
              list.map(inner_result.columns, fn(col) {
                Column(col.name, col.type_)
              })

            let rows =
              list.map(inner_result.rows, fn(row) {
                let zipped = list.zip(columns, row)

                Row(
                  list.map(zipped, fn(zip) {
                    let col = zip.0
                    let rowcol = zip.1

                    case col.type_, rowcol.type_ {
                      _, "null" -> Null
                      "INTEGER", _ -> {
                        let assert Ok(value) =
                          int.parse(rowcol.value |> option.unwrap(""))
                        Integer(value)
                      }
                      "REAL", _ -> {
                        let assert Ok(value) =
                          float.parse(rowcol.value |> option.unwrap(""))
                        Real(value)
                      }
                      "NUMERIC", _ -> {
                        let assert Ok(value) =
                          float.parse(rowcol.value |> option.unwrap(""))
                        Real(value)
                      }
                      // DECIMAL looks like "DECIMAL(10, 2)"
                      "DECIMAL" <> _, _ -> {
                        let assert Ok(value) =
                          float.parse(rowcol.value |> option.unwrap(""))
                        Real(value)
                      }
                      "BOOLEAN", _ ->
                        Boolean(rowcol.value |> option.unwrap("0") == "1")

                      "TEXT", _ -> Text(rowcol.value |> option.unwrap(""))
                      "DATETIME", _ ->
                        Datetime(rowcol.value |> option.unwrap(""))
                      "BLOB", _ -> Blob(rowcol.base64 |> option.unwrap(""))
                      coltype, rowcoltype -> {
                        panic as {
                          "Unhandled libsql data type "
                          <> coltype
                          <> " "
                          <> rowcoltype
                        }
                      }
                    }
                  }),
                )
              })

            ExecuteResponse(columns, rows)
          }
          None -> CloseResponse
        }
      })

    HttpResponse(results: responses, baton: resp.baton)
  })
  |> result.map_error(fn(_) { Nil })
}
