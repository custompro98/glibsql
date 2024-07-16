//// glibsql/http helps construct a `gleam/http/request` for use with the [Hrana over HTTP](https://docs.turso.tech/sdk/http/reference) variant of libSQL,
//// simply pass the constructed HTTP request into your http client of choice.

import decode
import gleam/bit_array
import gleam/dynamic
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
    |> http_request.set_header("User-Agent", "glibsql/0.5.1")
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

// IResponse side

/// Values are the actual column values within a row.
pub type Value {
  /// Integers are the integer values returned from a query.
  Integer(value: Int)
  /// Reals are the float values returned from a query.
  Real(value: Float)
  /// Booleans are the boolean values returned from a query.
  Boolean(value: Bool)
  /// Texts are the text values returned from a query.
  Text(value: String)
  /// Datetimes are the datetime values returned from a query (as Strings).
  Datetime(value: String)
  /// Blobs are the blob values returned from a query (as Strings).
  Blob(value: String)
  /// Nulls are the null values returned from a query.
  Null
}

/// Columns are the columns returned from a query, specifying the name and type.
pub type Column {
  Column(name: String, type_: String)
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

type IInnerResult {
  IInnerResult(rows: List(List(IRowColumn)), columns: List(Column))
}

type IResponse {
  IResponse(type_: String, inner_result: Option(IInnerResult))
}

type IResult {
  IResult(type_: String, response: IResponse)
}

type GlibsqlHttpResponse {
  GlibsqlHttpResponse(baton: Option(String), results: List(IResult))
}

fn build_decoder() {
  let row_column_decoder =
    decode.into({
      use type_ <- decode.parameter
      use value <- decode.parameter
      use base64 <- decode.parameter

      IRowColumn(type_, value, base64)
    })
    |> decode.field("type", decode.string)
    |> decode.field(
      "value",
      decode.one_of([
        decode.optional(decode.string),
        decode.optional(
          decode.float
          |> decode.map(float.to_string),
        ),
      ]),
    )
    |> decode.field("base64", decode.optional(decode.string))

  let row_decoder = decode.list(of: row_column_decoder)

  let column_decoder =
    decode.into({
      use name <- decode.parameter
      use type_ <- decode.parameter

      Column(name, type_)
    })
    |> decode.field("name", decode.string)
    |> decode.field("decltype", decode.string)

  let inner_result_decoder =
    decode.into({
      use rows <- decode.parameter
      use columns <- decode.parameter

      IInnerResult(rows, columns)
    })
    |> decode.field("rows", decode.list(of: row_decoder))
    |> decode.field("cols", decode.list(of: column_decoder))

  let response_decoder =
    decode.into({
      use type_ <- decode.parameter
      use inner_result <- decode.parameter

      IResponse(type_, inner_result)
    })
    |> decode.field("type", decode.string)
    |> decode.field("result", decode.optional(inner_result_decoder))

  let result_decoder =
    decode.into({
      use type_ <- decode.parameter
      use response <- decode.parameter

      IResult(type_, response)
    })
    |> decode.field("type", decode.string)
    |> decode.field("response", response_decoder)

  let glibsql_http_response_decoder =
    decode.into({
      use baton <- decode.parameter
      use results <- decode.parameter

      GlibsqlHttpResponse(baton, results)
    })
    |> decode.field("baton", decode.optional(decode.string))
    |> decode.field("results", decode.list(of: result_decoder))

  glibsql_http_response_decoder
}

// Turn the response into a Gleam data structure.

@target(erlang)
fn json_parse(json: String) {
  let ba = bit_array.from_string(json)
  use dynamic_value <- result.try(decode_bits(ba))

  Ok(dynamic_value)
}

@external(erlang, "glibsql_http_ffi", "decode")
fn decode_bits(json: BitArray) -> Result(dynamic.Dynamic, Nil)

@target(javascript)
fn json_parse(json: String) {
  use dynamic_value <- result.try(decode_string(json))

  Ok(dynamic_value)
}

@external(javascript, "glibsql_http_ffi.mjs", "decode")
fn decode_string(json: String) -> Result(dynamic.Dynamic, Nil)

pub fn decode_response(response: String) -> Result(HttpResponse, Nil) {
  use object <- result.try(json_parse(response))

  build_decoder()
  |> decode.from(object)
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
  |> result.nil_error
}
