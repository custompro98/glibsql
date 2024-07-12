import app/internal/env
import decode
import gleam/bit_array
import gleam/dynamic
import gleam/httpc
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import glibsql/http as glibsql

pub type User {
  User(id: Int, email: String, created_at: String, updated_at: Option(String))
}

pub fn main() {
  let env = case env.load() {
    Ok(env) -> env
    Error(reason) -> {
      panic as reason
    }
  }

  let request =
    glibsql.new_request()
    |> glibsql.with_database(env.database_name)
    |> glibsql.with_organization(env.database_organization)
    |> glibsql.with_token(env.database_auth_token)
    |> glibsql.with_statement(glibsql.ExecuteStatement(
      sql: "SELECT id, email, created_at, updated_at FROM users",
    ))
    |> glibsql.with_statement(glibsql.CloseStatement)
    |> glibsql.build

  let assert Ok(request) = request

  use response <- result.try(httpc.send(request))

  let assert Ok(object) = json_parse(response.body)

  let assert Ok(rows) =
    build_decoder()
    |> decode.from(object)
    |> result.map(fn(resp) {
      let assert Ok(first) = list.first(resp.results)

      case first.response.inner_result {
        Some(inner_result) -> inner_result.rows
        None -> []
      }
    })

  let users = list.map(rows, fn(row) {
    let values = list.map(row, fn(column) { column.value |> option.unwrap("") })

    let assert [id, email, created_at, updated_at] = values
    let assert Ok(id) = int.parse(id)
    let updated_at = case updated_at {
      "" -> None
      str -> Some(str)
    }

    User(id, email, created_at, updated_at)
  })

  let assert [
    User(1, "joe@example.com", "2024-07-12 02:17:13", None),
    User(2, "chantel@example.com", "2024-07-12 02:17:20", None),
    User(3, "bill@example.com", "2024-07-12 02:17:23", None),
    User(4, "tom@example.com", "2024-07-12 02:17:28", None),
  ] = users

  Ok(Nil)
}

// Turn the response into a Gleam data structure.

fn json_parse(json: String) {
  let ba = bit_array.from_string(json)
  use dynamic_value <- result.try(decode_bits(ba))

  Ok(dynamic_value)
}

@external(erlang, "app_ffi", "decode")
fn decode_bits(json: BitArray) -> Result(dynamic.Dynamic, Nil)

// Break down the response.

type RowColumn {
  RowColumn(type_: String, value: Option(String))
}

type InnerResult {
  InnerResult(rows: List(List(RowColumn)))
}

type Response {
  Response(type_: String, inner_result: Option(InnerResult))
}

type IResult {
  IResult(type_: String, response: Response)
}

type GlibsqlHttpResponse {
  GlibsqlHttpResponse(results: List(IResult))
}

fn build_decoder() {
  let row_column_decoder =
    decode.into({
      use type_ <- decode.parameter
      use value <- decode.parameter

      RowColumn(type_, value)
    })
    |> decode.field("type", decode.string)
    |> decode.field("value", decode.optional(decode.string))

  let row_decoder = decode.list(of: row_column_decoder)

  let inner_result_decoder =
    decode.into({
      use rows <- decode.parameter

      InnerResult(rows)
    })
    |> decode.field("rows", decode.list(of: row_decoder))

  let response_decoder =
    decode.into({
      use type_ <- decode.parameter
      use inner_result <- decode.parameter

      Response(type_, inner_result)
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
      use results <- decode.parameter

      GlibsqlHttpResponse(results)
    })
    |> decode.field("results", decode.list(of: result_decoder))

  glibsql_http_response_decoder
}
