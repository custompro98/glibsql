import app/internal/env
import birl
import gleam/httpc
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import glibsql/http as glibsql

pub type User {
  User(
    id: Int,
    email: String,
    created_at: birl.Time,
    updated_at: Option(birl.Time),
  )
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

  let users =
    glibsql.decode_response(response.body)
    |> result.map(fn(resp) {
      list.filter(resp.results, fn(res) {
        case res {
          glibsql.ExecuteResponse(_, _) -> True
          glibsql.CloseResponse -> False
        }
      })
      |> list.flat_map(fn(res) {
        case res {
          glibsql.ExecuteResponse(_columns, rows) -> {
            list.map(rows, fn(row) {
              let assert [id, email, created_at, updated_at] = row.values

              User(
                id: case id {
                  glibsql.Integer(value) -> value
                  _ -> panic as "Unexpected type"
                },
                email: case email {
                  glibsql.Text(value) -> value
                  _ -> panic as "Unexpected type"
                },
                created_at: case created_at {
                  glibsql.Datetime(value) -> to_time(value)
                  _ -> panic as "Unexpected type"
                },
                updated_at: case updated_at {
                  glibsql.Datetime(value) -> Some(to_time(value))
                  glibsql.Null -> None
                  _ -> panic as "Unexpected type"
                },
              )
            })
          }
          _ -> []
        }
      })
    })
    |> result.unwrap([])

  let assert [
    User(1, "joe@example.com", _user_1_created_at, None),
    User(2, "chantel@example.com", _user_2_created_at, None),
    User(3, "bill@example.com", _user_3_created_at, None),
    User(4, "tom@example.com", _user_4_created_at, None),
  ] = users

  Ok(Nil)
}

fn to_time(value: String) -> birl.Time {
  let assert Ok(time) = birl.parse(value)
  time
}
