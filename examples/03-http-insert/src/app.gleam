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

  let statement =
    glibsql.new_statement()
    |> glibsql.with_query(
      "INSERT INTO users (email) VALUES (?), (?) RETURNING *",
    )
    |> glibsql.with_argument(
      glibsql.AnonymousArgument(value: glibsql.Text("phil@example.com")),
    )
    |> glibsql.with_argument(
      glibsql.AnonymousArgument(value: glibsql.Text("joey@example.com")),
    )

  let request =
    glibsql.new_request()
    |> glibsql.with_database(env.database_name)
    |> glibsql.with_organization(env.database_organization)
    |> glibsql.with_token(env.database_auth_token)
    |> glibsql.with_statement(statement)
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
    User(_new_user_id_1, "phil@example.com", _new_user_1_created_at, None),
    User(_new_user_id_2, "joey@example.com", _new_user_2_created_at, None),
  ] = users

  let assert Ok(_response) = clean_up(env)

  Ok(Nil)
}

fn to_time(value: String) -> birl.Time {
  let assert Ok(time) = birl.parse(value)
  time
}

fn clean_up(env: env.Env) {
  let statement =
    glibsql.new_statement()
    |> glibsql.with_query("DELETE FROM users WHERE id > 4")

  let request =
    glibsql.new_request()
    |> glibsql.with_database(env.database_name)
    |> glibsql.with_organization(env.database_organization)
    |> glibsql.with_token(env.database_auth_token)
    |> glibsql.with_statement(statement)
    |> glibsql.with_statement(glibsql.CloseStatement)
    |> glibsql.build

  let assert Ok(request) = request
  httpc.send(request)
}
