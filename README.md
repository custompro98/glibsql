# glibsql

[![Package Version](https://img.shields.io/hexpm/v/glibsql)](https://hex.pm/packages/glibsql)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/glibsql/)

Glibsql is a library for interacting with a hosted libSQL database such as [Turso](https://turso.tech).

Glibsql helps construct a `gleam/http/request` for use with the [Hrana over HTTP](https://docs.turso.tech/sdk/http/reference) variant of libSQL,
simply pass the constructed HTTP request into your http client of choice.

```sh
gleam add glibsql
```
```gleam
import gleam/httpc
import gleam/result
import glibsql

pub fn main() {
  // The first request does not have to include a `CloseStatement`,
  // a baton is returned from the first request to be used in subsequent requests.
  let request =
    base_request()
    |> glibsql.with_statement(glibsql.ExecuteStatement(
      sql: "BEGIN",
    ))
    |> glibsql.with_statement(glibsql.ExecuteStatement(
      sql: "SELECT * FROM users",
    ))
    |> glibsql.build

  use response <- result.try(httpc.send(request))

  // ...

  // The second request uses the baton from the first request to reuse the connection.
  // This is useful if you are making multiple requests to the same database, especially
  // within a transaction.
  let second_request =
    base_request()
    |> glibsql.with_statement(glibsql.ExecuteStatement(
      sql: "UPDATE posts SET status = 'published' WHERE user_id IN (1,2,3)",
    ))
    |> glibsql.with_statement(glibsql.ExecuteStatement(
      sql: "COMMIT",
    ))
    |> glibsql.with_statement(glibsql.CloseStatement)
    |> glibsql.with_baton("<baton from previous request>")
    |> glibsql.build

  use response <- result.try(httpc.send(second_request))
}

fn base_request() {
  glibsql.new_http_request()
  |> glibsql.with_database("database")
  |> glibsql.with_organization("organization")
  |> glibsql.with_token("token")
}

```

Further documentation can be found at <https://hexdocs.pm/glibsql>.

## Development

```sh
gleam run   # Run the project
gleam test  # Run the tests
gleam shell # Run an Erlang shell
```
