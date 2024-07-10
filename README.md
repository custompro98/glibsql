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
import glibsql
import gleam/httpc
import gleam/result

pub fn main() {
  let request = 
    glibsql.new_http_request()
    |> glibsql.with_database("database")
    |> glibsql.with_organization("organization")
    |> glibsql.with_token("token")
    |> glibsql.with_statement(glibsql.ExecuteStatement(
      sql: "SELECT * FROM users",
    ))
    |> glibsql.with_statement(glibsql.CloseStatement)
    |> glibsql.build

  use response <- result.try(httpc.send(request))

  // ...
}
```

Further documentation can be found at <https://hexdocs.pm/glibsql>.

## Development

```sh
gleam run   # Run the project
gleam test  # Run the tests
gleam shell # Run an Erlang shell
```
