import gleam/http
import gleam/http/request as http_request
import gleeunit/should
import glibsql/http as glibsql

pub fn builder_custom_host_test() {
  let expected =
    http_request.new()
    |> http_request.set_method(http.Post)
    |> http_request.set_scheme(http.Https)
    |> http_request.set_host("database-organization.example.com")
    |> http_request.set_path("/v1/acme")
    |> http_request.set_header("Authorization", "Bearer token")
    |> http_request.set_header("Content-Type", "application/json")
    |> http_request.set_header("Accept", "application/json")
    |> http_request.set_header("User-Agent", "glibsql/0.5.1")
    |> http_request.set_body("{\"baton\":null,\"requests\":[]}")

  glibsql.new_request()
  |> glibsql.with_database("database")
  |> glibsql.with_organization("organization")
  |> glibsql.with_host("example.com")
  |> glibsql.with_path("/v1/acme")
  |> glibsql.with_token("token")
  |> glibsql.build
  |> should.equal(Ok(expected))
}

pub fn builder_no_statements_test() {
  let expected =
    http_request.new()
    |> http_request.set_method(http.Post)
    |> http_request.set_scheme(http.Https)
    |> http_request.set_host("database-organization.turso.io")
    |> http_request.set_path("/v2/pipeline")
    |> http_request.set_header("Authorization", "Bearer token")
    |> http_request.set_header("Content-Type", "application/json")
    |> http_request.set_header("Accept", "application/json")
    |> http_request.set_header("User-Agent", "glibsql/0.5.1")
    |> http_request.set_body("{\"baton\":null,\"requests\":[]}")

  glibsql.new_request()
  |> glibsql.with_database("database")
  |> glibsql.with_organization("organization")
  |> glibsql.with_token("token")
  |> glibsql.build
  |> should.equal(Ok(expected))
}

pub fn builder_single_statement_test() {
  let expected =
    http_request.new()
    |> http_request.set_method(http.Post)
    |> http_request.set_scheme(http.Https)
    |> http_request.set_host("database-organization.turso.io")
    |> http_request.set_path("/v2/pipeline")
    |> http_request.set_header("Authorization", "Bearer token")
    |> http_request.set_header("Content-Type", "application/json")
    |> http_request.set_header("Accept", "application/json")
    |> http_request.set_header("User-Agent", "glibsql/0.5.1")
    |> http_request.set_body(
      "{\"baton\":null,\"requests\":[{\"type\":\"execute\",\"stmt\":{\"sql\":\"SELECT * FROM users\"}},{\"type\":\"close\"}]}",
    )

  glibsql.new_request()
  |> glibsql.with_database("database")
  |> glibsql.with_organization("organization")
  |> glibsql.with_token("token")
  |> glibsql.with_statement(glibsql.ExecuteStatement(sql: "SELECT * FROM users"))
  |> glibsql.with_statement(glibsql.CloseStatement)
  |> glibsql.build
  |> should.equal(Ok(expected))
}

pub fn builder_many_statement_test() {
  let expected =
    http_request.new()
    |> http_request.set_method(http.Post)
    |> http_request.set_scheme(http.Https)
    |> http_request.set_host("database-organization.turso.io")
    |> http_request.set_path("/v2/pipeline")
    |> http_request.set_header("Authorization", "Bearer token")
    |> http_request.set_header("Content-Type", "application/json")
    |> http_request.set_header("Accept", "application/json")
    |> http_request.set_header("User-Agent", "glibsql/0.5.1")
    |> http_request.set_body(
      "{\"baton\":null,\"requests\":[{\"type\":\"execute\",\"stmt\":{\"sql\":\"SELECT * FROM users\"}},{\"type\":\"execute\",\"stmt\":{\"sql\":\"SELECT * FROM posts\"}},{\"type\":\"close\"}]}",
    )

  glibsql.new_request()
  |> glibsql.with_database("database")
  |> glibsql.with_organization("organization")
  |> glibsql.with_token("token")
  |> glibsql.with_statement(glibsql.ExecuteStatement(sql: "SELECT * FROM users"))
  |> glibsql.with_statement(glibsql.ExecuteStatement(sql: "SELECT * FROM posts"))
  |> glibsql.with_statement(glibsql.CloseStatement)
  |> glibsql.build
  |> should.equal(Ok(expected))
}

pub fn builder_clear_statements_test() {
  let expected =
    http_request.new()
    |> http_request.set_method(http.Post)
    |> http_request.set_scheme(http.Https)
    |> http_request.set_host("database-organization.turso.io")
    |> http_request.set_path("/v2/pipeline")
    |> http_request.set_header("Authorization", "Bearer token")
    |> http_request.set_header("Content-Type", "application/json")
    |> http_request.set_header("Accept", "application/json")
    |> http_request.set_header("User-Agent", "glibsql/0.5.1")
    |> http_request.set_body("{\"baton\":null,\"requests\":[]}")

  glibsql.new_request()
  |> glibsql.with_database("database")
  |> glibsql.with_organization("organization")
  |> glibsql.with_token("token")
  |> glibsql.with_statement(glibsql.ExecuteStatement(sql: "SELECT * FROM users"))
  |> glibsql.with_statement(glibsql.ExecuteStatement(sql: "SELECT * FROM posts"))
  |> glibsql.with_statement(glibsql.CloseStatement)
  |> glibsql.clear_statements
  |> glibsql.build
  |> should.equal(Ok(expected))
}

pub fn builder_baton_test() {
  let expected =
    http_request.new()
    |> http_request.set_method(http.Post)
    |> http_request.set_scheme(http.Https)
    |> http_request.set_host("database-organization.turso.io")
    |> http_request.set_path("/v2/pipeline")
    |> http_request.set_header("Authorization", "Bearer token")
    |> http_request.set_header("Content-Type", "application/json")
    |> http_request.set_header("Accept", "application/json")
    |> http_request.set_header("User-Agent", "glibsql/0.5.1")
    |> http_request.set_body(
      "{\"baton\":\"baton\",\"requests\":[{\"type\":\"close\"}]}",
    )

  glibsql.new_request()
  |> glibsql.with_database("database")
  |> glibsql.with_organization("organization")
  |> glibsql.with_token("token")
  |> glibsql.with_statement(glibsql.CloseStatement)
  |> glibsql.with_baton("baton")
  |> glibsql.build
  |> should.equal(Ok(expected))
}

pub fn builder_missing_fields_test() {
  glibsql.new_request()
  |> glibsql.build
  |> should.be_error
}
