import gleam/http
import gleam/http/request as http_request
import gleam/json
import gleam/list
import gleam/string

pub type Statement {
  ExecuteStatement(sql: String)
  CloseStatement
}

pub type LibsqlRequest {
  LibsqlRequest(
    database: String,
    organization: String,
    host: String,
    path: String,
    token: String,
    statements: List(Statement),
  )
}

pub fn new_request() -> LibsqlRequest {
  LibsqlRequest(
    database: "",
    organization: "",
    host: "turso.io",
    path: "/v2/pipeline",
    token: "",
    statements: [],
  )
}

pub fn with_database(request: LibsqlRequest, database: String) -> LibsqlRequest {
  LibsqlRequest(..request, database: database)
}

pub fn with_organization(
  request: LibsqlRequest,
  organization: String,
) -> LibsqlRequest {
  LibsqlRequest(..request, organization: organization)
}

pub fn with_host(request: LibsqlRequest, host: String) -> LibsqlRequest {
  LibsqlRequest(..request, host: host)
}

pub fn with_path(request: LibsqlRequest, path: String) -> LibsqlRequest {
  LibsqlRequest(..request, path: path)
}

pub fn with_token(request: LibsqlRequest, token: String) -> LibsqlRequest {
  LibsqlRequest(..request, token: token)
}

pub fn with_statement(
  request: LibsqlRequest,
  statement: Statement,
) -> LibsqlRequest {
  LibsqlRequest(..request, statements: [statement, ..request.statements])
}

pub fn build(request: LibsqlRequest) {
  http_request.new()
  |> http_request.set_method(http.Post)
  |> http_request.set_scheme(http.Https)
  |> http_request.set_host(
    request.database <> "-" <> request.organization <> "." <> request.host,
  )
  |> http_request.set_path(request.path)
  |> http_request.set_header(
    "Authorization",
    string.join(["Bearer", request.token], " "),
  )
  |> http_request.set_body(build_json(request))
}

fn build_json(req: LibsqlRequest) {
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

  json.object([#("requests", json.preprocessed_array(statements))])
  |> json.to_string
}
