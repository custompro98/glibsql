import decode
import dot_env
import gleam/list
import gleam/string
import glenv

pub type Env {
  Env(
    database_name: String,
    database_organization: String,
    database_auth_token: String,
  )
}

const definitions = [
  #("DATABASE_NAME", glenv.String), #("DATABASE_ORGANIZATION", glenv.String),
  #("DATABASE_AUTH_TOKEN", glenv.String),
]

pub fn load() -> Result(Env, String) {
  dot_env.load_default()

  let decoder =
    decode.into({
      use database_name <- decode.parameter
      use database_organization <- decode.parameter
      use database_auth_token <- decode.parameter

      Env(database_name, database_organization, database_auth_token)
    })
    |> decode.field("DATABASE_NAME", decode.string)
    |> decode.field("DATABASE_ORGANIZATION", decode.string)
    |> decode.field("DATABASE_AUTH_TOKEN", decode.string)

  case glenv.load(decoder, definitions) {
    Ok(env) -> Ok(env)
    Error(err) -> {
      let reason = case err {
        glenv.NotFoundError(key) -> "Environment variable not found: " <> key
        glenv.ParseError(key, _) ->
          "Failed to parse environment variable: " <> key
        glenv.DefinitionMismatchError(errors) -> {
          let errors =
            list.map(errors, fn(error) {
              error.expected
              <> " expected, got "
              <> error.found
              <> " at "
              <> string.join(error.path, "->")
            })

          "Failed to match environment definition: "
          <> string.join(errors, ", ")
        }
      }

      Error(reason)
    }
  }
}
