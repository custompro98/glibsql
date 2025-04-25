import dot_env
import gleam/dynamic/decode
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

fn env_decoder() -> decode.Decoder(Env) {
  use database_name <- decode.field("DATABASE_NAME", decode.string)
  use database_organization <- decode.field(
    "DATABASE_ORGANIZATION",
    decode.string,
  )
  use database_auth_token <- decode.field("DATABASE_AUTH_TOKEN", decode.string)
  decode.success(Env(
    database_name:,
    database_organization:,
    database_auth_token:,
  ))
}

const definitions = [
  #("DATABASE_NAME", glenv.String),
  #("DATABASE_ORGANIZATION", glenv.String),
  #("DATABASE_AUTH_TOKEN", glenv.String),
]

pub fn load() -> Result(Env, String) {
  dot_env.load_default()

  case glenv.load(env_decoder(), definitions) {
    Ok(env) -> Ok(env)
    Error(err) -> {
      let reason = case err {
        glenv.MissingKeyError(key) -> "Environment variable not found: " <> key
        glenv.InvalidEnvValueError(key, _) ->
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
