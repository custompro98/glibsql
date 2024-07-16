-module(glibsql_http_ffi).

-export([
    decode/1
]).

decode(Json) ->
    thoas:decode(Json).
