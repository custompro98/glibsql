-module(app_ffi).

-export([
    decode/1
]).

decode(Json) ->
    thoas:decode(Json).
