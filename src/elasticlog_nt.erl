%% @doc
%%   iNTake interface for knowledge facts
-module(elasticlog_nt).
-include_lib("semantic/include/semantic.hrl").

-export([
   append/3,
   append_/3,
   identity/1
]).


%%
%%
append(Sock, {_, _, _} = Fact, Timeout) ->
   append(Sock, semantic:typed(Fact), Timeout);

append(Sock, #{s := S, p := P, o:= O}, Timeout) ->
   JsonS = semantic:to_json(S),
   JsonP = semantic:to_json(P),
   JsonO = semantic:to_json(O),
   esio:update(Sock, identity(JsonS), #{<<"rdf:id">> => JsonS, JsonP => JsonO}, Timeout);

append(Sock, #{} = JsonLD, Timeout) ->
   lists:foldl(
      fun(Fact, _) ->
         append(Sock, Fact, Timeout)
      end,
      {error, nocontent},
      semantic:jsonld(JsonLD)
   ).

%%
%%
append_(Sock, {_, _, _} = Fact, Flag) ->
   append_(Sock, semantic:typed(Fact), Flag);

append_(Sock, #{s := S, p := P, o:= O}, Flag) ->
   JsonS = semantic:to_json(S),
   JsonP = semantic:to_json(P),
   JsonO = semantic:to_json(O),
   esio:update_(Sock, identity(JsonS), #{<<"rdf:id">> => JsonS, JsonP => JsonO}, Flag);

append_(Sock, #{} = JsonLD, Flag) ->
   lists:foldl(
      fun(Fact, _) ->
         append_(Sock, Fact, Flag)
      end,
      {error, nocontent},
      semantic:jsonld(JsonLD)
   ).


%%
%%
identity(S) ->
   base64( crypto:hash(md5, [<<(erlang:phash2(S)):32>>]) ).

%%
%%
base64(Hash) ->
   << << (urlencode(D)) >> || <<D>> <= base64:encode(Hash), D =/= $= >>.

urlencode($/) -> $_;
urlencode($+) -> $-;
urlencode(D)  -> D.
