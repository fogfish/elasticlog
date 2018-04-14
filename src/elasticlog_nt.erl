%% @doc
%%   iNTake interface for knowledge facts
-module(elasticlog_nt).
-include_lib("semantic/include/semantic.hrl").

-export([
   append/3
]).


%%
%%
append(Sock, {_, _, _} = Fact, Timeout) ->
   #{s := S, p := P, o:= O, type := Type} = Spock = semantic:typed(Fact),
   JsonS = elasticlog_codec:encode(?XSD_ANYURI, S),
   JsonP = elasticlog_codec:encode(?XSD_ANYURI, P),
   JsonO = elasticlog_codec:encode(Type, O),
   esio:update(Sock, unique_key(JsonS), #{JsonP => JsonO}).


%%
%%
unique_key(S) ->
   base64( crypto:hash(md5, [<<(erlang:phash2(S)):32>>]) ).

%%
%%
base64(Hash) ->
   << << (urlencode(D)) >> || <<D>> <= base64:encode(Hash), D =/= $= >>.

urlencode($/) -> $_;
urlencode($+) -> $-;
urlencode(D)  -> D.
