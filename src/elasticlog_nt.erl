%% @doc
%%   iNTake interface for knowledge facts
-module(elasticlog_nt).

-export([
   append/3
]).


%%
%%
append(Sock, {_, _, _} = Fact, Timeout) ->
   #{s := S, p := P, o := O} = Spock = semantic:typed(Fact),
   Json  = elasticlog_codec:encode(Spock),
   Key   = <<"/nt/_doc/", (unique_key(S, P, O))/binary>>,
   esio:put(Sock, Key, Json).


%%
%%
unique_key(S, P, O) ->
   base64( crypto:hash(md5, [<<(erlang:phash2(S)):32>>, <<(erlang:phash2(P)):32>>, <<(erlang:phash2(O)):32>>]) ).

%%
%%
base64(Hash) ->
   << << (urlencode(D)) >> || <<D>> <= base64:encode(Hash), D =/= $= >>.

urlencode($/) -> $_;
urlencode($+) -> $-;
urlencode(D)  -> D.
