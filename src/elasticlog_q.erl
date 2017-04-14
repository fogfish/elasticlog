%%
%%   Copyright 2016 Dmitry Kolesnikov, All Rights Reserved
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
%% @doc
%%   datalog sigma function supports knowledge statement only.
-module(elasticlog_q).
-include_lib("semantic/include/semantic.hrl").

-export([
   sigma/1
]).

%%
%% build sigma function
sigma(Pattern) ->
   fun(Sock) ->
      fun(Stream) ->
         sigma(Sock, datalog:bind(stream:head(Stream), Pattern))
      end
   end. 

sigma(Sock, #{'@' := IRI, '_' := Head} = Pattern) ->
   #rdf_seq{subclass = SubClass} = Spec = semantic:lookup(IRI),
   Query  = elasticlog_q5x:build(Spec, Pattern),
   Stream = esio:stream(Sock, to_json(SubClass), Query), 
   heap(Spec, Head, Stream).

%%
%% convert stream head (json object) to datalog heap
heap(#rdf_seq{seq = Seq}, Head, Stream) ->
   Spec = lists:filter(fun({_, H}) -> H /= '_' end, lists:zip(Seq, Head)),
   stream:map(
      fun(#{<<"_source">> := Json}) ->
         lists:foldl(fun(X, Heap) -> json_to_heap(X, Heap, Json) end, #{}, Spec)   
      end,
      Stream
   ).

json_to_heap({#rdf_property{id = IRI}, Key}, Heap, Json) ->
   case maps:get(to_json(IRI), Json, undefined) of
      undefined ->
         Heap;
      Value ->
         Heap#{Key => Value}
   end.

%%
%%
to_json({iri, Prefix, Suffix}) ->
   <<Prefix/binary, $:, Suffix/binary>>.

