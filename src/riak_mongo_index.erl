%%
%% This file is part of riak_mongo
%%
%% Copyright (c) 2012 by Trifork
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%

%% @author Kresten Krab Thorup <krab@trifork.com>

-module(riak_mongo_index).


-include("riak_mongo_protocol.hrl").
-include("riak_mongo_bson.hrl").
-include("riak_mongo_state.hrl").


-export([create_index/2, index_object/3]).

-define(RING_KEY, riak_mongo_index).


create_index(#mongo_insert{ documents=Docs, continueonerror=ContinueOnError, coll = <<"system.indexes">> }, State) ->

    {ok, C} = riak:local_client(State),

    Errors =
        lists:foldl(fun(#bson_raw_document{ id=BSON_ID, body=Binary}, Err)
                          when Err=:=[]; ContinueOnError=:=true ->

                            {{struct, ATTs}, <<>>} = riak_mongo_bson:get_document(Binary),

                            NS   = proplists:get_value(<<"ns">>, ATTs),
                            Key  = proplists:get_value(<<"key">>, ATTs),
                            Name = proplists:get_value(<<"name">>, ATTs),

                            Props = C:get_bucket(NS),
                            Was = proplists:get_value(?RING_KEY, Props, []),
                            NewIndexes = [{Name,Key}|proplists:delete(Name,Was)],
                            NewProps = [{?RING_KEY, NewIndexes}|proplists:delete(?RING_KEY,Props)],
                            C:set_bucket(NS,NewProps),

                            error_logger:info_msg("processing index ns=~p, key=~p, name=~p ~n",
                                                  [NS, Key, Name]),

                            Err
                    end,
                    [],
                    Docs),

    State#worker_state{ lastError=Errors }.

get_indexes(Bucket, Client) ->
    Props = Client:get_bucket(Bucket),
    proplists:get_value(?RING_KEY, Props, []).

%% @doc compute the index value to add to the riak_object's metadata

index_object(Bucket, #bson_raw_document{ body=Binary }, Client) ->
    case get_indexes(Bucket, Client) of
        [] -> [];
        Indexes ->
            {JSONDoc, <<>>} = riak_mongo_bson:get_document(Binary),
            [ compute_index(Index, JSONDoc) || Index <- Indexes ]
    end.

compute_index({Name, {struct, Bindings}}, JSONDoc) ->
    Values = lists:foldl(fun({IDX, Order}, Acc) when Order == 1.0 ->
                               [ get_property(IDX, JSONDoc) | Acc];
                            ({_IDX, <<"2d">>}, Acc) ->
                               %% ignoring spatial index
                               Acc
                         end,
                         [],
                         Bindings),

    {<< Name/binary, "_bin">>, sext:encode(list_to_tuple(Values))}.

get_property(Name, {struct, Atts}) ->
    case binary:match(Name, <<$.>>) of
       {Pos, _Len} ->
           <<First :Pos /binary, $.:8, Rest /binary>> = Name,
           SubObject = proplists:get_value(First, Atts, undefined),
           get_property(Rest, SubObject);
       _ ->
           proplists:get_value(Name, Atts, undefined)
    end.
