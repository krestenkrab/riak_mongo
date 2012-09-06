%%
%% This file is part of riak_mongo
%%
%% Copyright (c) 2012 by Pavlo Baron (pb at pbit dot org)
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
%% @author Pavlo Baron <pb at pbit dot org>
%% @author Ward Bekker <ward@equanimity.nl>
%% @doc Here we process all kind of messages
%% @copyright 2012 Pavlo Baron

-module(riak_mongo_message).

-export([process_messages/2]).

-include ("riak_mongo_protocol.hrl").
-include("riak_mongo_state.hrl").
-include("riak_mongo_bson.hrl").
-include_lib("bson/include/bson_binary.hrl").

-define(CMD, <<"$cmd">>).
-define(ADM, <<"admin">>).
-define(DROP, <<"drop">>).
-define(COLLSTATS, <<"collstats">>).
-define(COUNT, <<"count">>).
-define(QUERY, <<"query">>).

%%
%% loop over messages
%%
process_messages([], State) ->
    State;
process_messages([Message|Rest], State) ->
    error_logger:info_msg("processing ~p~n", [Message]),
    case process_message(Message, State) of
        {noreply, OutState} ->
            ok;

        {reply, Reply, #worker_state{sock=Sock}=State2} ->
            MessageID = element(2, Message),
            ReplyMessage = Reply#mongo_reply{ request_id = State2#worker_state.request_id,
                                              reply_to = MessageID },
            OutState = State2#worker_state{ request_id = (State2#worker_state.request_id+1) },

            error_logger:info_msg("replying ~p~n", [ReplyMessage]),

            {ok, Packet} = riak_mongo_protocol:encode_packet(ReplyMessage),
            Size = byte_size(Packet),
            gen_tcp:send(Sock, <<?put_int32(Size+4), Packet/binary>>)
    end,

    process_messages(Rest, OutState);
process_messages(A1,A2) ->
    error_logger:info_msg("BAD ~p,~p~n", [A1,A2]),
    exit({badarg,A1,A2}).

process_message(#mongo_query{ db=DataBase, coll=?CMD,
                              selector=Selector}, State) ->

    {struct, [{Command, Collection}|Options]} = Selector,

    case db_command(DataBase, Command, Collection, Options, State) of
        {ok, Reply, State2} ->
            {reply, #mongo_reply{ documents=[ {struct, Reply} ]} , State2}
    end
;

process_message(#mongo_query{}=Message, State) ->
    {ok, Reply, State2} = riak_mongo_riak:find(Message, State),
    {reply, Reply, State2};

process_message(#mongo_getmore{}=Message, State) ->
    {ok, Reply, State2} = riak_mongo_riak:getmore(Message, State),
    {reply, Reply, State2};

process_message(#mongo_killcursor{ cursorids=IDs }, State = #worker_state { cursors=Dict0 }) ->
    %% todo: move this to riak_mongo_riak
    NewDict = lists:foldl(fun(CursorID, Dict) ->
                                  case dict:find(CursorID, Dict) of
                                      {ok, {Ref,PID}} ->
                                          erlang:demonitor(Ref),
                                          erlang:kill(PID, kill),
                                          dict:erase(CursorID, Dict);
                                      error ->
                                          Dict
                                  end
                          end,
                          Dict0,
                          IDs),
    {noreply, State#worker_state{ cursors=NewDict }};

process_message(#mongo_insert{ coll = <<"system.indexes">> }=Insert, State) ->
    State2 = riak_mongo_index:create_index( Insert, State ),
    {noreply, State2};

process_message(#mongo_insert{}=Insert, State) ->
    case check_docs(Insert) of
	[] -> 
	    State2 = riak_mongo_riak:insert(Insert, State),
	    {noreply, State2};
	Errors ->
	    {noreply, State#worker_state{ lastError=Errors }}
    end;

process_message(#mongo_delete{}=Delete, State) ->
    State2 = riak_mongo_riak:delete(Delete, State),
    {noreply, State2};

process_message(#mongo_update{}=Update, State) ->
    State2 = riak_mongo_riak:update(Update, State),
    {noreply, State2};

process_message(Message, State) ->
    error_logger:info_msg("unhandled message: ~p~n", [Message]),
    {noreply, State}.

%% internals

check_docs(#mongo_insert{documents=Docs, continueonerror=ContinueOnError}) ->
    lists:foldl(fun(#bson_raw_document{ body = Doc}, Err)
		      when Err =:= []; ContinueOnError =:= true ->
			{{struct, Fields}, _} = riak_mongo_bson:get_document(Doc),
			lists:foldl(fun({K, _V}, Erro) when Erro =:= [] ->
					   try <<"$", _Rest/binary>> = K of
					       _ ->
						   error_logger:info_msg("$ field found: ~p~n", [K]),
						   [{error, "can't insert with $ fields"}|Err]
					   catch
					       _:_ -> []
					   end
				   end,
				   [],
				   Fields)
		end,
		[],
		Docs).

you(#worker_state{sock=Sock}) ->
    {ok, {{A, B, C, D}, P}} = inet:peername(Sock), %IPv6???
    io_lib:format("~p.~p.~p.~p:~p", [A, B, C, D, P]).

clean_ok([ok|L]) -> clean_ok(L);
clean_ok(L) -> L.

db_command(?ADM, <<"whatsmyuri">>, _Collection, _Options, State) ->
    {ok, [{you, {utf8, you(State)}}, {ok, 1}], State};

db_command(?ADM, <<"replSetGetStatus">>, _Collection, _Options, State) ->
    _IsForShell = proplists:is_defined(forShell, _Options),
    {ok, [{ok, false}], State};

db_command(_DataBase, <<"getlasterror">>, _Collection, _Options, State) ->
    E = clean_ok(State#worker_state.lastError),
    case E of
        [] ->
            {ok, [{ok,true}], State#worker_state{lastError=[]}};
        MSG ->
            {ok, [{err, io:format("~p", [MSG])}], State#worker_state{lastError=[]}}
    end;

db_command(DataBase, ?DROP, Collection, _Options, State) ->
    NewState = riak_mongo_riak:delete(#mongo_delete{singleremove=false, selector={struct, []},
					 dbcoll=riak_mongo_protocol:join_dbcoll({DataBase, Collection})},
				      State),
    {ok, [{ok,true}], NewState};

db_command(DataBase, ?COLLSTATS, Collection, _Options, State) ->
    riak_mongo_riak:stats(riak_mongo_protocol:join_dbcoll({DataBase, Collection}), State);

db_command(DataBase, ?COUNT, Collection, Options, State) ->
    [{?QUERY, Query}|_] = Options,
    riak_mongo_riak:count(riak_mongo_protocol:join_dbcoll({DataBase, Collection}), Query, State);

db_command(DataBase, Command, Collection, _Options, State) ->
    error_logger:info_msg("unhandled command: ~p, ~p:~p~n", [Command, DataBase, Collection]),
    {ok, [{err, <<"unknown command: db=", DataBase, ", cmd=", Command/binary>>}, {ok, false}], State}.
