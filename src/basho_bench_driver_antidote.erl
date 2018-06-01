%%%-------------------------------------------------------------------
%%% @author joao
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. set 2017 13:27
%%%-------------------------------------------------------------------
-module(basho_bench_driver_antidote).
-author("joao").

-export([new/1,
  run/4]).

-include_lib("basho_bench.hrl").

-record(state, {actor, artists = [], albums = []}).


%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
  Actors = basho_bench_config:get(aql_actors, []),
  Ip = aql_utils:get_ip(Actors),
  AntidoteNodeStr = lists:concat(["antidote@", Ip]),
  AntidoteNode = list_to_atom(AntidoteNodeStr),
  case net_adm:ping(AntidoteNode) of
    pang ->
      lager:error("~s is not available", [AntidoteNode]),
      {error, "Connection error", #state{actor = undefined}};
    pong ->
      lager:info("worker ~b is bound to ~s", [Id, AntidoteNode]),
      {ok, #state{actor = AntidoteNode}}
  end.

run(get, KeyGen, ValGen, #state{actor=Node} = State) ->
  Key = KeyGen(),
  KeyStr = integer_to_list(Key),
  Value = ValGen(),
  case exec(Node, read_objects, {KeyStr, antidote_crdt_gmap, Value}, KeyGen()) of
    {ok, _} ->
      {ok, State};
    {error, Reason} ->
      {error, Reason, State}
  end;
run(put, KeyGen, ValGen, #state{actor=Node} = State) ->
  Key = KeyGen(),
  KeyStr = integer_to_list(Key),
  Value = ValGen(),
  Obj = {{KeyStr, antidote_crdt_gmap, Value}, update, [{keyA, antidote_crdt_integer, {set, 5}},
    {keyB, antidote_crdt_integer, {set, 5}},
    {keyC, antidote_crdt_integer, {set, 5}}]},
  case exec(Node, update_objects, Obj, KeyGen()) of
    {ok, _} ->
      {ok, State};
    {error, Err} ->
      {error, Err, State}
  end;
run(Op, _KeyGen, _ValGen, State) ->
  Reason = lists:concat(["Unrecognized operation: ", Op]),
  {err, Reason, State}.

exec(AntidoteNode, Method, Query, ExtraKey) ->
  {ok, TxId} = rpc:call(AntidoteNode, antidote, start_transaction, [ignore, []]),
  case Method of
    update_objects ->
      rpc:call(AntidoteNode, antidote, read_objects, [[{ExtraKey, antidote_crdt_gmap, random}], TxId]);
    _Else ->
      ok
  end,
  rpc:call(AntidoteNode, antidote, Method, [[Query], TxId]),
  rpc:call(AntidoteNode, antidote, commit_transaction, [TxId]).
