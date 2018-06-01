%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2010 Basho Techonologies
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(basho_bench_driver_antidote_index).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-define(BOBJ_KEY, key).
-define(BOBJ_BUCKET, <<"bucket">>).

-define(TIMEOUT, 20000).
-record(state, {worker_id,
                target_node,
                index_type,
                entry_type,
                clock = ignore,
                acc_keys = []}).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
  Nodes = basho_bench_config:get(antidote_nodes),
  Cookie = basho_bench_config:get(antidote_cookie),

  Index = basho_bench_config:get(index_crdt),
  EntryType = basho_bench_config:get(index_entry_type),
  
  Population = basho_bench_config:get(population),

  %% Initialize cookie for each of the nodes
  true = erlang:set_cookie(node(), Cookie),
  [true = erlang:set_cookie(N, Cookie) || N <- Nodes],

  %% Try to ping each of the nodes
  AvailableNodes = ping_each(Nodes, []),

  %% Choose the node using our ID as a modulus
  TargetNode = lists:nth((Id rem length(AvailableNodes)+1), AvailableNodes),
  ?INFO("Using target node ~p for worker ~p\n", [TargetNode, Id]),
  %KeyDict= dict:new(),

  AccKeys = case Population of
  	0 -> ordsets:new();
  	_Else -> populate_index(Population, Index, EntryType, Id, TargetNode) % ordsets:from_list(lists:seq(1, Population))
  end,

  {ok,
    #state{worker_id = Id, target_node = TargetNode, index_type = Index, entry_type = EntryType, acc_keys = AccKeys}}.
    
populate_index(Population, CRDT, EntryType, PId, TargetNode) ->
	Seq = [lists:concat(["p", PId, ".", N]) || N <- lists:seq(1, Population)],
	
	BoundObject = {?BOBJ_KEY, CRDT, ?BOBJ_BUCKET},
	io:format("Populating index: ~p...~n", [BoundObject]),
	
	{ok, TxId} = rpc:call(TargetNode, antidote, start_transaction, [ignore, []]),
	
	Ops = lists:map(fun(EntryVal) ->
		EntryPK = random_string(10),
		to_operation(EntryType, EntryPK, EntryVal)
	end, Seq),
	
	Update = {BoundObject, update, Ops},
  	ok = rpc:call(TargetNode, antidote, update_objects, [[Update], TxId]),
  	{ok, _} = rpc:call(TargetNode, antidote, commit_transaction, [TxId]),
	  	
	ordsets:from_list(Seq).

%% @doc Read a key
run(get, _KeyGen, _ValueGen, State=#state{worker_id = Id, target_node = Node, index_type = Index,
  acc_keys = Keys, clock = Clock}) ->

  RndElem = get_random(Keys),
  BoundObject = {?BOBJ_KEY, Index, ?BOBJ_BUCKET},
  Read = {BoundObject, get, RndElem},
  ReadObjs = rpc:call(Node, antidote, read_objects, [Clock, [], [Read]]),
  case ReadObjs of
    {ok, _Value, CommitTime} ->
      {ok, State#state{clock=CommitTime}};
    {error,timeout} ->
      lager:info("Timeout on client ~p",[Id]),
      {error, timeout, State};
    {error, Reason} ->
      lager:error("Error: ~p",[Reason]),
      {error, Reason, State};
    {badrpc, Reason} ->
      {error, Reason, State}
  end;

run(add, KeyGen, _ValueGen,
    State=#state{worker_id = Id, target_node = Node, index_type = Index,
      entry_type = EntryType, acc_keys = Keys, clock = Clock}) ->

  IntKey = KeyGen(),
  EntryVal = list_to_binary(integer_to_list(IntKey)),
  EntryPK = random_string(10), %ValueGen(),
  Op = to_operation(EntryType, EntryPK, EntryVal),
  BoundObject = {?BOBJ_KEY, Index, ?BOBJ_BUCKET},
  Update = {BoundObject, update, Op},

  Response = rpc:call(Node, antidote, update_objects, [Clock, [], [Update]]),
  case Response of
    {ok, TS} ->
      Set = ordsets:add_element(EntryVal, Keys),
      {ok, State#state{clock=TS, acc_keys = Set}};
    {error,timeout} ->
      lager:info("Timeout on client ~p",[Id]),
      {error, timeout, State};
    {error, Reason} ->
      lager:error("Error: ~p",[Reason]),
      {error, Reason, State};
    error ->
      {error, abort, State};
    {badrpc, Reason} ->
      {error, Reason, State}
  end;

run(range, _KeyGen, _ValueGen, State=#state{worker_id = Id, target_node = Node, index_type = Index,
  acc_keys = Keys, clock = Clock}) ->

  RndRange = gen_range(Keys),
  BoundObject = {?BOBJ_KEY, Index, ?BOBJ_BUCKET},
  Read = {BoundObject, range, RndRange},

  ReadObjs = rpc:call(Node, antidote, read_objects, [Clock, [], [Read]]),
  case ReadObjs of
    {ok, _Value, CommitTime} ->
      {ok, State#state{clock=CommitTime}};
    {error,timeout} ->
      lager:info("Timeout on client ~p",[Id]),
      {error, timeout, State};
    {error, Reason} ->
      lager:error("Error: ~p",[Reason]),
      {error, Reason, State};
    {badrpc, Reason} ->
      {error, Reason, State}
  end;

run(read, _KeyGen, _ValueGen, State=#state{worker_id = Id, target_node = Node, index_type = Index,
  clock = Clock}) ->

  BoundObject = {?BOBJ_KEY, Index, ?BOBJ_BUCKET},
  ReadObjs = rpc:call(Node, antidote, read_objects, [Clock, [], [BoundObject]]),
  case ReadObjs of
    {ok, _Value, CommitTime} ->
      {ok, State#state{clock=CommitTime}};
    {error,timeout} ->
      lager:info("Timeout on client ~p",[Id]),
      {error, timeout, State};
    {error, Reason} ->
      lager:error("Error: ~p",[Reason]),
      {error, Reason, State};
    {badrpc, Reason} ->
      {error, Reason, State}
  end.

%% ====================================================================
%% Internal functions
%% ====================================================================

to_operation({CRDT, [Op]}, Key, Val) ->
  {CRDT, Key, {Op, Val}}.

ping_each([], Acc) ->
  Acc;
ping_each([Node | Rest], Acc) ->
  case net_adm:ping(Node) of
    pong ->
      ?INFO("Finished pinging ~p", [Node]),
      ping_each(Rest, Acc ++ [Node]);
    pang ->
      ?INFO("Failed to ping node ~p\n", [Node]),
      ping_each(Rest, Acc)
  end.

random_string(Len) ->
    Chrs = list_to_tuple("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"),
    ChrsSize = size(Chrs),
    F = fun(_, R) -> [element(rand_compat:uniform(ChrsSize), Chrs) | R] end,
    lists:foldl(F, "", lists:seq(1, Len)).

get_random([]) -> undefined;
get_random(Set) ->
  List = ordsets:to_list(Set),
  FirstPos = rand_compat:uniform(length(List)),
  lists:nth(FirstPos, List).

gen_range(WriteSet) ->
  List = ordsets:to_list(WriteSet),
  case length(List) of
    0 -> {infinity, infinity};
    1 -> [Elem] = List,
         case rand:normal() >= 0 of
           true -> {{greatereq, Elem}, infinity};
           false -> {infinity, {lessereq, Elem}}
         end;
    Len ->
         {FirstHalf, SecondHalf} = lists:split(Len div 2, List),
         FirstPos = rand_compat:uniform(length(FirstHalf)),
         {Elem1, _} = take_nth(FirstPos, FirstHalf),
         SecondPos = rand_compat:uniform(length(SecondHalf)),
         {Elem2, _} = take_nth(SecondPos, SecondHalf),
	 %%?INFO("{Elem1, Elem2} = {~p, ~p}\n", [Elem1, Elem2]),
         {{greatereq, Elem1}, {lessereq, Elem2}}
  end.

take_nth(N, List) ->
  take_nth(N, List, []).
take_nth(1, [H | T], Acc) -> {H, Acc ++ T};
take_nth(N, [H | T], Acc) when N > 1 ->
  take_nth(N - 1, T, Acc ++ [H]).
