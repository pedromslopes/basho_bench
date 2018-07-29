%%%-------------------------------------------------------------------
%%% @author pedrolopes
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(basho_bench_driver_aql_range).
-author("pedrolopes").

-export([new/1, run/4]).

-include_lib("basho_bench.hrl").

-record(state, {actor, tx = undefined, artists = [], column_dist = []}).

-define(ADD_WINS, aw).
-define(REMOVE_WINS, rw).
-define(UPDATE_WINS, 'update-wins').
-define(DELETE_WINS, 'delete-wins').
-define(CASCADE, cascade).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
  Actors = basho_bench_config:get(aql_actors, []),
  Shell = basho_bench_config:get(aql_shell, "aql"),
  Population = basho_bench_config:get(population, 500),
  Ip = lists:nth((Id rem length(Actors)+1), Actors),
  AQLNodeStr = lists:concat([Shell, "@", Ip]),
  AntidoteNodeStr = lists:concat(["antidote@", Ip]),
  AQLNode = list_to_atom(AQLNodeStr),
  AntidoteNode = list_to_atom(AntidoteNodeStr),
  case net_adm:ping(AQLNode) of
    pang ->
      lager:error("~s is not available", [AQLNode]),
      {error, "Connection error", #state{actor = undefined}};
    pong ->
      lager:info("worker ~b is bound to ~s", [Id, AQLNode]),
      %% start AQL application
      start_application(AQLNode),

      TxId = begin_transaction(AQLNode, AntidoteNode),
      create_schema(AQLNode, AntidoteNode, TxId),
      
      Artists =
        case Population of
          0 -> [];
          _Else -> populate_db(Id, Population, AQLNode, AntidoteNode, TxId)
        end,

      commit_transaction(AQLNode, AntidoteNode, TxId),

      ColumnDist = calc_column_dist(),
      
      {ok, #state{actor = {AQLNode, AntidoteNode}, tx = undefined,
        artists = Artists, column_dist = ColumnDist}}
  end.

run(get, KeyGen, ValGen, #state{actor = Node, tx = TxId, artists = Artists, column_dist = ColDist} = State) ->
  Table = "Artist",

  Value =
    case get_random(Artists) of
      undefined -> {to_varchar(KeyGen()), ValGen()};
      RandomValue -> RandomValue
    end,
  
  Query = lists:concat(["SELECT * FROM ", Table, " WHERE ", eq_where(Value, ColDist), ";"]),
  case exec(Node, Query, TxId) of
    {ok, _} ->
      {ok, State};
    {ok, _, _} ->
    	{ok, State};
    {error, _Reason} = Error ->
      lager:error("Error while querying: ~p", [Error]),
      {ok, State};
    Other ->
      lager:error("Something happened while querying: ~p", [Other]),
      {ok, State}
  end;
run(range, KeyGen, ValGen, #state{actor = Node, tx = TxId, artists = Artists, column_dist = ColDist} = State) ->
  Table = "Artist",

  Value1 =
    case get_random(Artists) of
      undefined -> {to_varchar(KeyGen()), ValGen()};
      RndValue1 -> RndValue1
    end,
  Value2 =
    case get_random(Artists) of
      undefined -> {to_varchar(KeyGen()), ValGen()};
      RndValue2 -> RndValue2
    end,

  Query = lists:concat(["SELECT * FROM ", Table, " WHERE ", range_where(Value1, Value2, ColDist), ";"]),
  case exec(Node, Query, TxId) of
    {ok, _} ->
      {ok, State};
    {ok, _, _} ->
      {ok, State};
    {error, _Reason} = Error ->
      lager:error("Error while querying: ~p", [Error]),
      {ok, State};
    Other ->
      lager:error("Something happened while querying: ~p", [Other]),
      {ok, State}
  end;
run(put, KeyGen, ValGen, #state{actor = Node, tx = TxId, artists = Artists} = State) ->
  Table = "Artist",

  Key = to_varchar(KeyGen()),
  Value = ValGen(),
  Values = create_insert(Key, Value),

  Query = lists:concat(["INSERT INTO ", Table, " VALUES ", Values, ";"]),
  case exec(Node, Query, TxId) of
    {ok, _} ->
      NewArtists = put_value({Key, Value}, Artists),
      {ok, State#state{artists = NewArtists}};
    {ok, _, _} ->
    	NewArtists = put_value({Key, Value}, Artists),
      {ok, State#state{artists = NewArtists}};
    {error, _Err} = Error ->
      lager:error("Error while inserting row: ~p", [Error]),
      {ok, State};
    Other ->
      lager:error("Something happened while inserting row: ~p", [Other]),
      {ok, State}
  end;
run(delete, KeyGen, _ValGen, #state{actor = Node, tx = TxId, artists = Artists} = State) ->
  Table = "Artist",

  Key =
    case get_random(Artists) of
      undefined -> to_varchar(KeyGen());
      {RndKey, _RndVal} -> RndKey
    end,
  
  Query = lists:concat(["DELETE FROM ", Table, " WHERE Name = ", Key, ";"]),
  case exec(Node, Query, TxId) of
    {ok, _} ->
      NewArtists = del_value(Key, Artists),
      {ok, State#state{artists=NewArtists}};
    {ok, _, _} ->
      NewArtists = del_value(Key, Artists),
      {ok, State#state{artists=NewArtists}};
    {error, _Err} = Error ->
      lager:error("Error while deleting row: ~p", [Error]),
      {ok, State};
    Other ->
      lager:error("Something happened while deleting row: ~p", [Other]),
      {ok, State}
  end;
run(Op, _KeyGen, _ValGen, _State) ->
  lager:warning("Unrecognized operation: ~p", [Op]).

start_application(AQLNode) ->
  {ok, _Started} = rpc:call(AQLNode, aql, start, []).

exec({AQLNode, AntidoteNode}, Query, TxId) ->
  %lager:info("Executing query: ~p", [Query]),
  rpc:call(AQLNode, aql, query, [Query, AntidoteNode, TxId]).

begin_transaction(AQLNode, AntidoteNode) ->
	{ok, _, Tx} = exec({AQLNode, AntidoteNode}, "BEGIN TRANSACTION;", undefined),
	Tx.

commit_transaction(AQLNode, AntidoteNode, TxId) ->
  {ok, _, _} = exec({AQLNode, AntidoteNode}, "COMMIT TRANSACTION;", TxId).

create_schema(AQLNode, AntidoteNode, TxId) ->
  TableQuery = "CREATE AW TABLE Artist (Name VARCHAR PRIMARY KEY, Age INTEGER);",
  exec({AQLNode, AntidoteNode}, TableQuery, TxId),

  case basho_bench_config:get(sec_indexes, false) of
    true ->
      IndexQuery = "CREATE INDEX AgeIdx ON Artist (Age)",
      exec({AQLNode, AntidoteNode}, IndexQuery, TxId);
    _ ->
      ok
  end.

populate_db(Id, Population, AQLNode, AntidoteNode, TxId) ->
	KeyGen = basho_bench_keygen:new(basho_bench_config:get(key_generator), Id),
	ValGen = basho_bench_keygen:new(basho_bench_config:get(value_generator), Id),

	lists:foldl(fun(_Elem, Artists) ->
    Key = to_varchar(KeyGen()),
    Value = ValGen(),
    Values = create_insert(Key, Value),
    Table = "Artist",
    Query = lists:concat(["INSERT INTO ", Table, " VALUES ", Values, ";"]),
		case exec({AQLNode, AntidoteNode}, Query, TxId) of
		  {ok, _} ->
		    put_value({Key, Value}, Artists);
		  {ok, _, _} ->
		  	put_value({Key, Value}, Artists);
		  {error, _Err} = Error ->
        lager:error("Error while populating: ~p", [Error]),
		    Artists;
      Other ->
        lager:error("Something happened while populating: ~p", [Other]),
        Artists
		end
	end, [], lists:seq(1, Population)).

calc_column_dist() ->
  ColFreq = basho_bench_config:get(column_search, [{1, "Name", 1}, {2, "Age", 1}]),
  lists:foldl(fun({Num, ColName, Frequency}, Acc) ->
    lists:append(Acc, lists:duplicate(Frequency, {Num, ColName}))
  end, [], ColFreq).

eq_where(Value, ColumnDist) when is_tuple(Value) ->
  {Num, Column} = get_random(ColumnDist),
  FinalValue = element(Num, Value),
  lists:concat([Column, " = ", FinalValue]);
eq_where(Value, ColumnDist) ->
  {_Num, Column} = get_random(ColumnDist),
  lists:concat([Column, " = ", Value]).

range_where(Value1, Value2, ColumnDist)
  when is_tuple(Value1) andalso is_tuple(Value2) ->
  {Num, Column} = get_random(ColumnDist),
  FinalValue1 = element(Num, Value1),
  FinalValue2 = element(Num, Value2),
  lists:concat([Column, " >= ", FinalValue1, " AND ", Column, " <= ", FinalValue2]);
range_where(Value1, Value2, ColumnDist) ->
  {_Num, Column} = get_random(ColumnDist),
  lists:concat([Column, " >= ", Value1, " AND ", Column, " <= ", Value2]).

create_insert(Key, Value) ->
  lists:concat(["(", Key, ", ", Value, ")"]).

to_varchar(Value) ->
  lists:concat(["'", Value, "'"]).

put_value(Value, Artists) ->
  [Value | Artists].

del_value(Key, Artists) ->
  lists:keydelete(Key, 1, Artists).

get_random([]) -> undefined;
get_random(Keys) ->
  FirstPos = rand:uniform(length(Keys)),
  lists:nth(FirstPos, Keys).
