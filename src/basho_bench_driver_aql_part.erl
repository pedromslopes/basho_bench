%%%-------------------------------------------------------------------
%%% @author pedrolopes
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(basho_bench_driver_aql_part).
-author("pedrolopes").

-export([new/1, run/4]).

-include_lib("basho_bench.hrl").

-record(state, {actor, tx = undefined, artists = []}).

-define(UPDATE_WINS, 'update-wins').
-define(DELETE_WINS, 'delete-wins').
-define(NO_CONCURRENCY, 'no-concurrency').
-define(CASCADE, cascade).
-define(RESTRICT, restrict).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
  Actors = basho_bench_config:get(aql_actors, []),
  Shell = basho_bench_config:get(aql_shell, "aql"),
  Population = basho_bench_config:get(population, 0),
  Ip = lists:nth((Id rem length(Actors)+1), Actors),
  AQLNodeStr = lists:concat([Shell, "@", Ip]),
  AQLNode = list_to_atom(AQLNodeStr),
  case net_adm:ping(AQLNode) of
    pang ->
      lager:error("~s is not available", [AQLNode]),
      {error, "Connection error", #state{actor = undefined}};
    pong ->
      lager:info("worker ~b is bound to ~s", [Id, AQLNode]),
      %% start AQL application
      %start_application(AQLNode),

      TxId = begin_transaction(AQLNode),
      create_schema(Id, AQLNode, TxId),
      
      Artists =
        case Population of
          0 -> [];
          _Else -> populate_db(Id, Population, AQLNode, TxId)
        end,

      commit_transaction(AQLNode, TxId),
      
      {ok, #state{actor = AQLNode, tx = undefined, artists = Artists}}
  end.

run(get, KeyGen, _ValGen, #state{actor = Node, tx = TxId, artists = Artists} = State) ->
  Key =
    case get_random(Artists) of
      undefined -> KeyGen();
      RandomKey -> RandomKey
    end,
  KeyStr = create_key(Key),
  
  Query = lists:concat(["SELECT * FROM Artist WHERE Name = ", KeyStr, ";"]),
  case exec(Node, Query, TxId) of
    {ok, _} ->
      {ok, State};
    {ok, [{error, _Msg}], _} ->
      %lager:error("Error while querying: ~p", [Msg]),
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
  Key = KeyGen(),
  KeyStr = create_key(Key),
  Value = ValGen(),
  Values = gen_values(KeyStr, Value),
  Query = lists:concat(["INSERT INTO Artist VALUES ", Values, ";"]),
  case exec(Node, Query, TxId) of
    {ok, _} ->
      NewArtists = put_value(Key, Artists),
      {ok, State#state{artists = NewArtists}};
    {ok, [{error, _Msg}], _} ->
      %lager:error("Error while inserting row: ~p", [Msg]),
      {ok, State};
    {ok, _, _} ->
    	NewArtists = put_value(Key, Artists),
      {ok, State#state{artists = NewArtists}};
    {error, _Err} = Error ->
      lager:error("Error while inserting row: ~p", [Error]),
      {ok, State};
    Other ->
      lager:error("Something happened while inserting row: ~p", [Other]),
      {ok, State}
  end;
run(delete, KeyGen, _ValGen, #state{actor = Node, tx = TxId, artists = Artists} = State) ->
  Key = case get_random(Artists) of
  				undefined -> KeyGen();
  				RandomKey -> RandomKey
  			end,
  KeyStr = create_key(Key),
  
  Query = lists:concat(["DELETE FROM Artist WHERE Name = ", KeyStr, ";"]),
  case exec(Node, Query, TxId) of
    {ok, _} ->
      NewArtists = del_value(Key, Artists),
      {ok, State#state{artists = NewArtists}};
    {ok, [{error, _Msg}], _} ->
      %lager:error("Error while deleting row: ~p", [Msg]),
      {ok, State};
    {ok, _, _} ->
      NewArtists = del_value(Key, Artists),
      {ok, State#state{artists = NewArtists}};
    {error, _Err} = Error ->
      lager:error("Error while deleting row: ~p", [Error]),
      {ok, State};
    Other ->
      lager:error("Something happened while deleting row: ~p", [Other]),
      {ok, State}
  end;
run(Op, _KeyGen, _ValGen, _State) ->
  lager:warning("Unrecognized operation: ~p", [Op]).

%start_application(AQLNode) ->
%  {ok, _Started} = rpc:call(AQLNode, aql, start, []).

exec(AQLNode, Query, TxId) ->
  %lager:info("Executing query: ~p", [Query]),
  rpc:call(AQLNode, aql, query, [Query, TxId]).

begin_transaction(AQLNode) ->
	{ok, _, Tx} = exec(AQLNode, "BEGIN TRANSACTION;", undefined),
	Tx.

commit_transaction(AQLNode, TxId) ->
  {ok, _, _} = exec(AQLNode, "COMMIT TRANSACTION;", TxId).

create_schema(1, AQLNode, TxId) ->
  Partitions = basho_bench_config:get(partitions, []),

  ArtistQuery = "CREATE UPDATE-WINS TABLE Artist (Name VARCHAR PRIMARY KEY, Age INTEGER) " ++
    partition_to_string('Artist', Partitions) ++ ";",

  exec(AQLNode, ArtistQuery, TxId);
create_schema(_Id, _AQLNode, _TxId) ->
  ok.

  %IndexAlbumQuery = "CREATE INDEX ArtistIdx ON Album(Artist);",
  %IndexTrackQuery = "CREATE INDEX AlbumIdx ON Track(Album);",
  %exec(AQLNode, IndexAlbumQuery, TxId),
  %exec(AQLNode, IndexTrackQuery, TxId).

populate_db(Id, Population, AQLNode, TxId) ->
	KeyGen = basho_bench_keygen:new(basho_bench_config:get(key_generator), Id),
	ValGen = basho_bench_keygen:new(basho_bench_config:get(value_generator), Id),

	lists:foldl(fun(_Elem, Artists) ->
    Key = KeyGen(),
    KeyStr = create_key(Key),
    Value = ValGen(),
    Values = gen_values(KeyStr, Value),
		Query = lists:concat(["INSERT INTO Artist VALUES ", Values, ";"]),
		case exec(AQLNode, Query, TxId) of
		  {ok, _} ->
		    put_value(Key, Artists);
		  {ok, _, _} ->
		  	put_value(Key, Artists);
		  {error, _Err} = Error ->
        lager:error("Error while populating: ~p", [Error]),
		    Artists;
      Other ->
        lager:error("Something happened while populating: ~p", [Other]),
        Artists
		end
	end, [], lists:seq(1, Population)).

create_key(Key) ->
  lists:concat(["'", integer_to_list(Key), "'"]).

put_value(Key, Artists) ->
  [Key | Artists].

del_value(Key, Artists) ->
  lists:delete(Key, Artists).

gen_values(Key, Value) ->
  lists:concat(["(", Key, ", '", Value, "')"]).

get_random([]) -> undefined;
get_random(Keys) ->
  FirstPos = rand:uniform(length(Keys)),
  lists:nth(FirstPos, Keys).

partition_to_string(_Table, []) -> "";
partition_to_string(Table, Partitions) ->
  case lists:keyfind(Table, 1, Partitions) of
    false ->
      "";
    {Table, PartColumn} ->
      lists:append(["PARTITION ON (", atom_to_list(PartColumn), ")"])
  end.
