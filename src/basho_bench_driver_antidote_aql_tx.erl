%%%-------------------------------------------------------------------
%%% @author pedrolopes
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(basho_bench_driver_antidote_aql_tx).
-author("pedrolopes").

-export([new/1, run/4]).

-include_lib("basho_bench.hrl").

-record(state, {actor, tx = undefined, artists = [], albums = [], tracks = []}).


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
      TxId = begin_transaction(AQLNode, AntidoteNode),
      create_schema(Id, AQLNode, AntidoteNode, TxId),
      
      {Artists, Albums, Tracks} =
        case Population of
          0 -> {[], [], []};
          _Else -> populate_db(Id, Population, AQLNode, AntidoteNode, TxId)
        end,
      
      {ok, #state{actor = {AQLNode, AntidoteNode}, tx = TxId, artists=Artists, albums=Albums, tracks=Tracks}}
  end.

run(get, KeyGen, ValGen, #state{actor=Node, tx=TxId, artists=Artists, albums=Albums, tracks=Tracks} = State) ->
  Value = ValGen(),
  Table = integer_to_table(Value, undefined, undefined),
  
  Key = case get_random(Table, Artists, Albums, Tracks) of
  				undefined -> KeyGen();
  				RandomKey -> RandomKey
  			end,
  KeyStr = create_key(Key),
  
  Query = lists:concat(["SELECT * FROM ", Table, " WHERE Name = ", KeyStr]),
  case exec(Node, Query, TxId) of
    {ok, _} ->
      {ok, State};
    {ok, _, _} ->
    	{ok, State};
    {error, _Reason} ->
      {ok, State}
  end;
run(put, KeyGen, ValGen, #state{actor=Node, tx=TxId, artists=Artists, albums=Albums, tracks=Tracks} = State) ->
  Key = KeyGen(),
  KeyStr = create_key(Key),
  Value = ValGen(),
  Table = integer_to_table(Value, Artists, Albums),
  Values = gen_values(KeyStr, Table, Artists, Albums),
  Query = lists:concat(["INSERT INTO ", Table, " VALUES ", Values]),
  case exec(Node, Query, TxId) of
    {ok, _} ->
      {NewArtists, NewAlbums, NewTracks} = put_value(Table, Key, Artists, Albums, Tracks),
      {ok, State#state{artists=NewArtists, albums=NewAlbums, tracks=NewTracks}};
    {ok, _, _} ->
    	{NewArtists, NewAlbums, NewTracks} = put_value(Table, Key, Artists, Albums, Tracks),
      {ok, State#state{artists=NewArtists, albums=NewAlbums, tracks=NewTracks}};
    {error, _Err} ->
      {ok, State}
  end;
run(delete, KeyGen, ValGen, #state{actor=Node, tx=TxId, artists=Artists, albums=Albums, tracks=Tracks} = State) ->  
  Value = ValGen(),
  Table = integer_to_table(Value, Artists, Albums),
  
  Key = case get_random(Table, Artists, Albums, Tracks) of
  				undefined -> KeyGen();
  				RandomKey -> RandomKey
  			end,
  KeyStr = create_key(Key),
  
  Query = lists:concat(["DELETE FROM ", Table, " WHERE Name = ", KeyStr]),
  case exec(Node, Query, TxId) of
    {ok, _} ->
      {NewArtists, NewAlbums, NewTracks} = del_value(Table, Key, Artists, Albums, Tracks),
      {ok, State#state{artists=NewArtists, albums=NewAlbums, tracks=NewTracks}};
    {ok, _, _} ->
      {NewArtists, NewAlbums, NewTracks} = del_value(Table, Key, Artists, Albums, Tracks),
      {ok, State#state{artists=NewArtists, albums=NewAlbums, tracks=NewTracks}};
    {error, _Err} ->
      {ok, State}
  end;
run(Op, _KeyGen, _ValGen, _State) ->
  lager:warning("Unrecognized operation: ~p", [Op]).

exec({AQLNode, AntidoteNode}, Query, TxId) ->
  rpc:call(AQLNode, aqlparser, parse, [{str, Query}, AntidoteNode, TxId]).

begin_transaction(AQLNode, AntidoteNode) ->
	{ok, _, Tx} = exec({AQLNode, AntidoteNode}, "BEGIN TRANSACTION", undefined),
	Tx.

create_schema(1, AQLNode, AntidoteNode, TxId) ->
  ArtistsQuery = "CREATE AW TABLE Artist (Name VARCHAR PRIMARY KEY);",
  AlbumsQuery = "CREATE AW TABLE Album (Name VARCHAR PRIMARY KEY, " ++
    "Artist VARCHAR FOREIGN KEY UPDATE-WINS REFERENCES Artist(Name) ON DELETE CASCADE);",
  TracksQuery = "CREATE AW TABLE Track (Name VARCHAR PRIMARY KEY, " ++
    "Album VARCHAR FOREIGN KEY UPDATE-WINS REFERENCES Album(Name) ON DELETE CASCADE);",
  exec({AQLNode, AntidoteNode}, ArtistsQuery, TxId),
  exec({AQLNode, AntidoteNode}, AlbumsQuery, TxId),
  exec({AQLNode, AntidoteNode}, TracksQuery, TxId);
create_schema(_, _, _, _) -> ok.

populate_db(Id, Population, AQLNode, AntidoteNode, TxId) ->
	KeyGen = basho_bench_keygen:new(basho_bench_config:get(key_generator), Id),
	ValGen = basho_bench_keygen:new(basho_bench_config:get(value_generator), Id),

	lists:foldl(fun(_Elem, {Artists, Albums, Tracks}) ->
		Key = KeyGen(),
		KeyStr = create_key(Key),
		Value = ValGen(),
		Table = integer_to_table(Value, Artists, Albums),
		Values = gen_values(KeyStr, Table, Artists, Albums),
		Query = lists:concat(["INSERT INTO ", Table, " VALUES ", Values]),
		case exec({AQLNode, AntidoteNode}, Query, TxId) of
		  {ok, _} ->
		    put_value(Table, Key, Artists, Albums, Tracks);
		  {ok, _, _} ->
		  	put_value(Table, Key, Artists, Albums, Tracks);
		  {error, _Err} ->
		    {Artists, Albums, Tracks}
		end
	end, {[], [], []}, lists:seq(1, Population)).

create_key(Key) ->
  lists:concat(["'", integer_to_list(Key), "'"]).

put_value("Artist", Key, Artists, Albums, Tracks) ->
  {[Key | Artists], Albums, Tracks};
put_value("Album", Key, Artists, Albums, Tracks) ->
  {Artists, [Key | Albums], Tracks};
put_value("Track", Key, Artists, Albums, Tracks) ->
  {Artists, Albums, [Key | Tracks]}.

del_value("Artist", Key, Artists, Albums, Tracks) ->
  {lists:delete(Key, Artists), Albums, Tracks};
del_value("Album", Key, Artists, Albums, Tracks) ->
  {Artists, lists:delete(Key, Albums), Tracks};
del_value("Track", Key, Artists, Albums, Tracks) ->
  {Artists, Albums, lists:delete(Key, Tracks)}.

gen_values(Key, "Artist", _, _) ->
  lists:concat(["(", Key, ")"]);
gen_values(Key, "Album", [Artist | _Artists], _) ->
  lists:concat(["(", Key, ", '", Artist, "')"]);
gen_values(Key, "Track", _, [Album | _Albums]) ->
  lists:concat(["(", Key, ", '", Album, "')"]).

integer_to_table(1, _, _) -> "Artist";
integer_to_table(2, [], _) -> "Artist";
integer_to_table(2, _, _) -> "Album";
integer_to_table(3, [], []) -> "Artist";
integer_to_table(3, _, []) -> "Album";
integer_to_table(3, _, _) -> "Track".

get_random("Artist", Artists, _, _) ->
	get_random(Artists);
get_random("Album", _, Albums, _) ->
	get_random(Albums);
get_random("Track", _, _, Tracks) ->
	get_random(Tracks).

get_random([]) -> undefined;
get_random(Keys) ->
  FirstPos = rand:uniform(length(Keys)),
  lists:nth(FirstPos, Keys).

