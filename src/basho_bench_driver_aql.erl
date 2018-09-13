%%%-------------------------------------------------------------------
%%% @author pedrolopes
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(basho_bench_driver_aql).
-author("pedrolopes").

-export([new/1, run/4]).

-include_lib("basho_bench.hrl").

-record(state, {actor, tx = undefined, galleries = [], artists = [], artworks = []}).

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
  Population = basho_bench_config:get(population, 500),
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
      create_schema(AQLNode, TxId),
      
      {Galleries, Artists, ArtWorks} =
        case Population of
          0 -> {[], [], []};
          _Else -> populate_db(Id, Population, AQLNode, TxId)
        end,

      commit_transaction(AQLNode, TxId),
      
      {ok, #state{actor = AQLNode, tx = undefined, galleries = Galleries, artists = Artists, artworks = ArtWorks}}
  end.

run(get, KeyGen, ValGen, #state{actor = Node, tx = TxId, galleries = Galleries, artists = Artists, artworks = ArtWorks} = State) ->
  Value = ValGen(),
  Table = integer_to_table(Value, undefined, undefined),
  
  Key = case get_random(Table, Galleries, Artists, ArtWorks) of
  				undefined -> KeyGen();
  				RandomKey -> RandomKey
  			end,
  KeyStr = create_key(Key),
  
  Query = lists:concat(["SELECT * FROM ", Table, " WHERE Name = ", KeyStr, ";"]),
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
run(put, KeyGen, ValGen, #state{actor = Node, tx = TxId, galleries = Galleries, artists = Artists, artworks = ArtWorks} = State) ->
  Key = KeyGen(),
  KeyStr = create_key(Key),
  Value = ValGen(),
  Table = integer_to_table(Value, Galleries, Artists),
  Values = gen_values(KeyStr, Table, Galleries, Artists),
  Query = lists:concat(["INSERT INTO ", Table, " VALUES ", Values, ";"]),
  case exec(Node, Query, TxId) of
    {ok, _} ->
      {NewGalleries, NewArtists, NewArtWorks} = put_value(Table, Key, Galleries, Artists, ArtWorks),
      {ok, State#state{galleries = NewGalleries, artists = NewArtists, artworks = NewArtWorks}};
    {ok, [{error, _Msg}], _} ->
      %lager:error("Error while inserting row: ~p", [Msg]),
      {ok, State};
    {ok, _, _} ->
    	{NewGalleries, NewArtists, NewArtWorks} = put_value(Table, Key, Galleries, Artists, ArtWorks),
      {ok, State#state{galleries = NewGalleries, artists = NewArtists, artworks = NewArtWorks}};
    {error, _Err} = Error ->
      lager:error("Error while inserting row: ~p", [Error]),
      {ok, State};
    Other ->
      lager:error("Something happened while inserting row: ~p", [Other]),
      {ok, State}
  end;
run(delete, KeyGen, ValGen, #state{actor = Node, tx = TxId, galleries = Galleries, artists = Artists, artworks = ArtWorks} = State) ->
  Value = ValGen(),
  Table = integer_to_table(Value, Galleries, Artists),
  
  Key = case get_random(Table, Galleries, Artists, ArtWorks) of
  				undefined -> KeyGen();
  				RandomKey -> RandomKey
  			end,
  KeyStr = create_key(Key),
  
  Query = lists:concat(["DELETE FROM ", Table, " WHERE Name = ", KeyStr, ";"]),
  case exec(Node, Query, TxId) of
    {ok, _} ->
      {NewGalleries, NewArtists, NewArtWorks} = del_value(Table, Key, Galleries, Artists, ArtWorks),
      {ok, State#state{galleries = NewGalleries, artists = NewArtists, artworks = NewArtWorks}};
    {ok, [{error, _Msg}], _} ->
      %lager:error("Error while deleting row: ~p", [Msg]),
      {ok, State};
    {ok, _, _} ->
      {NewGalleries, NewArtists, NewArtWorks} = del_value(Table, Key, Galleries, Artists, ArtWorks),
      {ok, State#state{galleries = NewGalleries, artists = NewArtists, artworks = NewArtWorks}};
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

create_schema(AQLNode, TxId) ->
  {GalleryTPolicy, _, _} =
    to_string(basho_bench_config:get(gallery_crp, {?UPDATE_WINS, undefined, undefined})),
  {ArtistTPolicy, ArtistDepPolicy, ArtistCascade} =
    to_string(basho_bench_config:get(artist_crp, {?UPDATE_WINS, ?UPDATE_WINS, ?CASCADE})),
  {ArtWorkTPolicy, ArtWorkDepPolicy, ArtWorkCascade} =
    to_string(basho_bench_config:get(artwork_crp, {?UPDATE_WINS, ?UPDATE_WINS, ?CASCADE})),

  Partitions = basho_bench_config:get(partitions, []),

  GalleryQuery = "CREATE " ++ GalleryTPolicy ++ " TABLE Gallery (Name VARCHAR PRIMARY KEY) " ++
    partition_to_string('Gallery', Partitions) ++ ";",
  ArtistQuery = "CREATE " ++ ArtistTPolicy ++ " TABLE Artist (Name VARCHAR PRIMARY KEY, " ++
    "Gallery VARCHAR FOREIGN KEY " ++ ArtistDepPolicy ++ " REFERENCES Gallery(Name) " ++
    ArtistCascade ++ ") " ++ partition_to_string('Artist', Partitions) ++ ";",
  ArtWorkQuery = "CREATE " ++ ArtWorkTPolicy ++ " TABLE ArtWork (Name VARCHAR PRIMARY KEY, " ++
    "Artist VARCHAR FOREIGN KEY " ++ ArtWorkDepPolicy ++ " REFERENCES Artist(Name) " ++
    ArtWorkCascade ++ ") " ++ partition_to_string('ArtWork', Partitions) ++ ";",

  exec(AQLNode, GalleryQuery, TxId),
  exec(AQLNode, ArtistQuery, TxId),
  exec(AQLNode, ArtWorkQuery, TxId),

  Indexes = basho_bench_config:get(indexes, []),
  lists:foreach(fun(IndexSpec) ->
    IndexQuery = index_query(IndexSpec),
    exec(AQLNode, IndexQuery, TxId)
  end, Indexes).

  %IndexAlbumQuery = "CREATE INDEX ArtistIdx ON Album(Artist);",
  %IndexTrackQuery = "CREATE INDEX AlbumIdx ON Track(Album);",
  %exec(AQLNode, IndexAlbumQuery, TxId),
  %exec(AQLNode, IndexTrackQuery, TxId).

populate_db(Id, Population, AQLNode, TxId) ->
	KeyGen = basho_bench_keygen:new(basho_bench_config:get(key_generator), Id),
	ValGen = basho_bench_keygen:new(basho_bench_config:get(value_generator), Id),

	lists:foldl(fun(_Elem, {Galleries, Artists, ArtWorks}) ->
		Key = KeyGen(),
		KeyStr = create_key(Key),
		Value = ValGen(),
		Table = integer_to_table(Value, Galleries, Artists),
		Values = gen_values(KeyStr, Table, Galleries, Artists),
		Query = lists:concat(["INSERT INTO ", Table, " VALUES ", Values, ";"]),
		case exec(AQLNode, Query, TxId) of
		  {ok, _} ->
		    put_value(Table, Key, Galleries, Artists, ArtWorks);
		  {ok, _, _} ->
		  	put_value(Table, Key, Galleries, Artists, ArtWorks);
		  {error, _Err} = Error ->
        lager:error("Error while populating: ~p", [Error]),
		    {Galleries, Artists, ArtWorks};
      Other ->
        lager:error("Something happened while populating: ~p", [Other]),
        {Galleries, Artists, ArtWorks}
		end
	end, {[], [], []}, lists:seq(1, Population)).

create_key(Key) ->
  lists:concat(["'", integer_to_list(Key), "'"]).

put_value("Gallery", Key, Galleries, Artists, ArtWorks) ->
  {[Key | Galleries], Artists, ArtWorks};
put_value("Artist", Key, Galleries, Artists, ArtWorks) ->
  {Galleries, [Key | Artists], ArtWorks};
put_value("ArtWork", Key, Galleries, Artists, ArtWorks) ->
  {Galleries, Artists, [Key | ArtWorks]}.

del_value("Gallery", Key, Galleries, Artists, ArtWorks) ->
  {lists:delete(Key, Galleries), Artists, ArtWorks};
del_value("Artist", Key, Galleries, Artists, ArtWorks) ->
  {Galleries, lists:delete(Key, Artists), ArtWorks};
del_value("ArtWork", Key, Galleries, Artists, ArtWorks) ->
  {Galleries, Artists, lists:delete(Key, ArtWorks)}.

gen_values(Key, "Gallery", _, _) ->
  lists:concat(["(", Key, ")"]);
gen_values(Key, "Artist", [Gallery | _Galleries], _) ->
  lists:concat(["(", Key, ", '", Gallery, "')"]);
gen_values(Key, "ArtWork", _, [Artist | _Artists]) ->
  lists:concat(["(", Key, ", '", Artist, "')"]).

integer_to_table(1, _, _) -> "Gallery";
integer_to_table(2, [], _) -> "Gallery";
integer_to_table(2, _, _) -> "Artist";
integer_to_table(3, [], []) -> "Gallery";
integer_to_table(3, _, []) -> "Artist";
integer_to_table(3, _, _) -> "ArtWork".

get_random("Gallery", Galleries, _, _) ->
	get_random(Galleries);
get_random("Artist", _, Artists, _) ->
	get_random(Artists);
get_random("ArtWork", _, _, ArtWorks) ->
	get_random(ArtWorks).

get_random([]) -> undefined;
get_random(Keys) ->
  FirstPos = rand:uniform(length(Keys)),
  lists:nth(FirstPos, Keys).

to_string({TPolicy, DepPolicy, Cascade}) ->
  {to_string(TPolicy), to_string(DepPolicy), to_string(Cascade)};
to_string(?UPDATE_WINS) -> "UPDATE-WINS";
to_string(?DELETE_WINS) -> "DELETE-WINS";
to_string(?NO_CONCURRENCY) -> "";
to_string(?CASCADE) -> "ON DELETE CASCADE";
to_string(?RESTRICT) -> "";
to_string(_) -> "".

partition_to_string(_Table, []) -> "";
partition_to_string(Table, Partitions) ->
  case lists:keyfind(Table, 1, Partitions) of
    false ->
      "";
    {Table, PartColumn} ->
      lists:append(["PARTITION ON (", atom_to_list(PartColumn), ")"])
  end.

index_query({IName, ITable, IColumn}) ->
  INameStr = atom_to_list(IName),
  ITableStr = atom_to_list(ITable),
  IColumnStr = atom_to_list(IColumn),
  lists:append(["CREATE INDEX ", INameStr, " ON ", ITableStr, "(", IColumnStr , ");"]).