%%%-------------------------------------------------------------------
%%% @author joao
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. nov 2017 16:13
%%%-------------------------------------------------------------------
-module(aql_utils).
-author("joao").

%% API
-export([get_ip/1]).

get_ip(Actors) ->
  Remotes = basho_bench_config:get(remote_nodes, []),
  RemoteNodes = lists:map(fun toNode/1, Remotes),
  case indexof(node(), RemoteNodes) of
    -1 -> lists:nth(1, Actors);
    Index -> lists:nth(Index+1, Actors)
  end.

toNode({Host, Name}) ->
  NameStr = atom_to_list(Name),
  HostStr = atom_to_list(Host),
  list_to_atom(lists:flatten([NameStr, "@", HostStr])).

indexof(Item, List) -> indexof(Item, List, 1).

indexof(_, [], _) -> -1;
indexof(Item, [Item|_], Index) -> Index;
indexof(Item, [_|List], Index) -> indexof(Item, List, Index+1).
