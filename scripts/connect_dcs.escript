#!/usr/bin/env escript
%% -*- erlang -*-
%%! -name setup@BASHOIP
main(_) ->
  erlang:set_cookie(node(), antidote),
  io:fwrite("Starting dc connection script..."),
  Ips = Nodes,
  NodeNames = create_node_names(Ips),
  io:fwrite("Current list of servers: ~p", [NodeNames]),
  start_bg_processes(NodeNames),
  Descriptors = get_descriptors(NodeNames),
  connect_dcs(NodeNames, Descriptors).

create_node_names(Ips) ->
  lists:map(fun(Ip) ->
    NameStr = lists:concat(["antidote@", Ip]),
    list_to_atom(NameStr)
  end, Ips).

start_bg_processes(NodeNames) ->
  lists:foreach(fun(NodeName) ->
    io:fwrite("Invoking background processes on: ~p", [NodeName]),
    rpc:call(NodeName, inter_dc_manager, start_bg_processes, [stable])
  end, NodeNames).

get_descriptors(NodeNames) ->
  io:fwrite("Fetching descriptors..."),
  lists:map(fun(NodeName) ->
    {ok, Desc} = rpc:call(NodeName, inter_dc_manager, get_descriptor, []),
    Desc
  end, NodeNames).

connect_dcs(NodeNames, Descriptors) ->
  lists:foreach(fun(NodeName) ->
    io:fwrite("Connecting ~p to other dcs...", [NodeName]),
    rpc:call(NodeName, inter_dc_manager, observe_dcs_sync, [Descriptors])
  end, NodeNames).