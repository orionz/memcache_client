-module(memcache_binary_tcp_connection, [ Host, Port, Socket ] ).

-include_lib("memcache.hrl").

-define(TCP_OPTIONS, [binary, {packet, 0}, {active, false}]).

-export([send_request/1,raw/3]).

%% binary raw
%% if its quiet - just write it and dont read
%% if its not quiet - read and pass back until you hit the opaque
%% special case stats

send_request(R) ->
  send_opaque_request(R#request{ opaque=gen_opaque() }).

send_opaque_request(R) when R#request.opcode > ?STAT ->
  write(R);
send_opaque_request(R) when R#request.opcode =:= ?STAT ->
  write(R),
  read_stats(R#request.opaque, []);
%% this  is a little screwy since text returns an array and this does not
%send_opaque_request(R) when R#request.num_keys > 1->
send_opaque_request(R) when (R#request.opcode =:= ?GET) or (R#request.opcode =:= ?GETK)->
  Terminal = gen_opaque(),
  many_write(R, Terminal, R#request.key),
  read_responses(R#request.opaque, Terminal, []);
send_opaque_request(R) ->
  write(R),
  read_response(R#request.opaque).

many_write(R, Terminal,[ K ]) ->
  write(R#request{ opaque=Terminal, key=K, num_keys=1});
many_write(R, Terminal,[ K | T ]) ->
  write(R#request{ opcode=?GETKQ, key=K, num_keys=1}), %% is there any reason to distinguish between GETQ / GETKQ?
  many_write(R, Terminal, T).

gen_extras(R) ->
  case R#request.opcode of
    ?SET -> <<(R#request.flags):32,(R#request.expires):32>>;
    ?SETQ -> <<(R#request.flags):32,(R#request.expires):32>>;
    ?ADD -> <<(R#request.flags):32,(R#request.expires):32>>;
    ?ADDQ -> <<(R#request.flags):32,(R#request.expires):32>>;
    ?REPLACE -> <<(R#request.flags):32,(R#request.expires):32>>;
    ?REPLACEQ -> <<(R#request.flags):32,(R#request.expires):32>>;
    ?INCREMENT -> <<(R#request.delta):64,(R#request.initial):64,(R#request.expires):32>>;
    ?INCREMENTQ -> <<(R#request.delta):64,(R#request.initial):64,(R#request.expires):32>>;
    ?DECREMENT -> <<(R#request.delta):64,(R#request.initial):64,(R#request.expires):32>>;
    ?DECREMENTQ -> <<(R#request.delta):64,(R#request.initial):64,(R#request.expires):32>>;
    ?FLUSH -> <<(R#request.expires):32>>;
    ?FLUSHQ -> <<(R#request.expires):32>>;
    _ -> <<>>
  end.

read_stats(Opaque, T) ->
  case(H = read()) of
    { ?STAT, 0, <<>>, <<>>, _, _, Opaque } ->
      lists:reverse(T);
    { ?STAT, 0, Key, Value, _, _, Opaque } ->
      read_stats(Opaque, [ { binary_to_list(Key), binary_to_list(Value) } ]);
    { ?STAT, Status, _, _, _, Opaque } ->
      [ { "error", integer_to_list(Status) } ];
    _ ->
      %% async error from a previous silent command - error handler pid?
      io:format("asynch error ~p~n",[H]),
      read_stats(Opaque, T)
  end.

read_response(Opaque) ->
  case(H = read()) of
    { _, _, _, _, _, _, Opaque } ->
      format_response(H);
    _ ->
      %% async error from a previous silent command - error handler pid?
      io:format("asynch error ~p~n",[H]),
      read_response(Opaque)
  end.

read_responses(Opaque, Terminal, T) ->
  case(H = read()) of
    { _, 0, _, _, _, _, Terminal } ->
      lists:reverse([ format_response(H) | T]);
    { _, _, _, _, _, _, Terminal } ->
      lists:reverse(T);
    { _, 0, _, _, _, _, Opaque } ->
      read_responses(Opaque, Terminal, [ format_response(H) | T ]);
    { _, _, _, _, _, _, Opaque } ->
      read_responses(Opaque, Terminal, T);
    _ ->
      %% async error from a previous silent command - error handler pid?
      io:format("asynch error ~p~n",[H]),
      read_responses(Opaque, Terminal, T)
  end.

%% bxor?
raw(Opcode, _Opaque, Data) when (Opcode > ?STAT) or (Opcode =:= ?GETQ) or (Opcode =:= ?GETKQ) ->
  send(Data), [];
raw(_Opcode, Opaque, Data) ->
  send(Data),
  raw_read_until_opaque(Opaque, []).

raw_read_until_opaque(Opaque, T) ->
  case raw_read() of
    { _, _, Opaque, Data } -> lists:reverse([ Data | T ]);
    { _, _, _, Data } -> raw_read_until_opaque(Opaque, [ Data | T ])
  end.

read() ->
  Head = recv(24),
  <<129,Opcode:8,KeyLength:16,ExtrasLength:8,_DataType:8,Status:16,TotalBodyLength:32,Opaque:32,Cas:64>> = Head,
  ExtrasBin = recv(ExtrasLength),
  ExtrasBits = ExtrasLength * 8,
  <<Extras:ExtrasBits>> = ExtrasBin,
  Key = recv(KeyLength),
  Value = recv(TotalBodyLength - ExtrasLength - KeyLength),
  { Opcode, Status, Key, Value, Extras, Cas, Opaque }.

raw_read() ->
  Head = recv(24),
  <<129,Opcode:8,KeyLength:16,ExtrasLength:8,_DataType:8,Status:16,TotalBodyLength:32,Opaque:32,_Cas:64>> = Head,
  Extras = recv(ExtrasLength),
  Key = recv(KeyLength),
  Value = recv(TotalBodyLength - ExtrasLength - KeyLength),
  { Opcode, Status, Opaque, [ Head, Extras, Key, Value ] }.

format_response( { _Opcode, Status, Key, Value, Extras, Cas, _Opaque } ) ->
  case Status of
    0 -> { ok, [ Value, Key, Extras, Cas ] };
    1 -> { error, key_not_found };
    2 -> { error, key_exists };
    3 -> { error, value_too_big };
    4 -> { error, invalid_arguments };
    5 -> { error, item_not_stored };
    128 -> { error, unknown_command }
  end.

send(Data) ->
  %io:format("SEND: ~p\n",[Data]),
  case gen_tcp:send(Socket, Data) of
    ok ->
      ok;
    _ ->
      gen_tcp:close(Socket),
      exit(normal)
  end.

recv(0) ->
  <<"">>;
recv(Size) ->
  case gen_tcp:recv(Socket, Size, 1000) of
    { ok, Data } ->
      %io:format("RECV: ~p\n",[Data]),
      Data;
    Error ->
      io:format("read error ~p~n",[Error]),
      gen_tcp:close(Socket),
      exit(normal)
  end.

l(X) when is_binary(X) ->
  size(X);
l(X) when is_list(X) ->
  length(X).

gen_opaque() ->
  { _, _, O } = now(),
  O.

write(R) ->
  Extras = gen_extras(R),
  KeyLength = l(R#request.key),
  ExtrasLength = l(Extras),
  TotalBodyLength = KeyLength + ExtrasLength + l(R#request.value),
  Command = [
    <<128>>,
    <<(R#request.opcode):8>>,
    <<KeyLength:16>>,
    <<ExtrasLength:8>>,
    <<(R#request.data_type):8>>,
    <<(R#request.reserved):16>>,
    <<TotalBodyLength:32>>,
    <<(R#request.opaque):32>>,
    <<(R#request.cas):64>>,
    Extras,
    R#request.key,
    R#request.value ],
  send(Command).
