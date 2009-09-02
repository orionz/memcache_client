-module(memcache_binary_tcp_connection, [ Host, Port, Socket ] ).

-include_lib("memcache.hrl").

-define(TCP_OPTIONS, [binary, {packet, 0}, {active, false}]).

-export([write/1, read/0 ]).
-export([send_request/1 ]).
-export([raw/1 ]).

send_request(R) when R#request.opcode > ?STAT ->
  write(R),
  read();
send_request(R) when R#request.opcode == ?STAT ->
  write(R),
  read_stats([]);
send_request(R) ->
  write(R),
  read().

read_stats(Stats) ->
  case(read()) of
    { ok, [ <<>>, <<>>, _, _ ] } ->
      lists:reverse(Stats);
    { ok, [ Value, Key, _, _ ] } ->
      read_stats([ { binary_to_list(Key), binary_to_list(Value) } | Stats]);
    Error ->
      Error
  end.

gen_extras(R) ->
  case R#request.opcode of
    ?SET -> <<(R#request.flags):32,(R#request.expires):32>>;
    ?ADD -> <<(R#request.flags):32,(R#request.expires):32>>;
    ?REPLACE -> <<(R#request.flags):32,(R#request.expires):32>>;
    ?INCREMENT -> <<(R#request.delta):64,(R#request.initial):64,(R#request.expires):32>>;
    ?DECREMENT -> <<(R#request.delta):64,(R#request.initial):64,(R#request.expires):32>>;
    ?FLUSH -> <<(R#request.expires):32>>;
    _ -> <<>>
  end.

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

raw_read() ->
  Head = recv(24),
  <<129,_Opcode:8,KeyLength:16,ExtrasLength:8,_DataType:8,_Status:16,TotalBodyLength:32,_Opaque:32,_Cas:64>> = Head,
  Extras = recv(ExtrasLength),
  Key = recv(KeyLength),
  Value = recv(TotalBodyLength - ExtrasLength - KeyLength),
  [ Head, Extras, Key, Value ].

read() ->
  [ Head, ExtrasBin, Key, Value ] = raw_read(),
  <<129,_Opcode:8,_KeyLength:16,ExtrasLength:8,_DataType:8,Status:16,_TotalBodyLength:32,_Opaque:32,Cas:64>> = Head,
  %% fixme - try <<A:B/binary>> to avoid the * 8
  ExtrasBits = ExtrasLength * 8,
  <<Extras:ExtrasBits>> = ExtrasBin,
  case Status of
    0 -> { ok, [ Value, Key, Extras, Cas ] };
    1 -> { error, key_not_found };
    2 -> { error, key_exists };
    3 -> { error, value_too_big };
    4 -> { error, invalid_arguments };
    5 -> { error, item_not_stored };
    128 -> { error, unknown_command }
  end.

raw(Data) ->
  send(Data),
  raw_read().

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

