-module(memcache_binary_tcp_connection, [ Host, Port, Socket ] ).

-include_lib("memcache.hrl").

-define(TCP_OPTIONS, [binary, {packet, 0}, {active, false}]).

-export([write/1, read/0 ]).
-export([send_request/1 ]).
-export([raw/1 ]).

%-define(GET,       16#00).
%-define(SET,       16#01).
%-define(ADD,       16#02).
%-define(REPLACE,   16#03).
%-define(DELETE,    16#04).
%-define(INCREMENT, 16#05).
%-define(DECREMENT, 16#06).
%-define(QUIT,      16#07).
%-define(FLUSH,     16#08).
%-define(GETQ,      16#09).
%-define(NOOP,      16#0a).
%-define(VERSION,   16#0b).
%-define(GETK,      16#0c).
%-define(GETKQ,     16#0d).
%-define(APPEND,    16#0e).
%-define(PREPEND,   16#0f).
%-define(STAT,      16#10).
-define(GET, 0).
-define(SET, 1).
-define(ADD, 2).
-define(REPLACE, 3).
-define(DELETE, 4).
-define(INCREMENT, 5).
-define(DECREMENT, 6).
-define(QUIT, 7).
-define(FLUSH, 8).
-define(GETQ, 9).
-define(NOOP, 10).
-define(VERSION, 11).
-define(GETK, 12).
-define(GETKQ, 13).
-define(APPEND, 14).
-define(PREPEND, 15).
-define(STAT, 16).

send_request(R) when R#request.opcode > ?STAT ->
  write(R#request { opcode = (R#request.opcode - ?STAT) });
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
  case gen_tcp:recv(Socket, Size) of
    { ok, Data } ->
      %io:format("RECV: ~p\n",[Data]),
      Data;
    _ ->
      gen_tcp:close(Socket),
      exit(normal)
  end.

l(X) when is_binary(X) ->
  size(X);
l(X) when is_list(X) ->
  length(X).

