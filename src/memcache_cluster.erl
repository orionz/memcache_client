-module(memcache_cluster).

-include_lib("memcache.hrl").

-define(TCP_BINARY_OPTIONS, [binary, {packet, 0}, {active, false}, {reuseaddr, true}, {nodelay, true} ]).
-define(TCP_TEXT_OPTIONS, [binary, {packet, 0}, {active, false}, {reuseaddr, true}, {nodelay, true} ]).

% this is too small - find the right size
% {recbuf, 8192}]

-export([start/0]).
-export([loop/1]).
-export([close/1]).
-export([disconnect/3]).
-export([connect/4]).

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

%   0x00    Get
%   0x01    Set
%   0x02    Add
%   0x03    Replace
%   0x04    Delete
%   0x05    Increment
%   0x06    Decrement
%   0x07    Quit
%   0x08    Flush
%   0x09    GetQ
%   0x0A    No-op
%   0x0B    Version
%   0x0C    GetK
%   0x0D    GetKQ
%   0x0E    Append
%   0x0F    Prepend
%   0x10    Stat

start() ->
  spawn(memcache_cluster,loop,[{ [], cache_hash:empty()}]).

loop(Cons) when is_tuple(Cons) ->
  receive
    { raw , Pid, Opcode, Key, Data } ->
      Pid ! { raw_response, raw( Cons, Opcode, Key, Data) },
      loop(Cons);
    { request, Pid, Request } ->
      Pid ! { response, request( Cons, Request) },
      loop(Cons);
    { disconnect, Ip, Port } ->
      loop( remove_cons( Cons, lookup_cons( Cons, Ip, Port)));
    { connect, Type, Ip, Port } ->
      loop(add_con(Cons, start_connection(Type, { Ip, Port })));
    { close, Pid } ->
      Pid ! { closed },
      exit(0);
    Other ->
      exit({ bad_msg, Other})
  end.

lookup_cons({ ConList, _ }, Ip, Port) ->
  [ { T, I, P, S } || { T, I, P, S } <- ConList, I == Ip, P == Port ].

add_con( { ConList, ConHash }, Con ) ->
  { [ Con | ConList ], cache_hash:add(Con, ConHash) }.

remove_cons( { ConList, ConHash }, [] ) ->
  { ConList, ConHash };
remove_cons( { ConList, ConHash }, [ Con | Tail ] ) ->
  remove_cons({ lists:delete(Con, ConList),  cache_hash:remove(Con, ConHash) }, Tail).

%% Tasks:
%%    route based on prefix
%%    AMQP -> Clouds
%%    Synch new cluster members on exact time

close(Con) ->
  Con ! { close, self() },
  receive
    { closed } -> ok
  end.

connect(Con, Type, Ip, Port) ->
  Con ! { connect, Type, Ip, Port }.

disconnect(Con, Ip, Port) ->
  Con ! { disconnect, Ip, Port }.

start_connection(binary, { Host, Port } ) ->
  case gen_tcp:connect(Host, Port, ?TCP_BINARY_OPTIONS) of
    { ok, Socket } ->
      { memcache_binary_tcp_connection, Host, Port, Socket };
    { error, Reason } ->
      { error, Reason }
  end;
start_connection(text, { Host, Port }) ->
  case gen_tcp:connect(Host, Port, ?TCP_TEXT_OPTIONS) of
    { ok, Socket } ->
      { memcache_text_tcp_connection, Host, Port, Socket };
    { error, Reason } ->
      { error, Reason }
  end.

raw({ ConList, _ }, Opcode, _Key, Data) when (Opcode == ?STAT) or (Opcode == ?FLUSH) ->
  [ C:raw(Data) || C <- ConList  ];
raw({ _, ConHash }, _Opcode, Key, Data) ->
  (cache_hash:lookup(Key, ConHash)):raw(Data).

%% requests with no key size (like FLUSH) should be sent to all backends - but need only return one response
request({ ConList, _ }, R) when (R#request.opcode == ?FLUSH) or (R#request.opcode == ?STAT) or (R#request.opcode == ?VERSION) ->
  [ C:send_request(R) || C <- ConList ];
request( { _, ConHash }, R) ->
  (cache_hash:lookup(R#request.key, ConHash)):send_request(R).

