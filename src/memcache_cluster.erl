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

start() ->
  spawn(memcache_cluster,loop,[{ [], cache_hash:empty()}]).

loop(Cons) when is_tuple(Cons) ->
  receive
    { raw , Pid, Opcode, Key, Data } ->
%      io:format("GOT RAW ~p,~p,~p~n",[Opcode,Key,Data]),
      Pid ! { raw_response, raw( Cons, Opcode, Key, Data) },
      loop(Cons);
    { request, Pid, Request } ->
%      io:format("GOT REQUEST ~p,~p~n",[Pid,Request]),
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

