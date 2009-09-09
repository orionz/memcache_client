-module(memcache_test).

-define(TCP_BINARY_OPTIONS, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]).
-define(TCP_TEXT_OPTIONS, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]).
-define(UDP_OPTIONS, [binary, {active, false} ]).

-define(FAST, true).
-define(DEFAULT_CACHE, [{ "127.0.0.1", 11211 }] ).

-export([test/0]).

test() ->
  inets:start(),
  Cache = memcache:open( ?DEFAULT_CACHE, [ text ]),
  test(Cache,"Text Test"),
  BinCache = memcache:open( ?DEFAULT_CACHE, [ binary ]),
  test(BinCache,"Binary Test"),
  ok.

test(C,TestName) ->
  io:format("----------------- START -----------------\n"),
  io:format("Testing: ~p~n",[TestName]),
  io:format("On connection: ~p~n",[C]),

  [ { ok, Version } | _ ] = memcache:version(C),
  io:format("Memcache V:~p~n",[ binary_to_list(Version) ]),

  [ Stat | _ ] = memcache:stat(C),
  io:format("stat(pid) -> ~p~n",[lists:keysearch("pid",1,Stat)]),

  [ ok | _ ] = memcache:flush(C),

  io:format("get -> key_not_found~n"),
  {error, key_not_found} = memcache:get(C,"k1"),

  io:format("set -> ok~n"),    memcache:set(C,"KEY001","v1"),
  io:format("get -> value~n"), { ok, <<"v1">>, _, _} = memcache:get(C,"KEY001"),
  io:format("set -> ok~n"),    memcache:set(C,"KEY002","v1"),
  io:format("get -> value~n"), { ok, <<"v1">>, _, _} = memcache:get(C,"KEY002"),
  io:format("set -> ok~n"),    memcache:set(C,"KEY003","v1"),
  io:format("get -> value~n"), { ok, <<"v1">>, _, _} = memcache:get(C,"KEY003"),
  io:format("set -> ok~n"),    memcache:set(C,"KEY004","v1"),
  io:format("get -> value~n"), { ok, <<"v1">>, _, _} = memcache:get(C,"KEY004"),
  io:format("set -> ok~n"),    memcache:set(C,"KEY005","v1"),
  io:format("get -> value~n"), { ok, <<"v1">>, _, _} = memcache:get(C,"KEY005"),
  io:format("set -> ok~n"),    memcache:set(C,"KEY006","v1"),
  io:format("get -> value~n"), { ok, <<"v1">>, _, _} = memcache:get(C,"KEY006"),
  io:format("set -> ok~n"),    memcache:set(C,"KEY007","v1"),
  io:format("get -> value~n"), { ok, <<"v1">>, _, _} = memcache:get(C,"KEY007"),
  io:format("set -> ok~n"),    memcache:set(C,"KEY008","v1"),
  io:format("get -> value~n"), { ok, <<"v1">>, _, _} = memcache:get(C,"KEY008"),
  io:format("set -> ok~n"),    memcache:set(C,"KEY009","v1"),
  io:format("get -> value~n"), { ok, <<"v1">>, _, _} = memcache:get(C,"KEY009"),

  io:format("mgetk -> value~n"),
  {ok,[
  {<<"KEY001">>,<<"v1">>,_,_},
  {<<"KEY002">>,<<"v1">>,_,_},
  {<<"KEY003">>,<<"v1">>,_,_},
  {<<"KEY009">>,<<"v1">>,_,_}]} = memcache:mgetk(C,[ "FOOBAR", "KEY001", "KEY002", "KEY003", "KEY009" ]),

  %% --- NOREPLY BATTERY ---

  memcache:flushq(C),

  io:format("get -> key_not_found~n"),
  {error, key_not_found} = memcache:get(C,"k1"),

  io:format("setq -> ok~n"),    memcache:setq(C,"KEY001","v1"),
  io:format("get -> value~n"),  X1 = memcache:get(C,"KEY001"),
  io:format("X1 ~p~n",[X1]),
  io:format("get -> value~n"),  X2 = memcache:get(C,"KEY001"),
  io:format("X2 ~p~n",[X2]),
  io:format("get -> value~n"), { ok, <<"v1">>, _, _} = memcache:get(C,"KEY001"),
  io:format("setq -> ok~n"),    memcache:setq(C,"KEY002","v1"),
  io:format("get -> value~n"), { ok, <<"v1">>, _, _} = memcache:get(C,"KEY002"),

  io:format("setq -> ok~n"),
  ok = memcache:setq(C,"k1","v1"),

  io:format("get -> value~n"),
  { ok, <<"v1">>, _, _} = memcache:get(C,"k1"),

  io:format("deleteq -> ok~n"),
  ok = memcache:deleteq(C,"k1"),

  io:format("get -> key_not_found~n"),
  {error, key_not_found} = memcache:get(C,"k1"),

  io:format("replaceq -> key_not_found / item_not_stored~n"), % difference between bin/text
  ok = memcache:replaceq(C,"k1","v2"),

  io:format("get -> key_not_found~n"),
  {error,key_not_found} = memcache:get(C,"k1"),

  io:format("addq -> ok~n"),
  ok = memcache:addq(C,"k1","v3"),

  io:format("get -> ok~n"),
  { ok, <<"v3">>, _, _ } = memcache:get(C,"k1"),

  io:format("replaceq -> ok~n"),
  ok = memcache:replaceq(C,"k1","v4"),

  io:format("get -> ok~n"),
  { ok, <<"v4">>, _, _ } = memcache:get(C,"k1"),

  io:format("addq -> key_exists/item_not_stored~n"), % difference between bin/text
  ok = memcache:addq(C,"k1","v5"),

  io:format("get -> ok~n"),
  { ok, <<"v4">>, _, Cas } = memcache:get(C,"k1"),

  io:format("casq v4.1/~p -> ok~n",[Cas]),
  ok = memcache:casq(C,"k1","v4.1",Cas),

  io:format("casq v4.2/~p -> ok~n",[Cas]),
  ok = memcache:casq(C,"k1","v4.2",Cas),

  io:format("get -> ok~n"),
  { ok, <<"v4.1">>, _, _ } = memcache:get(C,"k1"),

  io:format("setq -> ok~n"),
  memcache:setq(C,"k1","00"),

  io:format("appendq -> ok~n"),
  ok = memcache:appendq(C,"k1","zzz"),

  io:format("prependq -> ok~n"),
  ok = memcache:prependq(C,"k1","aaa"),

  io:format("get -> ok~n"),
  { ok, <<"aaa00zzz">>, _, _ } = memcache:get(C,"k1"),

  memcache:deleteq(C,"III"), %% if we dont flush I have to do this...

  io:format("addq -> ok~n"),
  ok = memcache:addq(C,"III","100"),

  io:format("incrementq -> ok~n"),
  ok = memcache:incrementq(C,"III",10),

  io:format("decrement -> ok~n"),
  ok = memcache:decrementq(C,"III",5),

  io:format("get -> ok~n"),
  { ok, <<"105">>, _, _ } = memcache:get(C,"III"),

  ok = memcache:flushq(C),

  io:format("NORMAL BATTERY --- ~n"),

  io:format("set -> ok~n"),
  memcache:set(C,"k1","v1"),
  %io:format("set -> ~s~n",[X]),

  io:format("get -> value~n"),
  { ok, <<"v1">>, _, _} = memcache:get(C,"k1"),

  io:format("getk -> key/value~n"),
  R = memcache:getk(C,"k1"),
  io:format("hint ~p~n",[R]),
  { ok, <<"k1">>, <<"v1">>, _, _ } = memcache:getk(C,"k1"),

  io:format("delete -> ok~n"),
  ok = memcache:delete(C,"k1"),

  io:format("get -> key_not_found~n"),
  {error, key_not_found} = memcache:get(C,"k1"),

  io:format("replace -> key_not_found / item_not_stored~n"), % difference between bin/text
  {error, _ } = memcache:replace(C,"k1","v2"),

  io:format("get -> key_not_found~n"),
  {error,key_not_found} = memcache:get(C,"k1"),

  io:format("add -> ok~n"),
  { ok, _ } = memcache:add(C,"k1","v3"),

  io:format("get -> /v3/ ok~n"),
  { ok, <<"v3">>, _, _ } = memcache:get(C,"k1"),

  io:format("replace -> ok~n"),
  { ok, _ } = memcache:replace(C,"k1","v4"),

  io:format("get -> ok~n"),
  { ok, <<"v4">>, _, _ } = memcache:get(C,"k1"),

  io:format("add -> key_exists/item_not_stored~n"), % difference between bin/text
  {error,_} = memcache:add(C,"k1","v5"),

  X = memcache:get(C,"k1"),
  io:format("get -> ~p~n",[X]),
  { ok, <<"v4">>, _, Cas2 } = memcache:get(C,"k1"),

  io:format("cas v4.1/~p -> ok~n",[Cas2]),
  { ok, _ } = memcache:cas(C,"k1","v4.1",Cas2),

  io:format("cas v4.2/~p -> ok~n",[Cas2]),
  {error,key_exists} = memcache:cas(C,"k1","v4.2",Cas2),

  io:format("get -> ok - v4.1~n"),
  { ok, <<"v4.1">>, _, _ } = memcache:get(C,"k1"),

  io:format("flush -> ok~n"),
  [ ok | _ ] = memcache:flush(C),

  io:format("get -> key_not_found~n"),
  {error,key_not_found} = memcache:get(C,"k1"),

  io:format("set w/ flags -> ok~n"),
  memcache:set(C,"k1","v1",1234,0),

  io:format("get w/ flags -> ok~n"),
  { ok, <<"v1">>, 1234, _ } = memcache:get(C,"k1"),

  case ?FAST of
    true -> ok;
    false ->
      io:format("set w/ expire -> ok~n"),
      memcache:set(C,"k1","v1",0,1),

      io:format("get under expire -> ok~n"),
      { ok, <<"v1">>, _, _ } = memcache:get(C,"k1"),

      io:format("get over expire -> ok~n"),
      timer:sleep(2000),
      {error,key_not_found} = memcache:get(C,"k1")
  end,

  io:format("set -> ok~n"),
  memcache:set(C,"k1","00"),

  io:format("append -> ok~n"),
  ok = memcache:append(C,"k1","zzz"),

  io:format("prepend -> ok~n"),
  ok = memcache:prepend(C,"k1","aaa"),

  io:format("get -> ok~n"),
  { ok, <<"aaa00zzz">>, _, _ } = memcache:get(C,"k1"),

  memcache:delete(C,"III"), %% if we dont flush I have to do this...

  io:format("add -> ok~n"),
  { ok, _ } = memcache:add(C,"III","100"),

  io:format("increment -> ok~n"),
  { ok, 110 } = memcache:increment(C,"III",10),

  io:format("decrement -> ok~n"),
  { ok, 105 } = memcache:decrement(C,"III",5),

  io:format("increment -> ok~n"),
  { ok, 12 } = memcache:increment(C,"XIII", 12, 2),
  io:format("increment -> ok~n"),
  { ok, 14 } = memcache:increment(C,"XIII", 12, 2),

  memcache:close(C),

  io:format("----------------- DONE! -----------------\n").

