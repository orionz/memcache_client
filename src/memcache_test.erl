-module(memcache_test).

-define(TCP_BINARY_OPTIONS, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]).
-define(TCP_TEXT_OPTIONS, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]).
-define(UDP_OPTIONS, [binary, {active, false} ]).

-define(SLUG_SIZE, 1024 * 1023).
-define(NUM_TESTS, 1000).

-define(FAST, true).
%-define(SLUG_SIZE, 10).
%-define(NUM_TESTS, 10000).
-define(DEFAULT_PORT, 11211).

-export([test/0,test_mnemosyne/0,benchmark/1]).

test() ->
  inets:start(),
  Cache = memcache:open(text,"127.0.0.1",?DEFAULT_PORT),
  test(Cache,"Text Test"),
  BinCache = memcache:open(binary,"127.0.0.1",?DEFAULT_PORT),
  test(BinCache,"Binary Test"),
  ok.

test_mnemosyne() ->
  inets:start(),
  io:format("connecting to mnemosyne/bin\n"),
%  BinCache = memcache:open(binary,"127.0.0.1",?DEFAULT_PORT),
%  io:format("set -> k1\n"),
%  memcache:set(BinCache, "k1", "xxxyyyzzz"),
%  io:format("get -> k1\n"),
%  io:format("connecting to mnemosyne/text\n"),
%  {ok,<<"xxxyyyzzz">>,_,_} = memcache:get(BinCache,"k1"),
  Cache = memcache:open(text,"127.0.0.1",?DEFAULT_PORT),
  io:format("set -> k1\n"),
  memcache:set(Cache, "k1", "xxxyyyzzz"),
  io:format("get -> k1\n"),
  {ok,<<"xxxyyyzzz">>,_,_} = memcache:get(Cache,"k1"),
  io:format("All done!\n"),
  ok.

benchmark(Type) ->
  inets:start(),
%  Cache = Cache = memcache:open(Type,"127.0.0.1",?DEFAULT_PORT),
  Cache = Cache = memcache:open(Type,"127.0.0.1",?DEFAULT_PORT),
  loop(Cache, 0, data()),
  ok.

data() ->
  data(0,[]).
data(?SLUG_SIZE,List) ->
  list_to_binary(List);
data(N,List) ->
  data(N+1,[ "x" | List ]).

loop(_Cache, ?NUM_TESTS, _Data) ->
  ok;
loop(Cache, Num, Data) ->
  Cache:set("Hello", Data),
  Cache:get("Hello"),
  loop(Cache, Num+1, Data).

test(C,TestName) ->
  io:format("-----------------------------------------\n"),
  io:format("Testing: ~p~n",[TestName]),
  io:format("On connection: ~p~n",[C]),

  [ { ok, Version } ] = memcache:version(C),
  io:format("Memcache V:~p~n",[ binary_to_list(Version) ]),

  [ Stat ] = memcache:stat(C),
  io:format("stat(pid) -> ~p~n",[lists:keysearch("pid",1,Stat)]),

  [ ok ] = memcache:flush(C),

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

  io:format("get -> ok~n"),
  { ok, <<"v3">>, _, _ } = memcache:get(C,"k1"),

  io:format("replace -> ok~n"),
  { ok, _ } = memcache:replace(C,"k1","v4"),

  io:format("get -> ok~n"),
  { ok, <<"v4">>, _, _ } = memcache:get(C,"k1"),

  io:format("add -> key_exists/item_not_stored~n"), % difference between bin/text
  {error,_} = memcache:add(C,"k1","v5"),

  io:format("get -> ok~n"),
  { ok, <<"v4">>, _, Cas } = memcache:get(C,"k1"),

  io:format("cas v4.1/~p -> ok~n",[Cas]),
  { ok, _ } = memcache:cas(C,"k1","v4.1",Cas),

  io:format("cas v4.2/~p -> ok~n",[Cas]),
  {error,key_exists} = memcache:cas(C,"k1","v4.2",Cas),

  io:format("get -> ok~n"),
  { ok, <<"v4.1">>, _, _ } = memcache:get(C,"k1"),

  io:format("flush -> ok~n"),
  [ ok ] = memcache:flush(C),

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

  io:format("----------- DONE! -----------~n").

