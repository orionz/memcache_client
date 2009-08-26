-module(cache_hash).

-export([empty/0, add/2, add/3, remove/2, lookup/2]).
-export([test/0]).
-define(DEFAULT_WEIGHT, 200).

-compile([export_all]).

empty() ->
  gb_trees:empty().

add(Object, Data) ->
  add(Object, ?DEFAULT_WEIGHT, Data).

add(Object, Weight, Data) when is_integer(Weight), Weight > 100 ->
  add_key(Object, Weight, Data).

add_key(_Object, 0, Data) ->
  gb_trees:balance(Data);
add_key(Object, N, Data) ->
  Key = erlang:phash2( { N, Object }),
  case gb_trees:lookup(Key, Data) of
    { value, _ } -> 
      add_key(Object, N - 1, gb_trees:update(Key, Object, Data));
    none ->
      add_key(Object, N - 1, gb_trees:insert(Key, Object, Data))
  end.

remove(Object, Data) ->
  remove(Object, gb_trees:to_list(Data), Data).

remove(_Object, [], Data) ->
  gb_trees:balance(Data);
remove(Object, [ { Key, Object } | Tail ], Data) ->
  remove(Object, Tail, gb_trees:delete(Key, Data));
remove(Object, [ { _Key, _Value } | Tail ], Data) ->
  remove(Object, Tail, Data).

tree_search(_Needle, Last, nil ) ->
  Last;
tree_search(Needle, _Last, { Key, Value, _Tree1, Tree2 } ) when Needle > Key ->
  tree_search(Needle, Value, Tree2 );
tree_search(Needle, _Last, { Key, Value, Tree1, _Tree2 } ) when Needle < Key ->
  tree_search(Needle, Value, Tree1 );
tree_search(Needle, _, { Key, Value, _, _ } ) when Needle == Key ->
  Value.

lookup(Key, { _Size, Tree  } ) ->
  Needle = erlang:phash2( Key ),
  tree_search(Needle, nil, Tree).

test() ->
  H1 = empty(),
  H2 = add({ o, 1 }, H1),
  H3 = add({ o, 2 }, H2),
  H4 = add({ o, 3 }, H3),
  test(100,H4),
  io:format("size: ~p~n",[gb_trees:size(H4)]),
  H5 = remove( { o, 1 }, H4 ),
  io:format("size: ~p~n",[gb_trees:size(H5)]),
  H6 = remove( { o, 2 }, H5 ),
  io:format("size: ~p~n",[gb_trees:size(H6)]),
  H7 = remove( { o, 3 }, H6 ),
  io:format("size: ~p~n",[gb_trees:size(H7)]),
  ok.

test(0, _) ->
  ok;
test(N, H) ->
  io:format("test ~p -> ~p~n",[ N, lookup({ foo, N }, H) ]),
  test(N-1,H).
