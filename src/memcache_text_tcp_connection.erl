-module(memcache_text_tcp_connection, [ Host, Port, Socket ] ).

-include_lib("memcache.hrl").

-define(TCP_OPTIONS, [list, {packet, 0}, {active, false}]).

-export([]).
-export([send_request/1, raw/1 ]).

send_request(R) ->
  write(R).

read_line() ->
  inet:setopts(Socket, [{packet, line}]),
  case gen_tcp:recv(Socket, 0) of
    {ok, Line} ->
      %io:format("Line: ~p~n",[Line]),
      inet:setopts(Socket, [{packet, raw}]),
      binary_to_list(chomp(Line));
    _ ->
      exit(normal)
  end.

chomp(Line) when is_binary(Line) ->
  Size = size(Line) - 2,
  case Line of
    <<Chunk:Size/binary,"\r\n">> -> Chunk;
    _ -> Line
  end.

words(Line) ->
  string:tokens(Line," ").

read_words() ->
  words(read_line()).

write(R=#request{ opcode=?GET }) ->
  base_get(R);
write(R=#request{ opcode=?GETK }) ->
  base_get(R);
write(R=#request{ opcode=?SET }) when R#request.cas > 0 ->
  base_set("cas", R);
write(R=#request{ opcode=?SET }) ->
  base_set("set", R);
write(R=#request{ opcode=?REPLACE }) ->
  base_set("replace", R);
write(R=#request{ opcode=?ADD }) ->
  base_set("add", R);
write(R=#request{ opcode=?APPEND }) ->
  base_set("append", R);
write(R=#request{ opcode=?PREPEND }) ->
  base_set("prepend", R);
write(R) when R#request.initial > 0 ->  %% only increment and decrement
  case write( R#request{ opcode=?ADD, value=integer_to_list(R#request.initial) }) of
    { ok, _ } ->
      { ok, [ <<(R#request.initial):64>>, 0, 0, 0] };
    _ ->
      write(R#request { initial=0 })
  end;
write(R=#request{ opcode=?INCREMENT }) ->
  send(["incr ", R#request.key, " ", integer_to_list(R#request.delta), "\r\n"]),
  case read_line() of
    "NOT_FOUND" -> { error, key_not_found };
    Value -> { ok, [ <<(list_to_integer(Value)):64>>, 0, 0, 0 ] }
  end;
write(R=#request{ opcode=?DECREMENT }) ->
  send(["decr ", R#request.key, " ", integer_to_list(R#request.delta), "\r\n"]),
  case read_line() of
    "NOT_FOUND" -> { error, key_not_found };
    Value -> { ok, [ <<(list_to_integer(Value)):64>>, 0, 0, 0 ] }
  end;
write(R=#request{ opcode=?DELETE }) ->
  send(["delete ", R#request.key, "\r\n"]),
  case read_line() of
    "DELETED"  -> { ok , [ 0, 0, 0, 0] };
    "NOT_FOUND" -> { error, key_not_found }
  end;
write(R=#request{ opcode=?SETQ }) when R#request.cas > 0 ->
  base_setq("cas", R);
write(R=#request{ opcode=?SETQ }) ->
  base_setq("set", R);
write(R=#request{ opcode=?REPLACEQ }) ->
  base_setq("replace", R);
write(R=#request{ opcode=?ADDQ }) ->
  base_setq("add", R);
write(R=#request{ opcode=?APPENDQ }) ->
  base_setq("append", R);
write(R=#request{ opcode=?PREPENDQ }) ->
  base_setq("prepend", R);
write(R=#request{ opcode=?INCREMENTQ }) ->
  send(["incr ", R#request.key, " ", integer_to_list(R#request.delta), " noreply\r\n"]);
write(R=#request{ opcode=?DECREMENTQ }) ->
  send(["decr ", R#request.key, " ", integer_to_list(R#request.delta), " noreply\r\n"]);
write(R=#request{ opcode=?DELETEQ }) ->
  send(["delete ", R#request.key, " noreply\r\n"]);
write(R=#request{ opcode=?FLUSHQ }) ->
  send(["flush_all ", integer_to_list(R#request.expires), " noreply\r\n"]);
write(_R=#request{ opcode=?VERSION }) ->
  send("version\r\n"),
  case read_words() of
    [ "VERSION", V ] -> { ok, [ list_to_binary(V), 0, 0, 0 ] };
    Error -> { error, Error }
  end;
write(_R=#request{ opcode=?STAT }) ->
  send("stats\r\n"),
  read_stats([]);
write(R=#request{ opcode=?FLUSH }) ->
  send(["flush_all ", integer_to_list(R#request.expires), "\r\n"]),
  case read_line() of
    "OK" -> { ok, [ 0, 0, 0, 0 ] };
    Error -> { error, Error }
  end;
write(_R=#request{ opcode=?NOOP }) ->
  { ok, not_implemented };
write(_R=#request{ opcode=?QUITQ }) ->
  send("quit\r\n"),
  exit(normal);
write(_R=#request{ opcode=?QUIT }) ->
  send("quit\r\n"),
  exit(normal).

%base_get(R) when R#request.num_keys =:= 1 ->
%  send(["gets ", R#request.key, "\r\n"]),
%  read_many([]);
base_get(R) ->
  send(["gets ", string:join(R#request.key," "), "\r\n"]),
  read_many([]).

read_many(T) ->
  case read_words() of
    [ "VALUE", Key, Flags, Bytes, Cas ] ->
      Value = recv(list_to_integer(Bytes)),
      read_many([ { ok, [ Value, list_to_binary(Key), list_to_integer(Flags), list_to_integer(Cas) ] } | T ]);
    [ "END" ] ->
      lists:reverse(T);
    [ "ERROR" ] ->
      read_many(T);
    [] -> %% keep reading
      read_many(T)
  end.

base_setq(Cmd, R) ->
  send([Cmd, " ", R#request.key, " ", integer_to_list(R#request.flags), " ", integer_to_list(R#request.expires), " ", integer_to_list(l(R#request.value)), cas_text(Cmd, R)," noreply\r\n", R#request.value, "\r\n"]).

base_set(Cmd, R) ->
  %% this needs to send the cas value, duh
  send([Cmd, " ", R#request.key, " ", integer_to_list(R#request.flags), " ", integer_to_list(R#request.expires), " ", integer_to_list(l(R#request.value)), cas_text(Cmd, R),"\r\n", R#request.value, "\r\n"]),
  case read_line() of
    "STORED"  -> { ok, [ 0, 0, 0, 0 ] };
    "NOT_STORED" -> { error, item_not_stored };
    "EXISTS" -> { error, key_exists };
    "NOT_FOUND" -> { error, key_not_found }
  end.

cas_text("cas", R) ->
  [" ", integer_to_list(R#request.cas) ];
cas_text(_, _) ->
  "".

send(Data) ->
  case gen_tcp:send(Socket, Data) of
    ok ->
      ok;
    _ ->
      gen_tcp:close(Socket),
      exit(normal)
  end.

raw(Data) ->
  send(Data),
  raw_read().

% STAT <name> <value>\r\n
% ...
% END\r\n
read_stats(Stats) ->
  case words(read_line()) of
    [ "END" ] ->
      lists:reverse(Stats);
    [ "STAT", Key, Value ] ->
      read_stats([ { Key, Value } | Stats ])
  end.

raw_read() ->
  Line = read_line(),
  case words(Line) of
    [ "VALUE", _, _, Bytes | _ ] ->
      %%  <<"\r\nEND\r\n">>
      Line ++ "\r\n" ++ recv(list_to_integer(Bytes)+7);
    _ ->
      Line ++ "\r\n"
  end.

recv(0) ->
  "";
recv(Size) ->
  case gen_tcp:recv(Socket, Size) of
    { ok, Data } ->
      Data;
    _ ->
      gen_tcp:close(Socket),
      exit(normal)
  end.

l(X) when is_binary(X) ->
  size(X);
l(X) when is_list(X) ->
  length(X).
