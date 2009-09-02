-module(memcache).

-export([open/0,open/2,open/3]).
-export([new/0,connect/3,disconnect/2,close/1]).

-export([add/3,add/5,replace/3,replace/5,set/3,set/5,set/6,cas/4,cas/6]).
-export([get/2,getk/2]).
-export([delete/2,quit/1,flush/1,flush/2,noop/1,version/1,stat/1]).
-export([raw/4]).
-export([append/3,prepend/3]).
-export([increment/3,decrement/3]).

%-define(TCP_BINARY_OPTIONS, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]).
%-define(TCP_TEXT_OPTIONS, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]).

-include_lib("memcache.hrl").

-define(DEFAULT_OPTIONS, [text]).
-define(DEFAULT_IP, "127.0.0.1").
-define(DEFAULT_PORT, 11211).

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

new() ->
  memcache_cluster:start().

open() ->
  open( [ { ?DEFAULT_IP, ?DEFAULT_PORT } ], ?DEFAULT_OPTIONS ).

open(Host, Options) when is_tuple(Host) ->
  open([ Host ], Options);
open(Hosts, Options ) when is_list(Hosts) and is_list(Options) ->
  memcache:open(memcache:new(), Hosts, Options).

%% kind of hard coding text and binary as the only options here
open(Con, Hosts, [ Type ]) when is_pid(Con) and is_list(Hosts) ->
  ok = verify_hosts(Hosts),
  [ memcache:connect(Con, { Ip, Port }, [ Type ]) || { Ip, Port } <- Hosts ],
  Con.

connect(Con, { Ip, Port }, [Type]) ->
  memcache_cluster:connect(Con, Type, Ip, Port).

disconnect(Con, { Ip, Port }) ->
  memcache_cluster:disconnect(Con, Ip, Port).

close(Con) ->
  memcache_cluster:close(Con).

verify_hosts(Hosts) ->
  case [ { Ip, Port } || { Ip, Port } <- Hosts, is_integer(Port), is_list(Ip) ] of
    Hosts -> ok;
    Good -> exit({ bad_hosts_args, Hosts -- Good })
  end.

request(Con, Request) ->
  Con ! { request, self(), Request },
  receive
    { response, Response } -> Response
  end.

%cas(Con, Key, Value, Cas) when is_integer(Cas) and Cas > 0 ->
cas(Con, Key, Value, Cas) when is_integer(Cas) ->
  cas(Con, Key, Value, 0, 0, Cas).
cas(Con, Key, Value, Flags, Expires, Cas) when is_integer(Cas) ->
  set(Con, Key, Value, Flags, Expires, Cas).

set(Con, Key, Value) ->
  set(Con, Key, Value, 0, 0, 0, false).
set(Con, Key, Value, Flags, Expires) ->
  set(Con, Key, Value, Flags, Expires, 0, false).
set(Con, Key, Value, Flags, Expires, Cas) ->
  set(Con, Key, Value, Flags, Expires, Cas, false).
set(Con, Key, Value, Flags, Expires, Cas, Noreply) ->
  case request( Con, #request{ opcode=?SET, key=Key, value=Value, flags=Flags, expires=Expires, cas=Cas, noreply=Noreply } ) of
    ok -> ok;
    { ok, [ _, _, _, Cas2 ] } -> { ok, Cas2 };
    { error, Error } -> { error, Error }
  end.

add(Con, Key, Value) ->
  add(Con, Key, Value, 0, 0).
add(Con, Key, Value, Flags, Expires) ->
  case request(Con, #request{ opcode=?ADD, key=Key, value=Value, flags=Flags, expires=Expires } ) of
    { ok, [ _, _, _, Cas ] } -> { ok, Cas };
    { error, Error } -> { error, Error }
  end.

replace(Con, Key, Value) ->
  replace(Con, Key, Value, 0, 0).
replace(Con, Key, Value, Flags, Expires) ->
  case request( Con, #request{ opcode=?REPLACE, key=Key, value=Value, flags=Flags, expires=Expires } ) of
    { ok, [ _, _, _, Cas ] } -> { ok, Cas };
    { error, Error } -> { error, Error }
  end.

append(Con, Key, Value) ->
  case request( Con, #request{ opcode=?APPEND, key=Key, value=Value } ) of
    { ok, _ } -> ok;
    { error, Error } -> { error, Error }
  end.

prepend(Con, Key, Value) ->
  case request( Con, #request{ opcode=?PREPEND, key=Key, value=Value } ) of
    { ok, _ } -> ok;
    { error, Error } -> { error, Error }
  end.

get(Con, Key) ->
  case request(Con, #request{ opcode=?GET, key=Key } ) of
    { ok, [ Value, _, Flags, Cas ] } -> { ok, Value, Flags, Cas };
    { error, Error } -> { error, Error }
  end.

getk(Con, Key) ->
  case request( Con, #request{ opcode=?GETK, key=Key } ) of
    { ok, [ Value, Key2, Flags, Cas ] } -> { ok, Key2, Value, Flags, Cas };
    { error, Error } -> { error, Error }
  end.

delete(Con, Key) ->
  delete(Con, Key, false).
delete(Con, Key, Noreply) ->
  case request( Con, #request{ opcode=?DELETE, key=Key, noreply=Noreply } ) of
    { ok, _ } -> ok;
    { error, Error } -> { error, Error }
  end.

quit(Con) ->
  case request( Con, #request{ opcode=?QUIT } ) of
    { ok, _ } -> ok;
    { error, Error } -> { error, Error }
  end.

raw(Con, Opcode, Key, Data) ->
  Con ! { raw, self(), Opcode, Key, Data },
  receive
    { raw_response, Response } ->
      Response
  end.

flush(Con) when is_pid(Con)->
  flush(Con, 0).
flush(Con, Exp) ->
  [
    case Request of
      { ok, _ } -> ok;
      { error, Error } -> { error, Error }
    end
    ||
    Request <- request( Con, #request{ opcode=?FLUSH, expires=Exp } )
  ].

noop(Con) ->
  case request( Con, #request{ opcode=?NOOP } ) of
    { ok, _ } -> ok;
    { error, Error } -> { error, Error }
  end.

version(Con) ->
  [
    case Response of
      { ok, [ Version, _, _, _ ] } -> { ok, Version };
      { error, Error } -> { error, Error }
    end
    ||
    Response <- request( Con, #request{ opcode=?VERSION } )
  ].

stat(Con) ->
  request( Con, #request{ opcode=?STAT } ).

%% increment(Key,Delta,Initial,Expires) -> initial and expires is only on binary protocol
%  case request( #request{ opcode=?INCREMENT, key=Key, delta=Delta, initial=Initial, expires=Expires } ) of
increment(Con, Key, Delta) ->
  increment(Con, Key, Delta, false).
increment(Con, Key, Delta, Noreply) ->
  case request( Con, #request{ opcode=?INCREMENT, key=Key, delta=Delta, noreply=Noreply } ) of
    { ok, [ <<Value:64>> | _ ] } -> { ok, Value };
    { error, Error } -> { error, Error }
  end.

%decrement(Key,Delta,Initial,Expires) ->
%  case request( #request{ opcode=?DECREMENT, key=Key, delta=Delta, initial=Initial, expires=Expires } ) of
decrement(Con, Key,Delta) ->
  decrement(Con, Key, Delta, false).
decrement(Con, Key,Delta, Noreply) ->
  case request( Con, #request{ opcode=?DECREMENT, key=Key, delta=Delta, noreply=Noreply } ) of
    { ok, [ <<Value:64>> | _ ] } -> { ok, Value };
    { error, Error } -> { error, Error }
  end.

