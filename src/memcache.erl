-module(memcache).

-export([open/0,open/2,open/3]).
-export([new/0,connect/3,disconnect/2,close/1]).

-export([add/3,add/5,replace/3,replace/5,set/3,set/5,set/6,cas/4,cas/6]).
-export([addq/3,addq/5,replaceq/3,replaceq/5,setq/3,setq/5,setq/6,casq/4,casq/6]).
-export([get/2,getk/2,mget/2,mgetk/2]).
-export([delete/2,quit/1,flush/1,flush/2,noop/1,version/1,stat/1]).
-export([deleteq/2,quitq/1,flushq/1,flushq/2]).
-export([raw/4]).
-export([append/3,prepend/3]).
-export([appendq/3,prependq/3]).
-export([increment/3,decrement/3]).
-export([increment/4,decrement/4]).
-export([incrementq/3,decrementq/3]).
-export([incrementq/4,decrementq/4]).

-include_lib("memcache.hrl").

-define(DEFAULT_OPTIONS, [text]).
-define(DEFAULT_HOSTS, [ {"127.0.0.1", 11211 } ]).

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
  open( ?DEFAULT_HOSTS, ?DEFAULT_OPTIONS ).

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

cas(Con, Key, Value, Cas) when is_integer(Cas) ->
  cas(Con, Key, Value, 0, 0, Cas).
cas(Con, Key, Value, Flags, Expires, Cas) when is_integer(Cas) ->
  set(Con, Key, Value, Flags, Expires, Cas).

casq(Con, Key, Value, Cas) when is_integer(Cas) ->
  casq(Con, Key, Value, 0, 0, Cas).
casq(Con, Key, Value, Flags, Expires, Cas) when is_integer(Cas) ->
  setq(Con, Key, Value, Flags, Expires, Cas).

set(Con, Key, Value) ->
  set(Con, Key, Value, 0, 0, 0).
set(Con, Key, Value, Flags, Expires) ->
  set(Con, Key, Value, Flags, Expires, 0).
set(Con, Key, Value, Flags, Expires, Cas) ->
  case request( Con, #request{ opcode=?SET, key=Key, value=Value, flags=Flags, expires=Expires, cas=Cas } ) of
    ok -> ok;
    { ok, [ _, _, _, Cas2 ] } -> { ok, Cas2 };
    { error, Error } -> { error, Error }
  end.

setq(Con, Key, Value) ->
  setq(Con, Key, Value, 0, 0, 0).
setq(Con, Key, Value, Flags, Expires) ->
  setq(Con, Key, Value, Flags, Expires, 0).
setq(Con, Key, Value, Flags, Expires, Cas) ->
  request( Con, #request{ opcode=?SETQ, key=Key, value=Value, flags=Flags, expires=Expires, cas=Cas } ),
  ok.

add(Con, Key, Value) ->
  add(Con, Key, Value, 0, 0).
add(Con, Key, Value, Flags, Expires) ->
  case request(Con, #request{ opcode=?ADD, key=Key, value=Value, flags=Flags, expires=Expires } ) of
    { ok, [ _, _, _, Cas ] } -> { ok, Cas };
    { error, Error } -> { error, Error }
  end.

addq(Con, Key, Value) ->
  addq(Con, Key, Value, 0, 0).
addq(Con, Key, Value, Flags, Expires) ->
  request(Con, #request{ opcode=?ADDQ, key=Key, value=Value, flags=Flags, expires=Expires } ),
  ok.

replace(Con, Key, Value) ->
  replace(Con, Key, Value, 0, 0).
replace(Con, Key, Value, Flags, Expires) ->
  case request( Con, #request{ opcode=?REPLACE, key=Key, value=Value, flags=Flags, expires=Expires } ) of
    { ok, [ _, _, _, Cas ] } -> { ok, Cas };
    { error, Error } -> { error, Error }
  end.

replaceq(Con, Key, Value) ->
  replaceq(Con, Key, Value, 0, 0).
replaceq(Con, Key, Value, Flags, Expires) ->
  request( Con, #request{ opcode=?REPLACEQ, key=Key, value=Value, flags=Flags, expires=Expires } ),
  ok.

append(Con, Key, Value) ->
  case request( Con, #request{ opcode=?APPEND, key=Key, value=Value } ) of
    { ok, _ } -> ok;
    { error, Error } -> { error, Error }
  end.

appendq(Con, Key, Value) ->
  request( Con, #request{ opcode=?APPENDQ, key=Key, value=Value } ),
  ok.

prepend(Con, Key, Value) ->
  case request( Con, #request{ opcode=?PREPEND, key=Key, value=Value } ) of
    { ok, _ } -> ok;
    { error, Error } -> { error, Error }
  end.

prependq(Con, Key, Value) ->
  request( Con, #request{ opcode=?PREPENDQ, key=Key, value=Value } ),
  ok.

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

mget(Con, Keys) when is_list(Keys)->
  { ok, [ { Value, Flags, Cas } || { ok, [ Value, _, Flags, Cas ] } <- request(Con, #request{ opcode=?GET, key=Keys, num_keys=length(Keys) } ) ] }.

mgetk(Con, Keys) when is_list(Keys)->
  { ok, [ { Key2, Value, Flags, Cas } || { ok, [ Value, Key2, Flags, Cas ] } <- request( Con, #request{ opcode=?GETK, key=Keys, num_keys=length(Keys)} ) ] }.

delete(Con, Key) ->
  case request( Con, #request{ opcode=?DELETE, key=Key } ) of
    { ok, _ } -> ok;
    { error, Error } -> { error, Error }
  end.

deleteq(Con, Key) ->
  request( Con, #request{ opcode=?DELETEQ, key=Key } ),
  ok.

quit(Con) ->
  case request( Con, #request{ opcode=?QUIT } ) of
    { ok, _ } -> ok;
    { error, Error } -> { error, Error }
  end.

quitq(Con) ->
  request( Con, #request{ opcode=?QUITQ } ),
  ok.

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

flushq(Con) when is_pid(Con)->
  flushq(Con, 0).
flushq(Con, Exp) ->
  request( Con, #request{ opcode=?FLUSHQ, expires=Exp } ),
  ok.

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

increment(Con, Key, Delta) ->
  increment(Con, Key, 0, Delta).
increment(Con, Key, Initial, Delta) ->
  case request( Con, #request{ opcode=?INCREMENT, key=Key, delta=Delta, initial=Initial } ) of
    { ok, [ <<Value:64>> | _ ] } -> { ok, Value };
    { error, Error } -> { error, Error }
  end.

incrementq(Con, Key, Delta) ->
  incrementq(Con, Key, 0, Delta).
incrementq(Con, Key, Initial, Delta) ->
  request( Con, #request{ opcode=?INCREMENTQ, key=Key, delta=Delta, initial=Initial } ),
  ok.

decrement(Con, Key, Delta) ->
  decrement(Con, Key, 0, Delta).
decrement(Con, Key, Initial, Delta) ->
  case request( Con, #request{ opcode=?DECREMENT, key=Key, delta=Delta, initial=Initial } ) of
    { ok, [ <<Value:64>> | _ ] } -> { ok, Value };
    { error, Error } -> { error, Error }
  end.

decrementq(Con, Key, Delta) ->
  decrementq(Con, Key, 0, Delta).
decrementq(Con, Key, Initial, Delta) ->
  request( Con, #request{ opcode=?DECREMENTQ, key=Key, delta=Delta, initial=Initial } ),
  ok.

