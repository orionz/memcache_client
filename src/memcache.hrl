
-record(request, {
    opcode=0,
    data_type=0,
    reserved=0,
    opaque=0,
    cas=0,
    flags=0,
    expires=0,
    delta=0,
    initial=0,
    num_keys=1,
    key = <<"">>,
    value = <<"">>
  }).

-record(response, {
    opcode=null, % get
    data_type=null,
    status=null,
    cas=null,
    opaque=null,
    flags=null,
    expires=null,
    key=null,
    value=null
    }).

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
-define(SETQ, 17).
-define(ADDQ, 18).
-define(REPLACEQ, 19).
-define(DELETEQ, 20).
-define(INCREMENTQ, 21).
-define(DECREMENTQ, 22).
-define(QUITQ, 23).
-define(FLUSHQ, 24).
-define(APPENDQ, 25).
-define(PREPENDQ, 26).
