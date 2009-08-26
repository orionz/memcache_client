
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
    noreply=false,
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

