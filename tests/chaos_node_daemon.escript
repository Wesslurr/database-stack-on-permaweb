#!/usr/bin/env escript
%%! -smp enable

main([RoleStr, HttpPortStr]) ->
    HttpPort = list_to_integer(HttpPortStr),
    ScriptDir = filename:dirname(escript:script_name()),
    RootDir = filename:absname(filename:join([ScriptDir, ".."])),
    case RoleStr of
        "hot" ->
            load_mod(filename:join([RootDir, "hymessage/src/hydb_spec.erl"])),
            load_mod(filename:join([RootDir, "hymessage/src/hydb_contract.erl"])),
            load_mod(filename:join([RootDir, "hymessage/src/hymessage_runtime.erl"])),
            {ok, _} = hymessage_runtime:ensure_started(),
            maybe_reconcile_hot_from_wal(),
            {ok, ListenSock} = gen_tcp:listen(HttpPort, [
                binary, {packet, raw}, {active, false}, {reuseaddr, true}, {nodelay, true}, {backlog, 256}
            ]),
            io:format("READY:hot:~p~n", [HttpPort]),
            accept_loop(ListenSock, hot);
        "sd" ->
            load_mod(filename:join([RootDir, "hymessage/src/hydb_spec.erl"])),
            load_mod(filename:join([RootDir, "sd/src/sd_runtime.erl"])),
            {ok, _} = sd_runtime:ensure_started(),
            {ok, ListenSock} = gen_tcp:listen(HttpPort, [
                binary, {packet, raw}, {active, false}, {reuseaddr, true}, {nodelay, true}, {backlog, 256}
            ]),
            io:format("READY:sd:~p~n", [HttpPort]),
            accept_loop(ListenSock, sd)
    end.

load_mod(Path) ->
    case compile:file(Path, [binary, return_errors]) of
        {ok, Mod, Bin} ->
            {module, Mod} = code:load_binary(Mod, Path, Bin),
            ok;
        Err ->
            io:format(standard_error, "Compile error for ~p: ~p~n", [Path, Err]),
            halt(1)
    end.

maybe_reconcile_hot_from_wal() ->
    case os:getenv("HYDB_RECONCILE_WAL_PATH") of
        false -> ok;
        "" -> ok;
        WalPath ->
            case file:read_file(WalPath) of
                {ok, Content} when byte_size(Content) > 0 ->
                    Lines = binary:split(Content, <<"\n">>, [global, trim_all]),
                    Events = lists:filtermap(fun(Line) ->
                        try {true, json:decode(Line)} catch _:_ -> false end
                    end, Lines),
                    case Events of
                        [] -> ok;
                        _ ->
                            {ok, _} = hymessage_runtime:replay(Events),
                            ok
                    end;
                _ -> ok
            end
    end,
    case os:getenv("HYDB_RECONCILE_EPOCH") of
        false -> ok;
        "" -> ok;
        EpStr ->
            case catch list_to_integer(EpStr) of
                TargetEp when is_integer(TargetEp), TargetEp > 1 ->
                    _ = hymessage_runtime:promote_epoch(TargetEp),
                    ok;
                _ -> ok
            end
    end.

accept_loop(ListenSock, Role) ->
    case gen_tcp:accept(ListenSock) of
        {ok, Sock} ->
            spawn(fun() -> handle_http_conn(Sock, Role) end),
            accept_loop(ListenSock, Role);
        {error, closed} ->
            ok;
        {error, _} ->
            timer:sleep(10),
            accept_loop(ListenSock, Role)
    end.

handle_http_conn(Sock, Role) ->
    try
        case recv_http_request(Sock, <<>>) of
            {ok, Method, FullPath, Headers, Body} ->
                ReqMap = build_req_map(Method, FullPath, Headers, Body),
                Endpoint = extract_endpoint(FullPath),
                RespMap = dispatch(Role, Endpoint, ReqMap),
                RawStatus = maps:get(<<"status">>, RespMap, 200),
                Status = if is_integer(RawStatus) -> RawStatus; true -> 200 end,
                JsonBin = iolist_to_binary(json:encode(RespMap)),
                HttpResp = iolist_to_binary([
                    <<"HTTP/1.1 ">>, integer_to_binary(Status), <<" ">>, status_text(Status), <<"\r\n">>,
                    <<"Content-Type: application/json\r\n">>,
                    <<"Content-Length: ">>, integer_to_binary(byte_size(JsonBin)), <<"\r\n">>,
                    <<"Connection: close\r\n\r\n">>,
                    JsonBin
                ]),
                _ = gen_tcp:send(Sock, HttpResp),
                gen_tcp:close(Sock);
            _ ->
                gen_tcp:close(Sock)
        end
    catch
        C:R:S ->
            io:format(standard_error, "HTTP error in ~p: ~p:~p~n~p~n", [Role, C, R, S]),
            ErrBin = iolist_to_binary(io_lib:format("{\"ok\":false,\"error\":\"~p:~p\"}", [C, R])),
            ErrResp = iolist_to_binary([
                <<"HTTP/1.1 500 Error\r\nContent-Type: application/json\r\nContent-Length: ">>,
                integer_to_binary(byte_size(ErrBin)),
                <<"\r\nConnection: close\r\n\r\n">>,
                ErrBin
            ]),
            _ = gen_tcp:send(Sock, ErrResp),
            gen_tcp:close(Sock)
    end.

recv_http_request(Sock, Acc) ->
    case binary:match(Acc, <<"\r\n\r\n">>) of
        {Pos, 4} ->
            HeaderPart = binary:part(Acc, 0, Pos),
            Rest = binary:part(Acc, Pos + 4, byte_size(Acc) - (Pos + 4)),
            [ReqLine | HdrLines] = binary:split(HeaderPart, <<"\r\n">>, [global]),
            [Method, FullPath | _] = binary:split(ReqLine, <<" ">>, [global, trim_all]),
            Headers = parse_headers(HdrLines, #{}),
            ContentLen = case maps:get(<<"content-length">>, Headers, <<"0">>) of
                CLBin -> try binary_to_integer(string:trim(CLBin)) catch _:_ -> 0 end
            end,
            Body = recv_body(Sock, Rest, ContentLen),
            {ok, Method, FullPath, Headers, Body};
        nomatch ->
            case gen_tcp:recv(Sock, 0, 5000) of
                {ok, Chunk} ->
                    recv_http_request(Sock, <<Acc/binary, Chunk/binary>>);
                Err ->
                    Err
            end
    end.

recv_body(_Sock, Rest, ContentLen) when byte_size(Rest) >= ContentLen ->
    binary:part(Rest, 0, ContentLen);
recv_body(Sock, Rest, ContentLen) ->
    Need = ContentLen - byte_size(Rest),
    case gen_tcp:recv(Sock, Need, 5000) of
        {ok, More} ->
            <<Rest/binary, More/binary>>;
        _ ->
            Rest
    end.

parse_headers([], Acc) -> Acc;
parse_headers([Line | Rest], Acc) ->
    case binary:split(Line, <<":">>) of
        [K, V] ->
            KeyLower = string:lowercase(string:trim(K)),
            ValTrim = string:trim(V),
            parse_headers(Rest, Acc#{ KeyLower => ValTrim, <<"headers/", KeyLower/binary>> => ValTrim });
        _ ->
            parse_headers(Rest, Acc)
    end.

build_req_map(_Method, FullPath, Headers, Body) ->
    QueryMap = case binary:split(FullPath, <<"?">>) of
        [_PathOnly, QStr] -> parse_query_string(QStr);
        _ -> #{}
    end,
    BodyMap = if byte_size(Body) > 0 ->
        try json:decode(Body) of
            M when is_map(M) -> M#{ <<"body">> => Body };
            _ -> #{ <<"body">> => Body }
        catch _:_ -> #{ <<"body">> => Body }
        end;
       true -> #{}
    end,
    maps:merge(Headers, maps:merge(BodyMap, QueryMap)).

parse_query_string(QStr) ->
    Pairs = binary:split(QStr, <<"&">>, [global, trim_all]),
    lists:foldl(fun(Pair, Acc) ->
        case binary:split(Pair, <<"=">>) of
            [RawK, RawV] ->
                K = uri_string:percent_decode(RawK),
                V = uri_string:percent_decode(RawV),
                Acc#{ K => V };
            [RawK] ->
                K = uri_string:percent_decode(RawK),
                Acc#{ K => <<"true">> }
        end
    end, #{}, Pairs).

extract_endpoint(FullPath) ->
    [PathOnly | _] = binary:split(FullPath, <<"?">>),
    Parts = binary:split(PathOnly, <<"/">>, [global, trim_all]),
    case Parts of
        [] -> <<"status">>;
        _ -> lists:last(Parts)
    end.

dispatch(hot, <<"put">>, Req) -> hymessage_runtime:put_from_req(#{}, Req, #{});
dispatch(hot, <<"get">>, Req) -> hymessage_runtime:get_from_req(#{}, Req, #{});
dispatch(hot, <<"del">>, Req) -> hymessage_runtime:del_from_req(#{}, Req, #{});
dispatch(hot, <<"exists">>, Req) -> hymessage_runtime:exists_from_req(#{}, Req, #{});
dispatch(hot, <<"schema">>, Req) -> hymessage_runtime:schema_from_req(#{}, Req, #{});
dispatch(hot, <<"scan">>, Req) -> hymessage_runtime:scan_from_req(#{}, Req, #{});
dispatch(hot, <<"state">>, Req) -> hymessage_runtime:get_state_from_req(#{}, Req, #{});
dispatch(hot, <<"sync">>, Req) -> hymessage_runtime:sync_from_req(#{}, Req, #{});
dispatch(hot, <<"replay">>, Req) -> hymessage_runtime:replay_from_req(#{}, Req, #{});
dispatch(hot, <<"promote">>, Req) -> hymessage_runtime:promote_from_req(#{}, Req, #{});
dispatch(hot, <<"reset">>, Req) -> hymessage_runtime:reset_from_req(#{}, Req, #{});
dispatch(hot, <<"cluster_membership">>, Req) -> hymessage_runtime:cluster_membership_from_req(#{}, Req, #{});
dispatch(hot, <<"request_status">>, Req) -> hymessage_runtime:request_status_from_req(#{}, Req, #{});
dispatch(hot, <<"join">>, Req) -> hymessage_runtime:join_from_req(#{}, Req, #{});
dispatch(hot, _, Req) -> hymessage_runtime:get_state_from_req(#{}, Req, #{});

dispatch(sd, Endpoint, Req) ->
    {ok, Res} = sd_runtime:handle_scheduler_http(Endpoint, #{}, Req, #{}),
    Res.

status_text(200) -> <<"OK">>;
status_text(400) -> <<"Bad Request">>;
status_text(401) -> <<"Unauthorized">>;
status_text(403) -> <<"Forbidden">>;
status_text(404) -> <<"Not Found">>;
status_text(409) -> <<"Conflict">>;
status_text(422) -> <<"Unprocessable Entity">>;
status_text(501) -> <<"Not Implemented">>;
status_text(502) -> <<"Bad Gateway">>;
status_text(503) -> <<"Service Unavailable">>;
status_text(504) -> <<"Gateway Timeout">>;
status_text(_) -> <<"Error">>.
