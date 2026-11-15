%%% @doc Native HyperBEAM Arweave L1 checkpoint settlement device (`l1-checkpoint@1.0`).
%%% Signs genuine Format 2 Arweave transactions via `tx@1.0` using the node's
%%% own wallet and dispatches them through `arweave@2.9` when explicitly enabled
%%% in node configuration (`l1-checkpoint-enabled`).
-module(dev_l1_checkpoint).
-export([device_info/1, info/3, commit/3]).
-include_lib("hb/include/hb.hrl").

%% @doc Expose `info' and `commit' on `l1-checkpoint@1.0'.
device_info(_Opts) ->
    #{ exports => [<<"info">>, <<"commit">>] }.

%% @doc Return read-only device metadata for Forge liveness verification.
-spec info(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) -> {ok, #{ _ => _ }}.
info(_Base, _Req, _Opts) ->
    {ok, #{
        <<"device">> => <<"l1-checkpoint@1.0">>,
        <<"exports">> => [<<"info">>, <<"commit">>]
    }}.

%% @doc Sign an Arweave Format 2 checkpoint transaction with the node wallet and
%% post it to L1 via `arweave@2.9`, optionally mining on ArLocal.
-spec commit(#{ _ => _ }, #{ _ => _ }, #{ _ => _ }) ->
    {ok, #{ _ => _ }} | {error, _}.
commit(_Base, Req, Opts) ->
    case hb_util:bool(hb_opts:get(<<"l1-checkpoint-enabled">>, false, Opts)) of
        true ->
            do_commit(Req, Opts);
        false ->
            {error, #{
                <<"status">> => 403,
                <<"body">> => <<"l1-checkpoint@1.0 is not enabled on this node.">>
            }}
    end.

do_commit(Req, Opts) ->
    NodeOpts = hb_http_server:with_node_committer(Opts),
    Address = hb_opts:address(NodeOpts),
    case resolve_l1_from_genesis(Req, NodeOpts) of
        {ok, GenesisUrl, L1Url} ->
            ScopedOpts = with_dynamic_l1_routes(L1Url, NodeOpts),
            maybe_fund_l1_wallet(L1Url, Address, ScopedOpts),
            case fetch_anchor(ScopedOpts) of
                {ok, AnchorB64} ->
                    TXMsg = build_tx_message(Req, AnchorB64, ScopedOpts),
                    SignedTX = hb_message:commit(
                        TXMsg,
                        ScopedOpts,
                        #{ <<"commitment-device">> => <<"tx@1.0">> }
                    ),
                    TXID = hb_message:id(SignedTX, signed, ScopedOpts),
                    case post_tx(SignedTX, ScopedOpts) of
                        {ok, _PostRes} ->
                            Height = maybe_mine_l1(L1Url, ScopedOpts),
                            {ok, #{
                                <<"status">> => <<"finalized">>,
                                <<"tx_id">> => TXID,
                                <<"height">> => Height,
                                <<"wallet">> => hb_util:bin(Address),
                                <<"l1_url">> => L1Url,
                                <<"genesis_url">> => GenesisUrl
                            }};
                        {error, Reason} ->
                            {error, #{
                                <<"status">> => 502,
                                <<"error">> => <<"l1-post-tx-failed">>,
                                <<"details">> => hb_util:bin(hb_format:term(Reason, ScopedOpts))
                            }}
                    end;
                {error, AnchorErr} ->
                    {error, #{
                        <<"status">> => 502,
                        <<"error">> => <<"l1-anchor-failed">>,
                        <<"details">> => hb_util:bin(hb_format:term(AnchorErr, ScopedOpts))
                    }}
            end;
        {error, DiscErr} ->
            {error, #{
                <<"status">> => 502,
                <<"error">> => <<"l1-genesis-discovery-failed">>,
                <<"details">> => hb_util:bin(hb_format:term(DiscErr, NodeOpts))
            }}
    end.

build_tx_message(Req, AnchorB64, Opts) ->
    #{
        <<"format">> => <<"2">>,
        <<"anchor">> => AnchorB64,
        <<"reward">> => <<"1000000">>,
        <<"App-Name">> => <<"HyperStack">>,
        <<"Type">> => <<"Checkpoint">>,
        <<"Official-Tick">> => bin_arg(<<"official-tick">>, Req, <<"0">>, Opts),
        <<"State-Root">> => bin_arg(<<"state-root">>, Req, <<>>, Opts),
        <<"Point-Count">> => bin_arg(<<"point-count">>, Req, <<"0">>, Opts),
        <<"Commitment-Seq">> => bin_arg(<<"commitment-seq">>, Req, <<"0">>, Opts),
        <<"Diff-Seq">> => bin_arg(<<"diff-seq">>, Req, <<"0">>, Opts),
        <<"Epoch">> => bin_arg(<<"epoch">>, Req, <<"1">>, Opts),
        <<"Shard">> => bin_arg(<<"shard">>, Req, <<"cluster">>, Opts),
        <<"Timestamp">> => bin_arg(<<"timestamp">>, Req, <<"0">>, Opts)
    }.

bin_arg(Key, Req, Default, Opts) ->
    hb_util:bin(hb_maps:get(Key, Req, Default, Opts)).

fetch_anchor(Opts) ->
    case hb_ao:resolve(
        #{ <<"device">> => <<"arweave@2.9">> },
        #{ <<"path">> => <<"tx_anchor">> },
        Opts
    ) of
        {ok, RawAnchor} when is_binary(RawAnchor), byte_size(RawAnchor) > 0 ->
            {ok, hb_util:encode(RawAnchor)};
        {ok, Other} ->
            {error, {invalid_anchor, Other}};
        Error ->
            Error
    end.

post_tx(SignedTX, Opts) ->
    TX = hb_message:convert(SignedTX, <<"tx@1.0">>, Opts),
    Serialized = hb_json:encode(ar_tx:tx_to_json_struct(TX#tx{ data = <<>> })),
    Res = hb_http:request(
        #{
            <<"path">> => <<"/arweave/tx">>,
            <<"method">> => <<"POST">>,
            <<"content-type">> => <<"application/json">>,
            <<"body">> => Serialized
        },
        Opts#{ <<"cache-control">> => [<<"no-cache">>, <<"no-store">>] }
    ),
    case Res of
        {ok, Resp = #{ <<"status">> := Status }} when Status >= 200, Status < 300 ->
            _ = hb_cache:write(SignedTX, Opts),
            {ok, Resp};
        {ok, [First | _]} ->
            case First of
                {_, Resp = #{ <<"status">> := Status }} when Status >= 200, Status < 300 ->
                    _ = hb_cache:write(SignedTX, Opts),
                    {ok, Resp};
                _ ->
                    {error, {unexpected_tx_status, First}}
            end;
        {ok, Resp} ->
            {error, {unexpected_tx_status, Resp}};
        Error ->
            Error
    end.

maybe_fund_l1_wallet(<<>>, _Address, _Opts) ->
    ok;
maybe_fund_l1_wallet(_L1Url, undefined, _Opts) ->
    ok;
maybe_fund_l1_wallet(L1Url, Address, Opts) ->
    Key = {?MODULE, funded, L1Url, Address},
    case persistent_term:get(Key, false) of
        true ->
            ok;
        false ->
            AddrBin = hb_util:bin(Address),
            BalPath = <<L1Url/binary, "/wallet/", AddrBin/binary, "/balance">>,
            NeedMint =
                case relay_get(BalPath, Opts) of
                    {ok, Body} ->
                        case hb_util:safe_int(string:trim(Body)) of
                            {ok, Bal} when Bal >= 100_000_000 -> false;
                            _ -> true
                        end;
                    _ ->
                        true
                end,
            case NeedMint of
                true ->
                    MintPath = <<L1Url/binary, "/mint/", AddrBin/binary, "/1000000000000">>,
                    _ = relay_get(MintPath, Opts);
                false ->
                    ok
            end,
            persistent_term:put(Key, true),
            ok
    end.

maybe_mine_l1(<<>>, _Opts) ->
    0;
maybe_mine_l1(L1Url, Opts) ->
    case relay_get(<<L1Url/binary, "/mine">>, Opts) of
        {ok, Body} ->
            case parse_height(Body) of
                Height when is_integer(Height) -> Height;
                _ -> 0
            end;
        _ ->
            0
    end.

relay_get(URL, Opts) ->
    relay_request(<<"GET">>, URL, undefined, Opts).

relay_post(URL, Body, Opts) ->
    relay_request(<<"POST">>, URL, Body, Opts).

relay_request(Method, URL, Body, Opts) ->
    BaseReq = #{
        <<"path">> => <<"call">>,
        <<"relay-path">> => URL,
        <<"relay-method">> => Method,
        <<"cache-control">> => <<"no-store">>
    },
    Req = case Body of
        undefined -> BaseReq;
        _ -> BaseReq#{
            <<"relay-body">> => Body,
            <<"content-type">> => <<"application/json">>
        }
    end,
    case hb_ao:resolve(#{ <<"device">> => <<"relay@1.0">> }, Req, Opts) of
        {ok, #{ <<"body">> := RespBody }} when is_binary(RespBody) ->
            {ok, RespBody};
        {ok, RespBody} when is_binary(RespBody) ->
            {ok, RespBody};
        Other ->
            {error, Other}
    end.

%% @doc Dynamically resolve the authoritative Arweave L1 URL from Genesis Process.
resolve_l1_from_genesis(Req, Opts) ->
    case find_genesis_url(Req, Opts) of
        {ok, GenesisUrl} ->
            NowUrl = <<GenesisUrl/binary, "/~node-process@1.0/genesis/now/results~message@1.0/output/body">>,
            case query_genesis_l1(fun() -> relay_get(NowUrl, Opts) end) of
                {ok, L1Url} ->
                    {ok, GenesisUrl, L1Url};
                _ ->
                    PushUrl = <<GenesisUrl/binary, "/~node-process@1.0/genesis/push/output~message@1.0">>,
                    PushBody = <<"{\"action\":\"Get-Topology\"}">>,
                    case query_genesis_l1(fun() -> relay_post(PushUrl, PushBody, Opts) end) of
                        {ok, L1Url} ->
                            {ok, GenesisUrl, L1Url};
                        Err ->
                            Err
                    end
            end;
        Error ->
            Error
    end.

find_genesis_url(Req, Opts) ->
    FromReq = trim_bin(hb_maps:get(<<"genesis-url">>, Req, <<>>, Opts)),
    case FromReq of
        <<>> ->
            FromOpts = trim_bin(hb_opts:get(<<"genesis-url">>, <<>>, Opts)),
            case FromOpts of
                <<>> -> find_genesis_in_node_processes(Opts);
                Val -> {ok, strip_trailing_slash(Val)}
            end;
        Val ->
            {ok, strip_trailing_slash(Val)}
    end.

find_genesis_in_node_processes(Opts) ->
    Procs = hb_opts:get(<<"node_processes">>, #{}, Opts),
    case is_map(Procs) of
        true ->
            Found = lists:search(
                fun(ProcDef) when is_map(ProcDef) ->
                    case trim_bin(hb_maps:get(<<"genesis-url">>, ProcDef, <<>>, Opts)) of
                        <<>> -> false;
                        _ -> true
                    end;
                   (_) -> false
                end,
                maps:values(Procs)
            ),
            case Found of
                {value, ProcDef} ->
                    Val = trim_bin(hb_maps:get(<<"genesis-url">>, ProcDef, <<>>, Opts)),
                    {ok, strip_trailing_slash(Val)};
                false ->
                    {error, missing_genesis_url}
            end;
        false ->
            {error, missing_genesis_url}
    end.

query_genesis_l1(FetchFun) ->
    case FetchFun() of
        {ok, Body} ->
            extract_l1_url(Body);
        Error ->
            Error
    end.

extract_l1_url(Body) when is_binary(Body) ->
    try hb_json:decode(Body) of
        Map when is_map(Map) ->
            case trim_bin(maps:get(<<"l1_url">>, Map, <<>>)) of
                <<>> ->
                    case maps:get(<<"body">>, Map, undefined) of
                        Inner when is_binary(Inner) -> extract_l1_url(Inner);
                        _ -> {error, missing_l1_url_in_genesis}
                    end;
                Url ->
                    {ok, strip_trailing_slash(Url)}
            end;
        _ ->
            {error, invalid_genesis_response}
    catch _:_ ->
        {error, invalid_genesis_json}
    end.

with_dynamic_l1_routes(L1Url, Opts) ->
    Opts#{
        <<"gateway">> => L1Url,
        <<"routes">> => [
            #{
                <<"template">> => <<"^/arweave">>,
                <<"nodes">> => [
                    #{
                        <<"match">> => <<"^/arweave">>,
                        <<"with">> => L1Url
                    }
                ],
                <<"parallel">> => false,
                <<"responses">> => 1,
                <<"stop-after">> => true,
                <<"admissible-status">> => 200
            },
            #{
                <<"template">> => <<"/graphql">>,
                <<"nodes">> => [
                    #{
                        <<"prefix">> => L1Url
                    }
                ]
            }
        ]
    }.

trim_bin(Val) when is_binary(Val) ->
    iolist_to_binary(string:trim(Val));
trim_bin(Val) when is_list(Val) ->
    iolist_to_binary(string:trim(Val));
trim_bin(_) ->
    <<>>.

strip_trailing_slash(<<>>) ->
    <<>>;
strip_trailing_slash(Bin) ->
    case binary:last(Bin) of
        $/ -> strip_trailing_slash(binary:part(Bin, 0, byte_size(Bin) - 1));
        _ -> Bin
    end.

parse_height(Body) ->
    try hb_json:decode(Body) of
        Map when is_map(Map) ->
            Val = maps:get(<<"height">>, Map, maps:get(<<"current">>, Map, 0)),
            case hb_util:safe_int(Val) of
                {ok, Int} -> Int;
                _ when is_integer(Val) -> Val;
                _ -> 0
            end;
        _ ->
            0
    catch _:_ ->
        0
    end.
