%%% @doc Native HyperBEAM Arweave L1 checkpoint settlement device (`l1-checkpoint@1.0`).
%%% Signs genuine Format 2 Arweave transactions via `tx@1.0` using the node's
%%% own wallet and dispatches them through `arweave@2.9` when explicitly enabled
%%% in node configuration (`l1-checkpoint-enabled`).
-module(dev_l1_checkpoint).
-export([device_info/1, commit/3]).
-include("include/hb.hrl").

%% @doc Expose only `commit` on `l1-checkpoint@1.0`.
device_info(_Opts) ->
    #{ exports => [<<"commit">>] }.

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
    L1Url = hb_util:bin(hb_maps:get(<<"l1-url">>, Req, <<"http://127.0.0.1:1984">>, NodeOpts)),
    maybe_fund_l1_wallet(L1Url, Address, NodeOpts),
    case fetch_anchor(NodeOpts) of
        {ok, AnchorB64} ->
            TXMsg = build_tx_message(Req, AnchorB64, NodeOpts),
            SignedTX = hb_message:commit(
                TXMsg,
                NodeOpts,
                #{ <<"commitment-device">> => <<"tx@1.0">> }
            ),
            TXID = hb_message:id(SignedTX, signed, NodeOpts),
            case post_tx(SignedTX, NodeOpts) of
                {ok, _PostRes} ->
                    Height = maybe_mine_l1(L1Url, NodeOpts),
                    {ok, #{
                        <<"status">> => <<"finalized">>,
                        <<"tx_id">> => TXID,
                        <<"height">> => Height,
                        <<"wallet">> => hb_util:bin(Address)
                    }};
                {error, Reason} ->
                    {error, #{
                        <<"status">> => 502,
                        <<"error">> => <<"l1-post-tx-failed">>,
                        <<"details">> => hb_util:bin(hb_format:term(Reason, NodeOpts))
                    }}
            end;
        {error, AnchorErr} ->
            {error, #{
                <<"status">> => 502,
                <<"error">> => <<"l1-anchor-failed">>,
                <<"details">> => hb_util:bin(hb_format:term(AnchorErr, NodeOpts))
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
    case hb_ao:resolve(
        #{ <<"device">> => <<"relay@1.0">> },
        #{
            <<"path">> => <<"call">>,
            <<"relay-path">> => URL,
            <<"relay-method">> => <<"GET">>,
            <<"cache-control">> => <<"no-store">>
        },
        Opts
    ) of
        {ok, #{ <<"body">> := Body }} when is_binary(Body) ->
            {ok, Body};
        {ok, Body} when is_binary(Body) ->
            {ok, Body};
        Other ->
            {error, Other}
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
