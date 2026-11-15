#!/usr/bin/env escript
%%! -noshell

%% ar_tx record fields needed to inspect TX#tx.data (element 12 of #tx{})
%% Avoid hardcoded path to ar.hrl by calling dev_ans104:to and reading TX#tx.data cleanly.
main(Args) ->
    ScriptDir = filename:dirname(filename:absname(escript:script_name())),
    ProjectRoot = filename:dirname(ScriptDir),
    DefaultDevSrc = filename:join([ProjectRoot, "engine", "devices", "l1_checkpoint", "src"]),
    DefaultWallet = filename:join([ProjectRoot, "engine", "data", "genesis", "wallet.json"]),
    DefaultHbRoot = filename:join([ProjectRoot, "HyperBEAM"]),
    {DeviceSrcRaw, WalletRaw, GenesisRaw, HbRootRaw} =
        case Args of
            [D, W, G, H] -> {D, W, G, H};
            [D, W, G] -> {D, W, G, DefaultHbRoot};
            [D, W] -> {D, W, "http://127.0.0.1:8733", DefaultHbRoot};
            [D] -> {D, DefaultWallet, "http://127.0.0.1:8733", DefaultHbRoot};
            [] -> {DefaultDevSrc, DefaultWallet, "http://127.0.0.1:8733", DefaultHbRoot}
        end,
    DeviceSrc = filename:absname(DeviceSrcRaw),
    WalletPath = filename:absname(WalletRaw),
    HbRoot = filename:absname(HbRootRaw),
    GenesisUrl = list_to_binary(GenesisRaw),

    LibEbins = filelib:wildcard(HbRoot ++ "/_build/default/lib/*/ebin"),
    PluginEbins = filelib:wildcard(HbRoot ++ "/_build/default/plugins/*/ebin"),
    CheckoutEbins = filelib:wildcard(HbRoot ++ "/_build/default/checkouts/*/ebin"),
    ok = code:add_pathsa(LibEbins ++ PluginEbins ++ CheckoutEbins),

    ok = file:set_cwd(HbRoot),
    {ok, _} = application:ensure_all_started(inets),
    {ok, _} = application:ensure_all_started(ssl),
    {ok, _} = application:ensure_all_started(hackney),

    Wallet = hb:wallet(WalletPath),
    Signer = hb:address(Wallet),
    PublishCodec = <<"ans104@1.0">>,
    BuildOpts =
        (hb_forge_args:package_opts(#{}))#{
            <<"priv-wallet">> => Wallet,
            <<"prometheus">> => false,
            <<"commitment-device">> => PublishCodec,
            <<"on">> => #{},
            <<"cache-control">> => [<<"no-store">>]
        },

    Built =
        hb_forge_seed:with_forge_bootstrap(
            BuildOpts,
            fun(NodeOpts) ->
                Groups =
                    hb_packager:scan(
                        [DeviceSrc],
                        #{ <<"device-roots">> => all }
                    ),
                Pkgs =
                    hb_packager:require_spec_bodies(
                        hb_packager:package_all(Groups, NodeOpts)
                    ),
                lists:map(
                    fun(Pkg) ->
                        Spec =
                            hb_message:commit(
                                hb_packager:spec_message(Pkg, NodeOpts),
                                NodeOpts,
                                PublishCodec
                            ),
                        SpecID = hb_message:id(Spec, signed, NodeOpts),
                        SpecRaw = tx_data_bytes(Spec, NodeOpts),
                        Impl =
                            hb_message:commit(
                                hb_ao:normalize_keys(
                                    hb_packager:impl_message(Pkg, SpecID, NodeOpts),
                                    NodeOpts
                                ),
                                NodeOpts,
                                PublishCodec
                            ),
                        ImplID = hb_message:id(Impl, signed, NodeOpts),
                        ImplRaw = tx_data_bytes(Impl, NodeOpts),
                        DevName = maps:get(device_name, Pkg),
                        {DevName, SpecID, Spec, SpecRaw, ImplID, Impl, ImplRaw}
                    end,
                    Pkgs
                )
            end
        ),

    HttpOpts = #{
        <<"priv-wallet">> => Wallet,
        <<"prometheus">> => false,
        <<"on">> => #{},
        <<"cache-control">> => [<<"no-store">>],
        <<"store">> => [#{ <<"store-module">> => hb_store_volatile, <<"name">> => <<"forge_pub_tmp">> }]
    },
    case hb_http_client:start_link(HttpOpts) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,

    Published =
        lists:map(
            fun({DevName, SpecID, Spec, SpecRaw, ImplID, Impl, ImplRaw}) ->
                {ok, SpecResp} = cache_write(GenesisUrl, Spec, HttpOpts),
                {ok, ImplResp} = cache_write(GenesisUrl, Impl, HttpOpts),
                register_local_raw(GenesisUrl, SpecID, SpecRaw, HttpOpts),
                register_local_raw(GenesisUrl, ImplID, ImplRaw, HttpOpts),
                #{
                    <<"device_name">> => DevName,
                    <<"spec_id">> => SpecID,
                    <<"impl_id">> => ImplID,
                    <<"spec_path">> => maps:get(<<"path">>, SpecResp, <<>>),
                    <<"impl_path">> => maps:get(<<"path">>, ImplResp, <<>>),
                    <<"spec_raw_bytes">> => byte_size(SpecRaw),
                    <<"impl_raw_bytes">> => byte_size(ImplRaw)
                }
            end,
            Built
        ),

    [First | _] = Published,
    Out = First#{
        <<"status">> => <<"ok">>,
        <<"genesis_url">> => GenesisUrl,
        <<"signer">> => Signer
    },
    io:format("~s~n", [hb_json:encode(Out)]),
    halt(0).

%% Extract canonical ANS-104 TX#tx.data bytes (element 12 of #tx{}).
tx_data_bytes(SignedMsg, Opts) ->
    {ok, RawTX} = dev_ans104:to(SignedMsg, #{}, Opts),
    case element(12, RawTX) of
        B when is_binary(B) -> B;
        _ -> <<>>
    end.

%% Register canonical TX#tx.data bytes on hb-genesis under ~local-name@1.0 so
%% hb-arbit's /arweave/raw/<ID> gateway route serves exact verifiable bytes.
register_local_raw(_GenesisUrl, _ID, <<>>, _Opts) ->
    ok;
register_local_raw(GenesisUrl, ID, RawBin, Opts) ->
    RawKey = <<"arweave-raw-", ID/binary>>,
    RegReq = hb_message:commit(
        #{
            <<"key">> => RawKey,
            <<"value">> => RawBin
        },
        Opts,
        <<"httpsig@1.0">>
    ),
    case hb_http:post(GenesisUrl, <<"/~local-name@1.0/register">>, RegReq, Opts) of
        {ok, <<"Registered.">>} ->
            ok;
        Other ->
            io:format(standard_error, "local_name register failed for ~s: ~p~n", [RawKey, Other]),
            erlang:error({local_name_register_failed, RawKey, Other})
    end.

cache_write(GenesisUrl, InnerMsg, Opts) ->
    WriteMsg =
        hb_message:commit(
            #{
                <<"path">> => <<"/~cache@1.0/write">>,
                <<"type">> => <<"cache-write">>,
                <<"method">> => <<"POST">>,
                <<"body">> => InnerMsg
            },
            Opts,
            <<"httpsig@1.0">>
        ),
    case hb_http:post(GenesisUrl, WriteMsg, Opts) of
        {ok, Res} ->
            {ok, Res};
        Other ->
            io:format(standard_error, "cache_write failed: ~p~n", [Other]),
            erlang:error({cache_write_failed, Other})
    end.
