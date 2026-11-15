#!/usr/bin/env escript
%%! -noshell

%% Populate a read-only hb_store_fs directory with WASM binary messages and
%% Forge-packaged external device messages (matching HyperBEAM's cache format),
%% and output JSON mapping each role/device key to its deterministic ImageID/ImplID.
main([StoreDirRaw, HbRootRaw | RoleSpecs]) when length(RoleSpecs) > 0 ->
    StoreDirStr = filename:absname(StoreDirRaw),
    StoreDir = list_to_binary(StoreDirStr),
    HbRoot = filename:absname(HbRootRaw),
    ResolvedSpecs =
        lists:map(
            fun(Spec) ->
                case lists:prefix("dev=", Spec) of
                    true ->
                        DevSpec = lists:nthtail(4, Spec),
                        case string:split(DevSpec, "=", leading) of
                            [KeyStr, DirStr] when KeyStr =/= "", DirStr =/= "" ->
                                {device, list_to_binary(KeyStr), list_to_binary(filename:absname(DirStr))};
                            _ ->
                                erlang:error({invalid_device_spec, Spec})
                        end;
                    false ->
                        case string:split(Spec, "=", leading) of
                            [RoleStr, PathStr] when RoleStr =/= "", PathStr =/= "" ->
                                {wasm, list_to_binary(RoleStr), filename:absname(PathStr)};
                            _ ->
                                erlang:error({invalid_wasm_spec, Spec})
                        end
                end
            end,
            RoleSpecs
        ),

    ok = filelib:ensure_dir(StoreDirStr ++ "/."),
    LibEbins = filelib:wildcard(HbRoot ++ "/_build/default/lib/*/ebin"),
    PluginEbins = filelib:wildcard(HbRoot ++ "/_build/default/plugins/*/ebin"),
    CheckoutEbins = filelib:wildcard(HbRoot ++ "/_build/default/checkouts/*/ebin"),
    ok = code:add_pathsa(LibEbins ++ PluginEbins ++ CheckoutEbins),
    ok = file:set_cwd(HbRoot),
    {ok, _} = application:ensure_all_started(crypto),

    Store = #{
        <<"store-module">> => hb_store_fs,
        <<"name">> => StoreDir
    },
    ok = hb_store:start(Store),
    BaseOpts = #{
        <<"store">> => [Store],
        <<"on">> => #{},
        <<"cache-control">> => [],
        <<"linkify-mode">> => offload,
        <<"prometheus">> => false
    },

    Pairs =
        hb_forge_seed:with_forge_bootstrap(
            BaseOpts,
            fun(Opts) ->
                lists:map(
                    fun({wasm, RoleBin, AbsPath}) ->
                        case file:read_file(AbsPath) of
                            {ok, Bin} ->
                                {ok, ImageID} = hb_cache:write(#{ <<"body">> => Bin }, Opts),
                                {RoleBin, ImageID};
                            {error, Reason} ->
                                erlang:error({wasm_read_failed, AbsPath, Reason})
                        end;
                       ({device, KeyBin, SrcDirBin}) ->
                        Groups = hb_packager:scan([SrcDirBin], #{}),
                        [Pkg] = hb_packager:package_all(Groups, Opts),
                        SpecMsg = hb_packager:spec_message(Pkg, Opts),
                        {ok, SpecID} = hb_cache:write(SpecMsg, Opts),
                        ImplMsg = hb_packager:impl_message(Pkg, SpecID, Opts),
                        {ok, ImplID} = hb_cache:write(ImplMsg, Opts),
                        {KeyBin, ImplID}
                    end,
                    ResolvedSpecs
                )
            end
        ),
    io:format("~s~n", [hb_json:encode(maps:from_list(Pairs))]),
    halt(0);
main(_) ->
    io:format(
        standard_error,
        "usage: prepare_wasm_store.escript <store_dir> <hb_root> <role>=<wasm_path>...~n",
        []
    ),
    halt(2).
