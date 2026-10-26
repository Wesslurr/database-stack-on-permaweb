#!/usr/bin/env escript
%% -*- coding: utf-8 -*-
%% Native Erlang Arweave RSA-4096 Wallet Generator (Zero Node.js dependency)

main(Args) ->
    DbDir = case os:getenv("DB_DIR") of
        false -> filename:dirname(filename:dirname(escript:script_name()));
        "" -> filename:dirname(filename:dirname(escript:script_name()));
        EnvDir -> EnvDir
    end,

    WalletPath = case Args of
        [WPath | _] -> WPath;
        _ -> filename:join([DbDir, "secrets/wallet_su.json"])
    end,

    AddressPath = case Args of
        [_, APath | _] -> APath;
        _ -> filename:join([DbDir, "secrets/address_su.txt"])
    end,

    filelib:ensure_dir(WalletPath),
    filelib:ensure_dir(AddressPath),

    io:format("[gen_wallet] Generating 4096-bit RSA Arweave JWK...~n"),
    {_Pub, [E, Modulus, DExp, P1, P2, Dmp1, Dmq1, Iqmp]} = crypto:generate_key(rsa, {4096, 65537}),

    JWK = #{
        <<"kty">> => <<"RSA">>,
        <<"e">> => b64url_encode(E),
        <<"n">> => b64url_encode(Modulus),
        <<"d">> => b64url_encode(DExp),
        <<"p">> => b64url_encode(P1),
        <<"q">> => b64url_encode(P2),
        <<"dp">> => b64url_encode(Dmp1),
        <<"dq">> => b64url_encode(Dmq1),
        <<"qi">> => b64url_encode(Iqmp)
    },

    Address = b64url_encode(crypto:hash(sha256, Modulus)),
    JWKJson = iolist_to_binary(json:encode(JWK)),

    ok = file:write_file(WalletPath, JWKJson),
    ok = file:write_file(AddressPath, <<Address/binary, "\n">>),

    io:format("[gen_wallet] Successfully generated wallet!~n"),
    io:format("  Path:    ~s~n", [WalletPath]),
    io:format("  Address: ~s~n", [Address]),
    halt(0).

b64url_encode(Bin) when is_binary(Bin) ->
    B64 = base64:encode(Bin),
    S1 = binary:replace(binary:replace(B64, <<"+">>, <<"-">>, [global]), <<"/">>, <<"_">>, [global]),
    binary:replace(S1, <<"=">>, <<>>, [global]).
