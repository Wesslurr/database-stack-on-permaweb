-module(sd_runtime).
-behaviour(gen_server).

%% ============================================================================
%% Shared Data Pool (SD) Runtime Engine (~sd@1.0)
%% ============================================================================
%% Responsibilities:
%% 1. Append-Only Disk Write-Ahead-Log (WAL) with group commit fsync.
%% 2. In-memory RAM dual-store & secondary query serving.
%% 3. Real-time TCP streaming subscriber from Hot Node (:8736).
%% 4. Multi-party batch checkpoint proposal for AO Arbitrator.
%% 5. Epoch fencing protection against split-brain scenarios.
%% ============================================================================

-export([start_link/0, ensure_started/0, reset/0, is_connected/0, get_current_tick/0, get_state_root/0, get_epoch/0]).
-export([get_tick/0, get_state/0, get_key/1, get_wal/0, get_wal_batch/2, get_status/0, get_status_map/0, fence_epoch/1, recover_wal/0, simulate_restart_from_wal/0]).
-export([get_tick_from_req/3, get_state_from_req/3, get_key_from_req/3, get_wal_from_req/3, get_wal_batch_from_req/3, get_status_from_req/3, reset_from_req/3, fence_from_req/3, restart_from_req/3, handle_scheduler_http/4]).
-export([verify_admin_req/1, is_test_profile/0, is_degraded/0, canonical_hash_64/3]).
-export([compute_event_hash/2, compute_canonical_event_hash/1, compute_canonical_event_hash/2, compute_legacy_event_hash/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(TABLE, sd_ram_store).
-define(COMPAT_TABLE, su_ram_store).
-define(SERVER, ?MODULE).
-define(HOT_NODE_PORT, 8736).
-define(GENESIS_HASH, <<"0000000000000000000000000000000000000000000000000000000000000000">>).

get_hot_host() ->
    case os:getenv("HOT_NODE_HOST") of
        false -> "127.0.0.1";
        "" -> "127.0.0.1";
        Host -> Host
    end.

get_hot_port() ->
    case os:getenv("HOT_NODE_PORT") of
        false -> 8736;
        "" -> 8736;
        PStr -> list_to_integer(PStr)
    end.

-record(state, {
    sub_pid,
    hot_host = "127.0.0.1",
    hot_port = 8736
}).

get_db_dir() ->
    case os:getenv("DB_DIR") of
        false -> ".";
        "" -> ".";
        Dir -> Dir
    end.

wal_path() ->
    case os:getenv("HYDB_WAL_PATH") of
        false -> filename:join([get_db_dir(), "sd/wal.log"]);
        "" -> filename:join([get_db_dir(), "sd/wal.log"]);
        P when is_list(P), length(P) > 0 -> P;
        _ -> filename:join([get_db_dir(), "sd/wal.log"])
    end.

is_connected() ->
    ensure_started(),
    lookup_val(connected_to_hot, false).

get_current_tick() ->
    ensure_started(),
    lookup_val(tick, 0).

get_state_root() ->
    ensure_started(),
    lookup_val(state_root, 0).

get_epoch() ->
    ensure_started(),
    lookup_val(epoch, 1).

fence_epoch(NewEpoch) when is_integer(NewEpoch) ->
    ensure_started(),
    gen_server:call(?SERVER, {fence_epoch, NewEpoch}, 5000).

recover_wal() ->
    ensure_started(),
    gen_server:call(?SERVER, recover_wal, 10000).

simulate_restart_from_wal() ->
    ensure_started(),
    gen_server:call(?SERVER, simulate_restart_from_wal, 10000).

ensure_started() ->
    case whereis(?SERVER) of
        undefined ->
            case gen_server:start({local, ?SERVER}, ?MODULE, [], []) of
                {ok, Pid} -> {ok, Pid};
                {error, {already_started, Pid}} -> {ok, Pid};
                Error -> Error
            end;
        Pid -> {ok, Pid}
    end.

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    init_ets(),
    recover_from_wal(wal_path()),
    SubHost = get_hot_host(),
    SubPort = get_hot_port(),
    SubPid = spawn_link(fun() -> subscriber_loop(?SERVER, SubHost, SubPort) end),
    {ok, #state{sub_pid = SubPid, hot_host = SubHost, hot_port = SubPort}}.

init_ets() ->
    case ets:info(?TABLE) of
        undefined ->
            ets:new(?TABLE, [public, named_table, set, {read_concurrency, true}, {write_concurrency, true}]),
            reset_ets_defaults();
        _ -> ok
    end,
    case ets:info(?COMPAT_TABLE) of
        undefined ->
            ets:new(?COMPAT_TABLE, [public, named_table, set, {read_concurrency, true}, {write_concurrency, true}]);
        _ -> ok
    end.

reset_ets_defaults() ->
    ets:insert(?TABLE, {epoch, 1}),
    ets:insert(?TABLE, {tick, 0}),
    ets:insert(?TABLE, {state_root, 0}),
    ets:insert(?TABLE, {latest_hash, ?GENESIS_HASH}),
    ets:insert(?TABLE, {total_replicated, 0}),
    ets:insert(?TABLE, {fenced_rejections, 0}),
    ets:insert(?TABLE, {gap_rejections, 0}),
    ets:insert(?TABLE, {hash_rejections, 0}),
    ets:insert(?TABLE, {degraded_rejections, 0}),
    ets:insert(?TABLE, {connected_to_hot, false}),
    ets:insert(?TABLE, {wal_corrupted, false}),
    ets:insert(?TABLE, {node_health, healthy}),
    ets:insert(?TABLE, {wal_recovery_halt_reason, none}),
    ets:insert(?TABLE, {wal_recovery_truncated_detected, false}),
    ets:insert(?TABLE, {history, []}).

get_or_open_wal_fd() ->
    case ets:lookup(?TABLE, wal_fd) of
        [{wal_fd, Fd}] -> Fd;
        [] ->
            Path = wal_path(),
            _ = filelib:ensure_dir(Path),
            {ok, Fd} = file:open(Path, [append, raw, binary]),
            ets:insert(?TABLE, {wal_fd, Fd}),
            Fd
    end.

close_wal_fd() ->
    case ets:lookup(?TABLE, wal_fd) of
        [{wal_fd, Fd}] ->
            _ = catch file:close(Fd),
            ets:delete(?TABLE, wal_fd),
            ok;
        [] -> ok
    end.

is_degraded() ->
    lookup_val(wal_corrupted, false).

mark_recovery_failure(Reason) ->
    ets:insert(?TABLE, {wal_recovery_halt_reason, Reason}),
    ets:insert(?TABLE, {wal_corrupted, true}),
    ets:insert(?TABLE, {node_health, degraded}).

canonical_hash_64(KeyInt, ValInt, VerInt) when is_integer(KeyInt), is_integer(ValInt), is_integer(VerInt) ->
    Mask64 = 16#FFFFFFFFFFFFFFFF,
    Prime = 16#100000001B3,
    UKey = KeyInt band Mask64,
    UVal = ValInt band Mask64,
    UVer = VerInt band Mask64,

    H0 = 16#CBF29CE484222325,
    H1 = ((H0 bxor UKey) * Prime) band Mask64,
    H2 = ((H1 bxor UVal) * Prime) band Mask64,
    H3 = ((H2 bxor UVer) * Prime) band Mask64,
    H4 = (H3 bxor (H3 bsr 30)) band Mask64,
    H5 = (H4 * 16#BF58476D1CE4E5B9) band Mask64,
    H6 = (H5 bxor (H5 bsr 27)) band Mask64,
    H7 = (H6 * 16#94D049BB133111EB) band Mask64,
    H8 = (H7 bxor (H7 bsr 31)) band Mask64,
    to_signed_64(H8).

to_signed_64(U) when U >= 16#8000000000000000 ->
    U - 16#10000000000000000;
to_signed_64(U) ->
    U.

namespace_id_64(<<"default">>) ->
    0;
namespace_id_64(Ns) when is_binary(Ns) ->
    <<Int32:32/big-unsigned-integer, _/binary>> = crypto:hash(sha256, Ns),
    1000 + (Int32 rem 100000000);
namespace_id_64(_) ->
    0.

bytes_to_u64(Bin) when is_binary(Bin) ->
    <<Int64:64/big-unsigned-integer, _/binary>> = crypto:hash(sha256, Bin),
    Int64.

compute_incremental_root(Event, CurrentRoot) ->
    Op = maps:get(<<"action">>, Event, maps:get(<<"op">>, Event, <<"put">>)),
    case Op of
        <<"register_schema">> ->
            CurrentRoot;
        _ ->
            RawKey = maps:get(<<"key">>, Event, undefined),
            case RawKey of
                undefined -> CurrentRoot;
                _ ->
                    Namespace = maps:get(<<"namespace">>, Event, <<"default">>),
                    Backend = maps:get(<<"backend">>, Event, <<"wasm_slot">>),
                    {MetaKey, CompKey} = case Backend of
                        <<"ets_doc">> ->
                            KeyBin = to_bin_spec(RawKey),
                            {KeyBin, bytes_to_u64(iolist_to_binary([Namespace, <<":">>, KeyBin]))};
                        _ ->
                            KeyInt = to_integer(RawKey, 0),
                            NsId = namespace_id_64(Namespace),
                            CK = if NsId =:= 0 -> KeyInt; true -> NsId bxor KeyInt end,
                            {KeyInt, CK}
                    end,
                    OldRec = case ets:lookup(?TABLE, {record_meta, Namespace, MetaKey}) of
                        [{{record_meta, Namespace, MetaKey}, Meta}] -> Meta;
                        _ -> undefined
                    end,
                    case Op of
                        <<"del">> ->
                            case OldRec of
                                {OldVal, OldVer} ->
                                    OldHash = canonical_hash_64(CompKey, OldVal, OldVer),
                                    to_signed_64((CurrentRoot bxor OldHash) band 16#FFFFFFFFFFFFFFFF);
                                _ ->
                                    CurrentRoot
                            end;
                        _ ->
                            NewVal = case Backend of
                                <<"ets_doc">> ->
                                    RawV = maps:get(<<"val">>, Event, <<>>),
                                    CanonBin = maps:get(<<"canonical_val">>, Event,
                                        if is_binary(RawV) -> RawV;
                                           is_integer(RawV) -> integer_to_binary(RawV);
                                           is_boolean(RawV) -> atom_to_binary(RawV, utf8);
                                           is_map(RawV) -> iolist_to_binary(json:encode(RawV));
                                           true -> <<>>
                                        end),
                                    bytes_to_u64(to_bin_spec(CanonBin));
                                _ ->
                                    to_integer(maps:get(<<"val">>, Event, 0), 0)
                            end,
                            NewVer = to_integer(maps:get(<<"ver">>, Event, 1), 1),
                            NewHash = canonical_hash_64(CompKey, NewVal, NewVer),
                            OldHash = case OldRec of
                                {OldVal, OldVer} -> canonical_hash_64(CompKey, OldVal, OldVer);
                                _ -> 0
                            end,
                            to_signed_64(((CurrentRoot bxor OldHash) bxor NewHash) band 16#FFFFFFFFFFFFFFFF)
                    end
            end
    end.

recover_from_wal(Path) ->
    case file:read_file(Path) of
        {ok, Content} ->
            Lines = binary:split(Content, <<"\n">>, [global, trim_all]),
            validate_and_apply_wal_lines(Lines, 1, ?GENESIS_HASH, 1, 0, 0);
        _ -> 0
    end.

validate_and_apply_wal_lines([], _ExpectedSeq, _LastHash, _LastEpoch, AppliedCount, AccRoot) ->
    ets:insert(?TABLE, {state_root, AccRoot}),
    AppliedCount;
validate_and_apply_wal_lines([Line | Rest], ExpectedSeq, LastHash, LastEpoch, AppliedCount, AccRoot) ->
    case catch json:decode(Line) of
        Event when is_map(Event) ->
            Seq = to_integer(maps:get(<<"seq">>, Event, 0), 0),
            EvEpoch = to_integer(maps:get(<<"epoch">>, Event, 1), 1),
            GivenHash = maps:get(<<"hash">>, Event, <<>>),
            GivenPrevHash = maps:get(<<"prev_hash">>, Event, <<>>),
            ExpectedHash = compute_event_hash(Event, LastHash),
            HashMatches = (string:uppercase(GivenHash) =:= string:uppercase(ExpectedHash)),
            PrevHashMatches = (string:uppercase(GivenPrevHash) =:= string:uppercase(LastHash)),

            ClaimedRootBin = maps:get(<<"state_root">>, Event, undefined),
            ClaimedRoot = case ClaimedRootBin of
                undefined -> undefined;
                RInt when is_integer(RInt) -> RInt;
                RBin when is_binary(RBin) -> to_integer(RBin, undefined)
            end,

            NewAccRoot = compute_incremental_root(Event, AccRoot),
            RootMatches = (ClaimedRoot =:= undefined orelse ClaimedRoot =:= NewAccRoot),

            if
                Seq =/= ExpectedSeq ->
                    mark_recovery_failure({sequence_gap, Seq, ExpectedSeq}),
                    AppliedCount;
                EvEpoch < LastEpoch ->
                    mark_recovery_failure({epoch_regression, EvEpoch, LastEpoch}),
                    AppliedCount;
                not PrevHashMatches ->
                    mark_recovery_failure({broken_hash_chain, GivenPrevHash, LastHash}),
                    AppliedCount;
                not HashMatches ->
                    mark_recovery_failure({hash_mismatch, GivenHash, ExpectedHash}),
                    AppliedCount;
                not RootMatches ->
                    mark_recovery_failure({state_root_mismatch, ClaimedRoot, NewAccRoot}),
                    AppliedCount;
                true ->
                    apply_event_to_ets(Event, NewAccRoot),
                    validate_and_apply_wal_lines(Rest, ExpectedSeq + 1, GivenHash, max(LastEpoch, EvEpoch), AppliedCount + 1, NewAccRoot)
            end;
        _CorruptOrTruncated ->
            case Rest of
                [] ->
                    %% Clean EOF truncation due to crash mid-write
                    ets:insert(?TABLE, {wal_recovery_truncated_detected, true}),
                    ets:insert(?TABLE, {state_root, AccRoot}),
                    AppliedCount;
                _ ->
                    %% Mid-file corruption! Subsequent lines exist after corrupt record
                    mark_recovery_failure({mid_file_corruption, Line}),
                    AppliedCount
            end
    end.

apply_event_to_ets(Event, ComputedRoot) ->
    Seq = to_integer(maps:get(<<"seq">>, Event, 0), 0),
    Epoch = to_integer(maps:get(<<"epoch">>, Event, 1), 1),
    Namespace = maps:get(<<"namespace">>, Event, <<"default">>),
    CurrEpoch = lookup_val(epoch, 1),
    ets:insert(?TABLE, {epoch, max(CurrEpoch, Epoch)}),
    ets:insert(?TABLE, {tick, Seq}),
    ets:insert(?TABLE, {state_root, ComputedRoot}),
    RootBin = integer_to_binary(ComputedRoot),
    case maps:get(<<"hash">>, Event, undefined) of
        undefined -> ok;
        Hash ->
            ets:insert(?TABLE, {latest_hash, Hash}),
            ets:insert(?TABLE, {{event_hash, Seq}, Hash})
    end,
    ets:insert(?TABLE, {{wal_event, Seq}, Event#{ <<"state_root">> => RootBin }}),
    ets:update_counter(?TABLE, total_replicated, 1, {total_replicated, 0}),
    Op = maps:get(<<"action">>, Event, maps:get(<<"op">>, Event, <<"put">>)),
    case Op of
        <<"register_schema">> ->
            SchemaDef = maps:get(<<"val">>, Event, #{}),
            ets:insert(?TABLE, {{schema_ns, Namespace}, SchemaDef});
        _ ->
            case maps:get(<<"key">>, Event, undefined) of
                undefined -> ok;
                Key0 ->
                    Backend = maps:get(<<"backend">>, Event, <<"wasm_slot">>),
                    MetaKey = case Backend of
                        <<"ets_doc">> -> to_bin_spec(Key0);
                        _ -> to_integer(Key0, 0)
                    end,
                    Key = case Namespace of
                        <<"default">> -> to_integer(Key0, Key0);
                        _ -> Key0
                    end,
                    case Op of
                        <<"del">> ->
                            ets:delete(?TABLE, {record_meta, Namespace, MetaKey}),
                            ets:delete(?TABLE, {kv_ns, Namespace, Key}),
                            if Namespace =:= <<"default">> -> ets:delete(?TABLE, {kv, Key}); true -> ok end;
                        _ ->
                            Val = maps:get(<<"val">>, Event, 0),
                            ValMeta = case Backend of
                                <<"ets_doc">> ->
                                    CanonBin = maps:get(<<"canonical_val">>, Event,
                                        if is_binary(Val) -> Val;
                                           is_integer(Val) -> integer_to_binary(Val);
                                           is_boolean(Val) -> atom_to_binary(Val, utf8);
                                           is_map(Val) -> iolist_to_binary(json:encode(Val));
                                           true -> <<>>
                                        end),
                                    bytes_to_u64(to_bin_spec(CanonBin));
                                _ ->
                                    to_integer(Val, 0)
                            end,
                            Ver = to_integer(maps:get(<<"ver">>, Event, 1), 1),
                            ets:insert(?TABLE, {{record_meta, Namespace, MetaKey}, {ValMeta, Ver}}),
                            ets:insert(?TABLE, {{kv_ns, Namespace, Key}, Val}),
                            if Namespace =:= <<"default">> -> ets:insert(?TABLE, {{kv, Key}, Val}); true -> ok end
                    end
            end
    end.

reset() ->
    ensure_started(),
    gen_server:call(?SERVER, reset, 5000).

get_tick() ->
    ensure_started(),
    gen_server:call(?SERVER, get_tick, 5000).

get_state() ->
    ensure_started(),
    gen_server:call(?SERVER, get_state, 5000).

get_key(Key) when is_integer(Key) ->
    ensure_started(),
    gen_server:call(?SERVER, {get_key, Key}, 5000).

get_wal() ->
    ensure_started(),
    gen_server:call(?SERVER, get_wal, 5000).

get_wal_batch(FromSeq, ToSeq) ->
    ensure_started(),
    gen_server:call(?SERVER, {get_wal_batch, FromSeq, ToSeq}, 10000).

get_status() ->
    ensure_started(),
    gen_server:call(?SERVER, get_status, 5000).

get_status_map() ->
    ensure_started(),
    Epoch = lookup_val(epoch, 1),
    Tick = lookup_val(tick, 0),
    Root = lookup_val(state_root, 0),
    Connected = lookup_val(connected_to_hot, false),
    Total = lookup_val(total_replicated, 0),
    FencedRej = lookup_val(fenced_rejections, 0),
    GapRej = lookup_val(gap_rejections, 0),
    HashRej = lookup_val(hash_rejections, 0),
    DegradedRej = lookup_val(degraded_rejections, 0),
    WalCorrupted = lookup_val(wal_corrupted, false),
    HaltReason = case ets:lookup(?TABLE, wal_recovery_halt_reason) of
        [{wal_recovery_halt_reason, none}] -> <<"none">>;
        [{wal_recovery_halt_reason, R}] -> list_to_binary(io_lib:format("~p", [R]));
        _ -> <<"none">>
    end,
    Truncated = case ets:lookup(?TABLE, wal_recovery_truncated_detected) of
        [{wal_recovery_truncated_detected, T}] -> T;
        _ -> false
    end,
    Health = case WalCorrupted of true -> <<"degraded">>; false -> <<"healthy">> end,
    Ready = not WalCorrupted,
    #{
        <<"ok">> => true,
        <<"role">> => <<"SD-HyperBEAM-Node">>,
        <<"node_name">> => <<"hb_su">>,
        <<"device">> => <<"sd@1.0">>,
        <<"health">> => Health,
        <<"ready">> => Ready,
        <<"wal_corrupted">> => WalCorrupted,
        <<"halt_reason">> => HaltReason,
        <<"truncated_detected">> => Truncated,
        <<"connected_to_hot">> => Connected,
        <<"epoch">> => Epoch,
        <<"latest_tick_ram">> => Tick,
        <<"latest_tick">> => Tick,
        <<"state_root_ram">> => Root,
        <<"state_root_str">> => integer_to_binary(Root),
        <<"total_replicated">> => Total,
        <<"fenced_rejections">> => FencedRej,
        <<"gap_rejections">> => GapRej,
        <<"hash_rejections">> => HashRej,
        <<"degraded_rejections">> => DegradedRej,
        <<"wal_file">> => list_to_binary(wal_path())
    }.

%% HyperBEAM Request Handlers for ~sd@1.0

get_tick_from_req(_Base, _Req, _Opts) ->
    ensure_started(),
    case get_tick() of
        {ok, Res} -> Res;
        Res -> Res
    end.

get_state_from_req(_Base, _Req, _Opts) ->
    ensure_started(),
    case get_state() of
        {ok, Res} -> Res;
        Res -> Res
    end.

get_key_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    Key = parse_int_field(<<"key">>, Req, 1),
    case get_key(Key) of
        {ok, Res} -> Res;
        {error, not_found} ->
            #{ <<"ok">> => false, <<"status">> => 404, <<"error">> => <<"key_not_found">> };
        Res -> Res
    end.

get_wal_from_req(_Base, _Req, _Opts) ->
    ensure_started(),
    case get_wal() of
        {ok, Res} -> Res;
        Res -> Res
    end.

get_wal_batch_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    FromSeq = parse_int_field(<<"from_seq">>, Req, 1),
    CurrTick = lookup_val(tick, 0),
    ToSeq = parse_int_field(<<"to_seq">>, Req, CurrTick),
    get_wal_batch(FromSeq, ToSeq).

get_status_from_req(_Base, _Req, _Opts) ->
    ensure_started(),
    get_status_map().

fence_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    case verify_admin_req(Req) of
        {error, ErrMap} -> ErrMap;
        ok ->
            CurrEpoch = lookup_val(epoch, 1),
            NewEpoch = parse_int_field(<<"epoch">>, Req, CurrEpoch),
            fence_epoch(NewEpoch)
    end.

restart_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    case verify_admin_req(Req) of
        {error, ErrMap} -> ErrMap;
        ok ->
            simulate_restart_from_wal()
    end.

reset_from_req(_Base, Req, _Opts) ->
    ensure_started(),
    case verify_admin_req(Req) of
        {error, ErrMap} -> ErrMap;
        ok ->
            case is_test_profile() of
                false ->
                    #{
                        <<"ok">> => false,
                        <<"status">> => 403,
                        <<"error_code">> => <<"ERR_RESET_DISABLED">>,
                        <<"error">> => <<"Reset endpoint is strictly disabled outside test profile">>
                    };
                true ->
                    case reset() of
                        {ok, Res} -> Res;
                        Res -> Res
                    end
            end
    end.

%% Compatibility handler for ~scheduler@1.0 backward routing
handle_scheduler_http(<<"wal_batch">>, Base, Req, Opts) ->
    {ok, get_wal_batch_from_req(Base, Req, Opts)};
handle_scheduler_http(<<"key">>, Base, Req, Opts) ->
    {ok, get_key_from_req(Base, Req, Opts)};
handle_scheduler_http(<<"fence_epoch">>, Base, Req, Opts) ->
    {ok, fence_from_req(Base, Req, Opts)};
handle_scheduler_http(<<"simulate_restart">>, Base, Req, Opts) ->
    {ok, restart_from_req(Base, Req, Opts)};
handle_scheduler_http(<<"reset">>, Base, Req, Opts) ->
    {ok, reset_from_req(Base, Req, Opts)};
handle_scheduler_http(<<"tick">>, Base, Req, Opts) ->
    {ok, get_tick_from_req(Base, Req, Opts)};
handle_scheduler_http(<<"state">>, Base, Req, Opts) ->
    {ok, get_state_from_req(Base, Req, Opts)};
handle_scheduler_http(<<"wal">>, Base, Req, Opts) ->
    {ok, get_wal_from_req(Base, Req, Opts)};
handle_scheduler_http(_, Base, Req, Opts) ->
    {ok, get_status_from_req(Base, Req, Opts)}.

%% GenServer Callbacks

handle_call({fence_epoch, NewEpoch}, _From, State) ->
    CurrEpoch = lookup_val(epoch, 1),
    if NewEpoch < CurrEpoch ->
        {reply, #{ <<"ok">> => false, <<"error">> => <<"epoch_cannot_decrease">>, <<"epoch">> => CurrEpoch }, State};
       true ->
        ets:insert(?TABLE, {epoch, NewEpoch}),
        {reply, #{ <<"ok">> => true, <<"previous_epoch">> => CurrEpoch, <<"epoch">> => NewEpoch }, State}
    end;

handle_call(recover_wal, _From, State) ->
    close_wal_fd(),
    Count = recover_from_wal(wal_path()),
    Tick = lookup_val(tick, 0),
    Root = lookup_val(state_root, 0),
    {reply, #{ <<"ok">> => true, <<"recovered_entries">> => Count, <<"tick">> => Tick, <<"state_root">> => Root, <<"state_root_str">> => integer_to_binary(Root) }, State};

handle_call(simulate_restart_from_wal, _From, State) ->
    close_wal_fd(),
    Conn = lookup_val(connected_to_hot, false),
    ets:delete_all_objects(?TABLE),
    reset_ets_defaults(),
    ets:insert(?TABLE, {connected_to_hot, Conn}),
    Count = recover_from_wal(wal_path()),
    Tick = lookup_val(tick, 0),
    Root = lookup_val(state_root, 0),
    Epoch = lookup_val(epoch, 1),
    KVCount = ets:foldl(fun({{kv, _}, _}, Acc) -> Acc + 1; (_, Acc) -> Acc end, 0, ?TABLE),
    WalCorrupted = lookup_val(wal_corrupted, false),
    HaltReason = case ets:lookup(?TABLE, wal_recovery_halt_reason) of
        [{wal_recovery_halt_reason, none}] -> <<"none">>;
        [{wal_recovery_halt_reason, R}] -> list_to_binary(io_lib:format("~p", [R]));
        _ -> <<"none">>
    end,
    Truncated = case ets:lookup(?TABLE, wal_recovery_truncated_detected) of
        [{wal_recovery_truncated_detected, T}] -> T;
        _ -> false
    end,
    Health = case WalCorrupted of true -> <<"degraded">>; false -> <<"healthy">> end,
    Ready = not WalCorrupted,
    {reply, #{
        <<"ok">> => true,
        <<"health">> => Health,
        <<"ready">> => Ready,
        <<"wal_corrupted">> => WalCorrupted,
        <<"recovered_entries">> => Count,
        <<"epoch">> => Epoch,
        <<"tick">> => Tick,
        <<"state_root">> => Root,
        <<"state_root_str">> => integer_to_binary(Root),
        <<"records_count">> => KVCount,
        <<"halt_reason">> => HaltReason,
        <<"truncated_detected">> => Truncated,
        <<"source">> => <<"WAL_TO_RAM_RECOVERY">>
    }, State};

handle_call(get_tick, _From, State) ->
    Epoch = lookup_val(epoch, 1),
    Tick = lookup_val(tick, 0),
    Root = lookup_val(state_root, 0),
    Hash = lookup_val(latest_hash, <<>>),
    KVCount = ets:foldl(fun({{kv, _}, _}, Acc) -> Acc + 1; (_, Acc) -> Acc end, 0, ?TABLE),
    Reply = #{
        <<"ok">> => true,
        <<"epoch">> => Epoch,
        <<"tick">> => Tick,
        <<"latest_tick">> => Tick,
        <<"state_root">> => Root,
        <<"state_root_str">> => integer_to_binary(Root),
        <<"latest_hash">> => Hash,
        <<"records_count">> => KVCount,
        <<"source">> => <<"RAM">>
    },
    {reply, Reply, State};

handle_call(get_state, _From, State) ->
    Epoch = lookup_val(epoch, 1),
    Tick = lookup_val(tick, 0),
    Root = lookup_val(state_root, 0),
    AllKV = ets:foldl(fun({{kv, K}, V}, Acc) ->
                          Acc#{ integer_to_binary(K) => V };
                         (_, Acc) -> Acc
                      end, #{}, ?TABLE),
    Reply = #{
        <<"ok">> => true,
        <<"epoch">> => Epoch,
        <<"tick">> => Tick,
        <<"latest_tick">> => Tick,
        <<"state_root">> => Root,
        <<"state_root_str">> => integer_to_binary(Root),
        <<"kv">> => AllKV,
        <<"records">> => AllKV,
        <<"total_keys">> => maps:size(AllKV),
        <<"records_count">> => maps:size(AllKV),
        <<"source">> => <<"RAM">>
    },
    {reply, Reply, State};

handle_call({get_key, Key}, _From, State) ->
    case ets:lookup(?TABLE, {kv, Key}) of
        [{{kv, Key}, Val}] ->
            Reply = #{
                <<"ok">> => true,
                <<"key">> => Key,
                <<"val">> => Val,
                <<"found">> => true,
                <<"source">> => <<"RAM">>
            },
            {reply, Reply, State};
        [] ->
            {reply, {error, not_found}, State}
    end;

handle_call({get_wal_batch, FromSeq0, ToSeq0}, _From, State) ->
    CurrTick = lookup_val(tick, 0),
    FromSeq = max(1, FromSeq0),
    ToSeq = if ToSeq0 =< 0 -> CurrTick; true -> min(ToSeq0, CurrTick) end,
    Events = if FromSeq =< ToSeq andalso ToSeq > 0 ->
        lists:filtermap(fun(S) ->
            case ets:lookup(?TABLE, {wal_event, S}) of
                [{{wal_event, S}, Ev}] -> {true, Ev};
                [] -> false
            end
        end, lists:seq(FromSeq, ToSeq));
       true -> []
    end,
    SuId = <<"SU_NODE_1">>,
    {BatchEpoch, ActualFrom, ActualTo, PropHash, PropRootBin} = case Events of
        [] ->
            {lookup_val(epoch, 1), FromSeq, ToSeq, lookup_val(latest_hash, ?GENESIS_HASH), integer_to_binary(lookup_val(state_root, 0))};
        [FirstEv | _] ->
            LastEv = lists:last(Events),
            Ep = to_integer(maps:get(<<"epoch">>, LastEv, lookup_val(epoch, 1)), 1),
            FSeq = to_integer(maps:get(<<"seq">>, FirstEv, FromSeq), FromSeq),
            TSeq = to_integer(maps:get(<<"seq">>, LastEv, ToSeq), ToSeq),
            PHash = maps:get(<<"hash">>, LastEv, ?GENESIS_HASH),
            PRoot0 = maps:get(<<"state_root">>, LastEv, <<"0">>),
            PRootB = if is_integer(PRoot0) -> integer_to_binary(PRoot0);
                        is_binary(PRoot0) -> PRoot0;
                        true -> <<"0">>
                     end,
            {Ep, FSeq, TSeq, PHash, PRootB}
    end,
    BatchSig = compute_batch_sig(SuId, BatchEpoch, ActualFrom, ActualTo, PropHash, PropRootBin),
    EventsJson = try iolist_to_binary(json:encode(Events)) catch _:_ -> <<"[]">> end,
    Reply = #{
        <<"ok">> => true,
        <<"su_id">> => SuId,
        <<"sd_id">> => <<"SD_NODE_1">>,
        <<"epoch">> => BatchEpoch,
        <<"from_seq">> => ActualFrom,
        <<"to_seq">> => ActualTo,
        <<"proposed_hash">> => PropHash,
        <<"proposed_state_root">> => PropRootBin,
        <<"batch_sig">> => BatchSig,
        <<"events_count">> => length(Events),
        <<"events_json">> => EventsJson
    },
    {reply, Reply, State};

handle_call(get_wal, _From, State) ->
    Entries = read_wal_entries(wal_path()),
    Reply = #{
        <<"ok">> => true,
        <<"wal_file">> => list_to_binary(wal_path()),
        <<"count">> => length(Entries),
        <<"total_lines">> => length(Entries),
        <<"entries">> => Entries
    },
    {reply, Reply, State};

handle_call(get_status, _From, State) ->
    {reply, get_status_map(), State};

handle_call(reset, _From, State) ->
    close_wal_fd(),
    Conn = lookup_val(connected_to_hot, false),
    ets:delete_all_objects(?TABLE),
    reset_ets_defaults(),
    ets:insert(?TABLE, {connected_to_hot, Conn}),
    _ = file:delete(wal_path()),
    {reply, #{ <<"ok">> => true, <<"reset">> => true }, State};

handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    close_wal_fd(),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Mutual HMAC-SHA256 Node Authentication

node_auth_secret() ->
    case ets:info(?TABLE) of
        undefined -> compute_node_auth_secret();
        _ ->
            case ets:lookup(?TABLE, cached_auth_secret) of
                [{cached_auth_secret, Sec}] -> Sec;
                [] ->
                    Sec = compute_node_auth_secret(),
                    ets:insert(?TABLE, {cached_auth_secret, Sec}),
                    Sec
            end
    end.

compute_node_auth_secret() ->
    WalletPath = case os:getenv("WALLET_PATH") of
        false -> filename:join([get_db_dir(), "secrets/wallet.json"]);
        "" -> filename:join([get_db_dir(), "secrets/wallet.json"]);
        WP -> WP
    end,
    case file:read_file(WalletPath) of
        {ok, Bin} when byte_size(Bin) > 0 ->
            crypto:hash(sha256, Bin);
        _ ->
            case os:getenv("HYDB_CLUSTER_SECRET") of
                false ->
                    throw({error, missing_cluster_auth_secret});
                EnvSec when is_list(EnvSec), length(EnvSec) > 0 ->
                    crypto:hash(sha256, list_to_binary(EnvSec));
                _ ->
                    throw({error, missing_cluster_auth_secret})
            end
    end.

compute_batch_sig(SdId, Epoch, FromSeq, ToSeq, PropHash, PropRootBin) ->
    UpperHash = string:uppercase(PropHash),
    Msg = iolist_to_binary([
        SdId, <<":">>,
        integer_to_binary(Epoch), <<":">>,
        integer_to_binary(FromSeq), <<":">>,
        integer_to_binary(ToSeq), <<":">>,
        UpperHash, <<":">>,
        PropRootBin
    ]),
    binary:encode_hex(crypto:mac(hmac, sha256, node_auth_secret(), Msg)).

subscriber_loop(Server, Host, Port) ->
    case gen_tcp:connect(Host, Port, [binary, {packet, 4}, {active, true}, {reuseaddr, true}, {nodelay, true}], 2000) of
        {ok, Socket} ->
            case sd_perform_handshake(Socket) of
                ok ->
                    ets:insert(?TABLE, {connected_to_hot, true}),
                    sd_socket_loop(Socket, Server, Host, Port);
                error ->
                    gen_tcp:close(Socket),
                    ets:insert(?TABLE, {connected_to_hot, false}),
                    timer:sleep(500),
                    subscriber_loop(Server, Host, Port)
            end;
        {error, _Reason} ->
            ets:insert(?TABLE, {connected_to_hot, false}),
            timer:sleep(500),
            subscriber_loop(Server, Host, Port)
    end.

sd_perform_handshake(Socket) ->
    receive
        {tcp, Socket, Data} ->
            try json:decode(Data) of
                #{ <<"type">> := <<"AUTH_CHALLENGE">>, <<"nonce">> := Nonce } when is_binary(Nonce) ->
                    NodeId = <<"hb_su">>,
                    Msg = <<Nonce/binary, ":", NodeId/binary>>,
                    Sig = binary:encode_hex(crypto:mac(hmac, sha256, node_auth_secret(), Msg)),
                    Resp = iolist_to_binary(json:encode(#{
                        <<"type">> => <<"AUTH_RESPONSE">>,
                        <<"node_id">> => NodeId,
                        <<"sig">> => Sig,
                        <<"last_tick">> => lookup_val(tick, 0)
                    })),
                    ok = gen_tcp:send(Socket, Resp),
                    receive
                        {tcp, Socket, AckData} ->
                            case json:decode(AckData) of
                                #{ <<"type">> := <<"AUTH_OK">> } -> ok;
                                _ -> error
                            end;
                        {tcp_closed, Socket} -> error;
                        {tcp_error, Socket, _} -> error
                    after 2000 ->
                        error
                    end;
                _ ->
                    error
            catch _:_ ->
                error
            end;
        {tcp_closed, Socket} -> error;
        {tcp_error, Socket, _} -> error
    after 2000 ->
        error
    end.

sd_socket_loop(Socket, Server, Host, Port) ->
    receive
        {tcp, Socket, Data} ->
            handle_incoming_event(Socket, Data),
            sd_socket_loop(Socket, Server, Host, Port);
        {tcp_closed, Socket} ->
            ets:insert(?TABLE, {connected_to_hot, false}),
            timer:sleep(500),
            subscriber_loop(Server, Host, Port);
        {tcp_error, Socket, _Reason} ->
            gen_tcp:close(Socket),
            ets:insert(?TABLE, {connected_to_hot, false}),
            timer:sleep(500),
            subscriber_loop(Server, Host, Port);
        _Other ->
            sd_socket_loop(Socket, Server, Host, Port)
    end.

handle_incoming_event(Socket, RawData) ->
    try
        Event = json:decode(RawData),
        Seq = to_integer(maps:get(<<"seq">>, Event, 0), 0),
        EvEpoch = to_integer(maps:get(<<"epoch">>, Event, 1), 1),
        MinEpoch = lookup_val(epoch, 1),
        LastTick = lookup_val(tick, 0),
        LastHash = lookup_val(latest_hash, ?GENESIS_HASH),

        %% 0. FAIL-CLOSED CHECK: Reject incoming writes if WAL recovery failed or node is degraded
        case is_degraded() of
            true ->
                ets:update_counter(?TABLE, degraded_rejections, 1, {degraded_rejections, 0}),
                ok;
            false ->
                %% 1. EPOCH FENCING CHECK: Reject stale Hot Node from older epoch
                if EvEpoch < MinEpoch ->
                    ets:update_counter(?TABLE, fenced_rejections, 1, {fenced_rejections, 0}),
                    ok;
                   Seq =< LastTick ->
                    case ets:lookup(?TABLE, {event_hash, Seq}) of
                        [{{event_hash, Seq}, ExistingHash}] ->
                            GivenHash = maps:get(<<"hash">>, Event, <<>>),
                            if ExistingHash == GivenHash ->
                                gen_tcp:send(Socket, <<Seq:32/big>>);
                               true ->
                                ets:update_counter(?TABLE, hash_rejections, 1, {hash_rejections, 0}),
                                ok
                            end;
                        [] ->
                            ok
                    end;
                   Seq > (LastTick + 1) ->
                    ets:update_counter(?TABLE, gap_rejections, 1, {gap_rejections, 0}),
                    ok;
                   true ->
                    ExpectedHash = compute_event_hash(Event, LastHash),
                    GivenHashRaw = maps:get(<<"hash">>, Event, <<>>),
                    HashMatches = (ExpectedHash =:= GivenHashRaw) orelse
                                  (string:uppercase(ExpectedHash) =:= string:uppercase(GivenHashRaw)),

                    if not HashMatches ->
                        ets:update_counter(?TABLE, hash_rejections, 1, {hash_rejections, 0}),
                        ok;
                       true ->
                        WalFd = get_or_open_wal_fd(),
                        ok = file:write(WalFd, [RawData, <<"\n">>]),
                        ok = file:datasync(WalFd),
                        CurrRoot = lookup_val(state_root, 0),
                        ComputedRoot = compute_incremental_root(Event, CurrRoot),
                        apply_event_to_ets(Event, ComputedRoot),
                        gen_tcp:send(Socket, <<Seq:32/big>>),
                        ok
                    end
                end
        end
    catch
        _:_Err ->
            ok
    end.

compute_event_hash(Event, LastHash) ->
    GivenHash = maps:get(<<"hash">>, Event, maps:get(hash, Event, <<>>)),
    CanonicalHash = compute_canonical_event_hash(Event, LastHash),
    case GivenHash of
        CanonicalHash ->
            CanonicalHash;
        _ ->
            LegacyHash = compute_legacy_event_hash(Event, LastHash),
            case GivenHash of
                LegacyHash ->
                    LegacyHash;
                _ ->
                    CanonicalHash
            end
    end.

compute_canonical_event_hash(Event) when is_map(Event) ->
    LastHash = maps:get(prev_hash, Event, maps:get(<<"prev_hash">>, Event, ?GENESIS_HASH)),
    compute_canonical_event_hash(Event, LastHash).

compute_canonical_event_hash(Event, LastHash) when is_map(Event) ->
    ContractVsn = maps:get(contract_version, Event, maps:get(<<"contract_version">>, Event, <<"hydb.core@1.0">>)),
    PrevHashHex = maps:get(prev_hash, Event, maps:get(<<"prev_hash">>, Event, LastHash)),
    PrevHashBin = hex_to_bin_32(PrevHashHex),
    Epoch = to_int_spec(maps:get(epoch, Event, maps:get(<<"epoch">>, Event, 1))),
    Seq = to_int_spec(maps:get(seq, Event, maps:get(<<"seq">>, Event, 1))),
    RawOp = maps:get(op, Event, maps:get(<<"op">>, Event, maps:get(action, Event, maps:get(<<"action">>, Event, <<"put">>)))),
    Op = case RawOp of
        <<"register_schema">> -> <<"register">>;
        register_schema -> <<"register">>;
        OtherOp -> to_bin_spec(OtherOp)
    end,
    Ns = to_bin_spec(maps:get(namespace, Event, maps:get(<<"namespace">>, Event, <<"default">>))),
    SchemaVsn = to_bin_spec(maps:get(schema_version, Event, maps:get(<<"schema_version">>, Event, <<"1.0.0">>))),
    RawKey = maps:get(key, Event, maps:get(<<"key">>, Event, absent)),
    NormKey = normalize_key_spec(RawKey, Op),
    ValType = to_bin_spec(maps:get(value_type, Event, maps:get(<<"val_type">>, Event, maps:get(<<"value_type">>, Event, maps:get(val_type, Event, <<"int64">>))))),
    RawVal = maps:get(value, Event, maps:get(<<"val">>, Event, maps:get(val, Event, maps:get(<<"value">>, Event, absent)))),
    NormVal = normalize_val_spec(RawVal, ValType, Op),
    Ver = to_int_spec(maps:get(ver, Event, maps:get(<<"ver">>, Event, 1))),

    FrameHeader = <<"HYDBE", 1>>,
    Bytes = iolist_to_binary([
        FrameHeader,
        encode_spec_val({string, to_bin_spec(ContractVsn)}),
        encode_spec_val({bytes, PrevHashBin}),
        encode_spec_val({int64, Epoch}),
        encode_spec_val({int64, Seq}),
        encode_spec_val({string, Op}),
        encode_spec_val({string, Ns}),
        encode_spec_val({string, SchemaVsn}),
        encode_spec_val(NormKey),
        encode_spec_val({string, ValType}),
        encode_spec_val(NormVal),
        encode_spec_val({int64, Ver})
    ]),
    binary:encode_hex(crypto:hash(sha256, Bytes)).

hex_to_bin_32(Hex) when is_binary(Hex), byte_size(Hex) =:= 64 ->
    try binary:decode_hex(Hex) catch _:_ -> <<0:256>> end;
hex_to_bin_32(Bin) when is_binary(Bin), byte_size(Bin) =:= 32 ->
    Bin;
hex_to_bin_32(_) ->
    <<0:256>>.

to_int_spec(I) when is_integer(I) -> I;
to_int_spec(B) when is_binary(B) ->
    try binary_to_integer(B) catch _:_ -> 0 end;
to_int_spec(L) when is_list(L) ->
    try list_to_integer(L) catch _:_ -> 0 end;
to_int_spec(_) -> 0.

to_bin_spec(B) when is_binary(B) -> B;
to_bin_spec(A) when is_atom(A) -> atom_to_binary(A, utf8);
to_bin_spec(L) when is_list(L) -> list_to_binary(L);
to_bin_spec(I) when is_integer(I) -> integer_to_binary(I);
to_bin_spec(_) -> <<>>.

normalize_key_spec(absent, _) -> absent;
normalize_key_spec(undefined, _) -> absent;
normalize_key_spec(_, <<"register">>) -> absent;
normalize_key_spec({int64, I}, _) -> {int64, I};
normalize_key_spec({string, S}, _) -> {string, S};
normalize_key_spec(I, _) when is_integer(I) -> {int64, I};
normalize_key_spec(S, _) when is_binary(S) ->
    case S of
        <<"__schema__">> -> absent;
        _ -> {string, S}
    end;
normalize_key_spec(Other, _) -> Other.

normalize_val_spec(absent, _, _) -> absent;
normalize_val_spec(undefined, _, _) -> absent;
normalize_val_spec(_, _, <<"del">>) -> absent;
normalize_val_spec({int64, I}, _, _) -> {int64, I};
normalize_val_spec({bool, B}, _, _) -> {bool, B};
normalize_val_spec({string, S}, _, _) -> {string, S};
normalize_val_spec({object, O}, _, _) -> {object, O};
normalize_val_spec({array, A}, _, _) -> {array, A};
normalize_val_spec(I, <<"int64">>, _) when is_integer(I) -> {int64, I};
normalize_val_spec(B, <<"bool">>, _) when is_boolean(B) -> {bool, B};
normalize_val_spec(S, <<"string">>, _) when is_binary(S) -> {string, S};
normalize_val_spec(Map, <<"schema">>, _) when is_map(Map) ->
    schema_map_to_h(Map);
normalize_val_spec(Other, _, _) -> Other.

schema_map_to_h(Schema) when is_map(Schema) ->
    case code:ensure_loaded(hydb_spec) of
        {module, hydb_spec} ->
            hydb_spec:schema_to_h(Schema);
        _ ->
            Backend = maps:get(<<"backend">>, Schema, maps:get(backend, Schema, <<"ets_doc">>)),
            KeyType = maps:get(<<"key_type">>, Schema, maps:get(key_type, Schema, <<"string">>)),
            Ns = maps:get(<<"namespace">>, Schema, maps:get(namespace, Schema, <<"default">>)),
            RequireAuth = maps:get(<<"require_auth">>, Schema, maps:get(require_auth, Schema, false)),
            Requires = maps:get(<<"requires">>, Schema, maps:get(requires, Schema, [])),
            Rules = maps:get(<<"rules">>, Schema, maps:get(rules, Schema, #{})),
            SchemaVsn = maps:get(<<"schema_version">>, Schema, maps:get(schema_version, Schema, <<"1.0.0">>)),
            ValType = maps:get(<<"value_type">>, Schema, maps:get(value_type, Schema, <<"int64">>)),
            ReqSorted = lists:sort([to_bin_spec(R) || R <- Requires]),
            RulesEntries = lists:map(fun({RK, RV}) ->
                RKBin = to_bin_spec(RK),
                RVEnc = if is_integer(RV) -> {int64, RV};
                           is_boolean(RV) -> {bool, RV};
                           is_binary(RV) -> {string, RV};
                           true -> RV
                        end,
                {RKBin, RVEnc}
            end, maps:to_list(Rules)),
            {object, [
                {<<"backend">>, {string, to_bin_spec(Backend)}},
                {<<"key_type">>, {string, to_bin_spec(KeyType)}},
                {<<"namespace">>, {string, to_bin_spec(Ns)}},
                {<<"require_auth">>, {bool, RequireAuth}},
                {<<"requires">>, {array, [{string, R} || R <- ReqSorted]}},
                {<<"rules">>, {object, RulesEntries}},
                {<<"schema_version">>, {string, to_bin_spec(SchemaVsn)}},
                {<<"value_type">>, {string, to_bin_spec(ValType)}}
            ]}
    end.

encode_spec_val(absent) -> <<8>>;
encode_spec_val(undefined) -> <<8>>;
encode_spec_val(null) -> <<5>>;
encode_spec_val({null}) -> <<5>>;
encode_spec_val({int64, Val}) when is_integer(Val) ->
    <<1, Val:64/big-signed-integer>>;
encode_spec_val({bool, true}) ->
    <<2, 1>>;
encode_spec_val({bool, false}) ->
    <<2, 0>>;
encode_spec_val({string, Str}) when is_binary(Str) ->
    Len = byte_size(Str),
    <<3, Len:32/big-unsigned-integer, Str/binary>>;
encode_spec_val({bytes, Bin}) when is_binary(Bin) ->
    Len = byte_size(Bin),
    <<4, Len:32/big-unsigned-integer, Bin/binary>>;
encode_spec_val({array, Elements}) when is_list(Elements) ->
    Count = length(Elements),
    EncodedElems = [encode_spec_val(El) || El <- Elements],
    iolist_to_binary([<<6, Count:32/big-unsigned-integer>>, EncodedElems]);
encode_spec_val({object, Pairs}) when is_list(Pairs) ->
    Sorted = lists:sort(fun({K1, _}, {K2, _}) ->
        to_bin_spec(K1) =< to_bin_spec(K2)
    end, Pairs),
    Count = length(Sorted),
    EncodedEntries = lists:map(fun({K, V}) ->
        KBin = to_bin_spec(K),
        KLen = byte_size(KBin),
        VEnc = encode_spec_val(V),
        <<KLen:32/big-unsigned-integer, KBin/binary, VEnc/binary>>
    end, Sorted),
    iolist_to_binary([<<7, Count:32/big-unsigned-integer>>, EncodedEntries]);
encode_spec_val(Map) when is_map(Map) ->
    encode_spec_val({object, maps:to_list(Map)});
encode_spec_val(I) when is_integer(I) ->
    encode_spec_val({int64, I});
encode_spec_val(B) when is_boolean(B) ->
    encode_spec_val({bool, B});
encode_spec_val(S) when is_binary(S) ->
    encode_spec_val({string, S}).

compute_legacy_event_hash(Event, LastHash) ->
    Seq = to_integer(maps:get(<<"seq">>, Event, maps:get(seq, Event, 0)), 0),
    Op = maps:get(<<"action">>, Event, maps:get(<<"op">>, Event, maps:get(action, Event, maps:get(op, Event, <<"put">>)))),
    Namespace = to_bin_spec(maps:get(<<"namespace">>, Event, maps:get(namespace, Event, <<"default">>))),
    ValType = to_bin_spec(maps:get(<<"val_type">>, Event, maps:get(val_type, Event, maps:get(value_type, Event, <<"int64">>)))),
    RawKey = maps:get(<<"key">>, Event, maps:get(key, Event, 1)),
    KeyBin = if is_integer(RawKey) -> integer_to_binary(RawKey);
                is_binary(RawKey) -> RawKey;
                true -> <<"1">>
             end,
    RawVal = maps:get(<<"val">>, Event, maps:get(val, Event, maps:get(value, Event, 0))),
    CanonicalValBin = maps:get(<<"canonical_val">>, Event,
        if is_integer(RawVal) -> integer_to_binary(RawVal);
           is_binary(RawVal) -> RawVal;
           true -> <<"0">>
        end),
    ActionCode = case Op of
        <<"register_schema">> -> <<"24">>;
        <<"register">> -> <<"24">>;
        register -> <<"24">>;
        <<"del">> -> <<"22">>;
        del -> <<"22">>;
        _ -> <<"20">>
    end,
    Payload = case {Namespace, ActionCode, ValType} of
        {_, <<"24">>, _} ->
            [LastHash, integer_to_binary(Seq), <<"24">>, Namespace, <<":">>, CanonicalValBin];
        {<<"default">>, <<"20">>, <<"int64">>} ->
            [LastHash, integer_to_binary(Seq), <<"20">>, KeyBin, CanonicalValBin];
        {<<"default">>, <<"22">>, _} ->
            [LastHash, integer_to_binary(Seq), <<"22">>, KeyBin];
        {_, <<"22">>, _} ->
            [LastHash, integer_to_binary(Seq), <<"22">>, Namespace, <<":">>, KeyBin];
        {_, <<"20">>, _} ->
            [LastHash, integer_to_binary(Seq), <<"20">>, Namespace, <<":">>, KeyBin, <<":">>, CanonicalValBin]
    end,
    binary:encode_hex(crypto:hash(sha256, Payload)).

lookup_val(Key, Default) ->
    case ets:lookup(?TABLE, Key) of
        [{Key, Val}] -> Val;
        [] -> Default
    end.

read_wal_entries(Path) ->
    case file:read_file(Path) of
        {ok, Content} ->
            Lines = binary:split(Content, <<"\n">>, [global, trim_all]),
            lists:filtermap(fun(Line) ->
                try
                    {true, json:decode(Line)}
                catch _:_ -> false
                end
            end, Lines);
        _ -> []
    end.

hb_safe_get(Key, Req, Default) ->
    case erlang:function_exported(hb_ao, get, 3) of
        true ->
            try hb_ao:get(Key, Req, #{}) of
                not_found -> Default;
                Val -> Val
            catch _:_ -> Default
            end;
        false -> Default
    end.

parse_raw_field(Key, Req, Default) when is_map(Req) ->
    case maps:get(Key, Req, not_found) of
        not_found ->
            QueryKey = << "query/", Key/binary >>,
            case maps:get(QueryKey, Req, not_found) of
                not_found ->
                    BodyKey = << "body/", Key/binary >>,
                    case maps:get(BodyKey, Req, not_found) of
                        not_found ->
                            case maps:get(<<"body">>, Req, not_found) of
                                Bin when is_binary(Bin), byte_size(Bin) > 0 ->
                                    try json:decode(Bin) of
                                        Decoded when is_map(Decoded) ->
                                            maps:get(Key, Decoded, Default);
                                        _ -> Default
                                    catch _:_ -> Default
                                    end;
                                Map when is_map(Map) ->
                                    maps:get(Key, Map, Default);
                                _ ->
                                    case hb_safe_get(Key, Req, not_found) of
                                        not_found ->
                                            case hb_safe_get(QueryKey, Req, not_found) of
                                                not_found ->
                                                    case hb_safe_get(BodyKey, Req, not_found) of
                                                        not_found -> Default;
                                                        V3 -> V3
                                                    end;
                                                V2 -> V2
                                            end;
                                        V1 -> V1
                                    end
                            end;
                        Vb -> Vb
                    end;
                Vq -> Vq
            end;
        V0 -> V0
    end;
parse_raw_field(_Key, _Req, Default) ->
    Default.

parse_bin_field(Key, Req, Default) when is_map(Req) ->
    Val = parse_raw_field(Key, Req, Default),
    if is_binary(Val) -> Val;
       is_atom(Val) andalso Val =/= undefined -> atom_to_binary(Val, utf8);
       is_integer(Val) -> integer_to_binary(Val);
       true -> Default
    end;
parse_bin_field(_Key, _Req, Default) ->
    Default.

extract_admin_token(Req) ->
    case parse_bin_field(<<"admin_token">>, Req, undefined) of
        undefined ->
            case parse_bin_field(<<"authorization">>, Req, undefined) of
                undefined ->
                    case parse_bin_field(<<"headers/authorization">>, Req, undefined) of
                        undefined -> undefined;
                        AuthHdr -> parse_bearer_token(AuthHdr)
                    end;
                AuthVal -> parse_bearer_token(AuthVal)
            end;
        Tok -> Tok
    end.

parse_bearer_token(<<"Bearer ", Token/binary>>) -> string:trim(Token);
parse_bearer_token(<<"bearer ", Token/binary>>) -> string:trim(Token);
parse_bearer_token(Token) when is_binary(Token) -> string:trim(Token);
parse_bearer_token(_) -> undefined.

verify_admin_req(Req) ->
    GivenToken = extract_admin_token(Req),
    ExpectedHex = string:uppercase(binary:encode_hex(node_auth_secret())),
    case GivenToken of
        undefined ->
            {error, #{
                <<"ok">> => false,
                <<"status">> => 401,
                <<"error_code">> => <<"ERR_ADMIN_UNAUTHORIZED">>,
                <<"error">> => <<"Missing operator admin_token for protected administrative action">>
            }};
        TokenBin when is_binary(TokenBin) ->
            case string:uppercase(TokenBin) =:= ExpectedHex of
                true -> ok;
                false ->
                    {error, #{
                        <<"ok">> => false,
                        <<"status">> => 403,
                        <<"error_code">> => <<"ERR_ADMIN_UNAUTHORIZED">>,
                        <<"error">> => <<"Invalid operator admin_token">>
                    }}
            end
    end.

is_test_profile() ->
    case os:getenv("HYDB_PROFILE") of
        "prod" -> false;
        "production" -> false;
        "test" -> true;
        _ ->
            case os:getenv("NODE_ENV") of
                "production" -> false;
                "test" -> true;
                _ ->
                    case os:getenv("HYDB_ENV") of
                        "prod" -> false;
                        "production" -> false;
                        "test" -> true;
                        _ -> true
                    end
            end
    end.

parse_int_field(Key, Req, Default) when is_map(Req) ->
    Val = parse_raw_field(Key, Req, Default),
    to_integer(Val, Default);
parse_int_field(_Key, _Req, Default) -> Default.

to_integer(V, _Default) when is_integer(V) -> V;
to_integer(V, Default) when is_binary(V) ->
    try binary_to_integer(V) catch _:_ -> Default end;
to_integer(V, Default) when is_list(V) ->
    try list_to_integer(V) catch _:_ -> Default end;
to_integer(_, Default) -> Default.
