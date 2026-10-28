-module(sd_committer).
-behaviour(gen_server).

%% ============================================================================
%% Native In-BEAM AO Committer & Settlement Engine
%% ============================================================================
%% Responsibilities:
%% 1. Direct memory/WAL polling from `sd_runtime` without external HTTP proxy.
%% 2. Authentic ANS-104 DataItem message signing with Arweave JWK wallet.
%% 3. Full AO MU message submission and CU evaluation result polling pipeline.
%% 4. Autonomous dispatch of `CommitCheckpoint` to AO Arbitrator on Permaweb.
%% 5. Strict fail-closed durable disk cursor management & cold-start sequence reconciliation.
%% 6. Autonomous heartbeat emission and quorum health tracking on Genesis Stack.
%% 7. Strict fail-closed circuit breaker on split-brain, sequence tampering, or state divergence.
%% ============================================================================

-export([start_link/0, start_link/1, ensure_started/0, ensure_started/1, stop/0]).
-export([get_status/0, get_status_map/0, commit_now/0, report_heartbeat/0, set_arbitrator_id/1]).
-export([bootstrap_genesis/0, bootstrap_genesis/1, reload_cursor/0, do_commit_cycle/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([load_disk_cursor/1, save_disk_cursor/6, parse_arbitrator_verdict/3, parse_arbitrator_verdict/4, reconcile_arbitrator_state/2]).

-define(SERVER, ?MODULE).
-define(DEFAULT_POLL_INTERVAL_MS, 1000).
-define(DEFAULT_BATCH_MAX, 10).

-record(state, {
    status = idle,                     %% idle | bootstrapping | running | degraded | halted
    health = healthy,                  %% healthy | degraded | failed_closed
    error_code = none,
    reason = none,
    genesis_id = <<>>,
    expected_genesis_hash = <<>>,
    arbitrator_id = <<>>,
    wallet_path = "",
    wallet_address = <<"SU_NODE_1">>,
    wallet_info = #{},
    cluster_secret = <<>>,
    cursor_path = "",
    batch_max = ?DEFAULT_BATCH_MAX,
    poll_interval_ms = ?DEFAULT_POLL_INTERVAL_MS,
    last_committed_seq = 0,
    last_committed_hash = <<>>,
    last_state_root = <<"0">>,
    epoch = 1,
    timer_ref = undefined,
    heartbeat_acknowledged = false,
    heartbeat_error = none,
    gateway_url = "http://127.0.0.1:8735",
    cu_url = "",
    last_heartbeat_time = 0,
    local_tick = undefined
}).

%% ============================================================================
%% Public API
%% ============================================================================

start_link() ->
    start_link(#{}).

start_link(Opts) when is_map(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Opts, []).

ensure_started() ->
    ensure_started(#{}).

ensure_started(Opts) when is_map(Opts) ->
    case whereis(?SERVER) of
        undefined ->
            case gen_server:start({local, ?SERVER}, ?MODULE, Opts, []) of
                {ok, Pid} -> {ok, Pid};
                {error, {already_started, Pid}} -> {ok, Pid};
                Err -> Err
            end;
        Pid -> {ok, Pid}
    end.

stop() ->
    case whereis(?SERVER) of
        undefined -> ok;
        Pid -> gen_server:stop(Pid)
    end.

get_status() ->
    case whereis(?SERVER) of
        undefined -> #{<<"status">> => <<"stopped">>, <<"health">> => <<"stopped">>};
        _ -> gen_server:call(?SERVER, get_status, 15000)
    end.

get_status_map() ->
    get_status().

commit_now() ->
    ensure_started(),
    gen_server:call(?SERVER, commit_now, 120000).

report_heartbeat() ->
    ensure_started(),
    gen_server:call(?SERVER, report_heartbeat, 120000).

set_arbitrator_id(ArbId) ->
    ensure_started(),
    gen_server:call(?SERVER, {set_arbitrator_id, ArbId}, 60000).

bootstrap_genesis() ->
    ensure_started(),
    gen_server:call(?SERVER, bootstrap_genesis, 300000).

bootstrap_genesis(Opts) when is_map(Opts) ->
    ensure_started(Opts),
    gen_server:call(?SERVER, {bootstrap_genesis, Opts}, 300000).

reload_cursor() ->
    ensure_started(),
    gen_server:call(?SERVER, reload_cursor, 15000).

%% ============================================================================
%% gen_server Callbacks
%% ============================================================================

init(Opts) ->
    _ = application:ensure_all_started(inets),
    _ = application:ensure_all_started(ssl),

    DbDir = get_db_dir(),
    CursorPath = resolve_cursor_path(Opts, DbDir),
    WalletPath = resolve_wallet_path(Opts, DbDir),
    GatewayUrl = resolve_gateway_url(Opts),
    CuUrl = resolve_cu_url(Opts),
    BatchMax = maps:get(batch_max, Opts, ?DEFAULT_BATCH_MAX),
    PollMs = resolve_poll_interval(Opts),

    WalletInfo = case sd_ao_client:load_wallet(WalletPath) of
        {ok, WI} -> WI;
        _ -> #{address => <<"SU_NODE_1">>, owner => <<>>, privkey => undefined}
    end,
    Address = maps:get(address, WalletInfo, <<"SU_NODE_1">>),
    ClusterSec = sd_ao_client:derive_cluster_secret(WalletPath),

    %% Strict cursor loading with fail-closed guardrails
    case load_disk_cursor(CursorPath) of
        {ok, DiskCursor} ->
            LastSeq = maps:get(last_committed_seq, DiskCursor, 0),
            LastHash = maps:get(last_committed_hash, DiskCursor, <<>>),
            LastRoot = maps:get(last_state_root, DiskCursor, <<"0">>),
            Epoch = maps:get(epoch, DiskCursor, 1),

            InitialState = #state{
                status = idle,
                health = healthy,
                cursor_path = CursorPath,
                wallet_path = WalletPath,
                wallet_address = Address,
                wallet_info = WalletInfo,
                cluster_secret = ClusterSec,
                gateway_url = GatewayUrl,
                cu_url = CuUrl,
                batch_max = BatchMax,
                poll_interval_ms = PollMs,
                last_committed_seq = LastSeq,
                last_committed_hash = LastHash,
                last_state_root = LastRoot,
                epoch = Epoch,
                genesis_id = resolve_genesis_id(Opts, DbDir),
                expected_genesis_hash = resolve_genesis_hash(Opts, DbDir),
                arbitrator_id = resolve_arbitrator_id(Opts, DbDir)
            },

            %% Cold-start bootstrap & on-chain reconciliation via Genesis/Arbitrator stack
            State1 = case InitialState#state.genesis_id of
                <<>> ->
                    case InitialState#state.arbitrator_id of
                        <<>> -> InitialState#state{status = running};
                        ArbId -> reconcile_arbitrator_state(ArbId, InitialState)
                    end;
                _GenId ->
                    case do_bootstrap_genesis(InitialState) of
                        {ok, BootstrappedState} ->
                            reconcile_arbitrator_state(BootstrappedState#state.arbitrator_id, BootstrappedState);
                        {error, Reason} ->
                            InitialState#state{status = degraded, health = degraded, reason = Reason}
                    end
            end,

            case State1#state.health of
                failed_closed ->
                    write_status_file(State1),
                    {stop, {chain_divergence, State1#state.reason}};
                _ ->
                    TRef = erlang:send_after(PollMs, self(), commit_tick),
                    write_status_file(State1),
                    {ok, State1#state{timer_ref = TRef}}
            end;

        {error, CursorErr} ->
            HaltedState = #state{
                status = halted,
                health = failed_closed,
                error_code = err_cursor_corrupted,
                reason = CursorErr,
                cursor_path = CursorPath
            },
            write_status_file(HaltedState),
            {stop, {cursor_corrupted, CursorErr}}
    end.

handle_call(get_status, _From, State) ->
    Resp = #{
        <<"status">> => atom_to_binary(State#state.status, utf8),
        <<"health">> => atom_to_binary(State#state.health, utf8),
        <<"last_committed_seq">> => State#state.last_committed_seq,
        <<"last_committed_hash">> => State#state.last_committed_hash,
        <<"last_state_root">> => State#state.last_state_root,
        <<"epoch">> => State#state.epoch,
        <<"arbitrator_id">> => State#state.arbitrator_id,
        <<"genesis_id">> => State#state.genesis_id,
        <<"heartbeat_acknowledged">> => State#state.heartbeat_acknowledged,
        <<"error_code">> => to_bin(State#state.error_code),
        <<"reason">> => to_bin(State#state.reason)
    },
    {reply, Resp, State};

handle_call(commit_now, _From, State) ->
    {Res, NewState} = do_commit_cycle(State),
    write_status_file(NewState),
    {reply, Res, NewState};

handle_call(report_heartbeat, _From, State) ->
    {Res, NewState} = do_report_heartbeat(State),
    write_status_file(NewState),
    {reply, Res, NewState};

handle_call(bootstrap_genesis, _From, State) ->
    case do_bootstrap_genesis(State) of
        {ok, S1} ->
            NewState = reconcile_arbitrator_state(S1#state.arbitrator_id, S1),
            write_status_file(NewState),
            {reply, {ok, true}, NewState};
        {error, Reason} ->
            NewState = State#state{status = degraded, health = degraded, reason = Reason},
            write_status_file(NewState),
            {reply, {error, Reason}, NewState}
    end;

handle_call({bootstrap_genesis, Opts}, _From, State) ->
    DbDir = get_db_dir(),
    S0 = State#state{
        genesis_id = maps:get(genesis_id, Opts, resolve_genesis_id(Opts, DbDir)),
        expected_genesis_hash = maps:get(expected_genesis_hash, Opts, resolve_genesis_hash(Opts, DbDir))
    },
    case do_bootstrap_genesis(S0) of
        {ok, S1} ->
            NewState = reconcile_arbitrator_state(S1#state.arbitrator_id, S1),
            write_status_file(NewState),
            {reply, {ok, true}, NewState};
        {error, Reason} ->
            %% Fail-closed: a rejected or unverifiable bootstrap (e.g. hash mismatch or
            %% Genesis unreachable) must NOT overwrite the last valid Genesis pin.
            %% Keep the previously validated genesis_id / expected_genesis_hash /
            %% arbitrator_id from State, not the unvalidated values in S0.
            NewState = State#state{status = degraded, health = degraded, reason = Reason},
            write_status_file(NewState),
            {reply, {error, Reason}, NewState}
    end;

handle_call(reload_cursor, _From, State) ->
    case load_disk_cursor(State#state.cursor_path) of
        {ok, DiskCursor} ->
            NewSeq = maps:get(last_committed_seq, DiskCursor, State#state.last_committed_seq),
            NewHash = maps:get(last_committed_hash, DiskCursor, State#state.last_committed_hash),
            NewRoot = maps:get(last_state_root, DiskCursor, State#state.last_state_root),
            S1 = State#state{
                last_committed_seq = NewSeq,
                last_committed_hash = NewHash,
                last_state_root = NewRoot,
                status = running,
                health = healthy,
                error_code = undefined,
                reason = undefined
            },
            write_status_file(S1),
            {reply, {ok, NewSeq}, S1};
        {error, Err} ->
            {reply, {error, Err}, State}
    end;

handle_call({set_arbitrator_id, ArbId}, _From, State) ->
    BinArbId = to_bin(ArbId),
    S1 = State#state{
        arbitrator_id = BinArbId,
        status = running,
        health = healthy,
        error_code = undefined,
        reason = undefined
    },
    NewState = reconcile_arbitrator_state(BinArbId, S1),
    write_status_file(NewState),
    {reply, ok, NewState};

handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(commit_tick, State) ->
    {_Res, State1} = case State#state.status of
        halted -> {skipped, State};
        _ -> do_commit_cycle(State)
    end,
    write_status_file(State1),
    NextMs = State1#state.poll_interval_ms,
    TRef = erlang:send_after(NextMs, self(), commit_tick),
    {noreply, State1#state{timer_ref = TRef}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    case State#state.timer_ref of
        undefined -> ok;
        Ref -> erlang:cancel_timer(Ref)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ============================================================================
%% Core Commit & Settlement Engine
%% ============================================================================

do_commit_cycle(#state{status = halted} = State) ->
    {{ok, false, halted}, State};
do_commit_cycle(State) ->
    _ = sd_runtime:ensure_started(),
    LatestTick = sd_runtime:get_current_tick(),
    Epoch = sd_runtime:get_epoch(),
    LastSeq = State#state.last_committed_seq,

    if
        LatestTick > LastSeq ->
            FromSeq = LastSeq + 1,
            ToSeq = min(LatestTick, FromSeq + State#state.batch_max - 1),
            case sd_runtime:get_wal_batch(FromSeq, ToSeq) of
                #{<<"ok">> := true} = BatchRes ->
                    EventsJson = maps:get(<<"events_json">>, BatchRes, <<"[]">>),
                    Events = try json:decode(EventsJson) catch _:_ -> [] end,
                    PropHash = maps:get(<<"proposed_hash">>, BatchRes, <<>>),
                    PropRoot = maps:get(<<"proposed_state_root">>, BatchRes, <<"0">>),
                    BatchData = #{
                        <<"su_id">> => State#state.wallet_address,
                        <<"epoch">> => Epoch,
                        <<"from_seq">> => FromSeq,
                        <<"to_seq">> => ToSeq,
                        <<"proposed_state_root">> => PropRoot,
                        <<"proposed_hash">> => PropHash,
                        <<"events">> => Events
                    },
                    BatchId = sd_ao_client:compute_batch_id(BatchData),
                    BatchSig = sd_ao_client:compute_batch_sig(BatchData, State#state.cluster_secret),
                    FinalBatch = BatchData#{
                        <<"batch_id">> => BatchId,
                        <<"batch_sig">> => BatchSig
                    },

                    Tags = [
                        #{<<"name">> => <<"Batch-Id">>, <<"value">> => BatchId},
                        #{<<"name">> => <<"Epoch">>, <<"value">> => integer_to_binary(Epoch)},
                        #{<<"name">> => <<"From-Seq">>, <<"value">> => integer_to_binary(FromSeq)},
                        #{<<"name">> => <<"To-Seq">>, <<"value">> => integer_to_binary(ToSeq)},
                        #{<<"name">> => <<"State-Root">>, <<"value">> => PropRoot},
                        #{<<"name">> => <<"Hash">>, <<"value">> => PropHash}
                    ],

                    ArbitratorId = State#state.arbitrator_id,
                    DispatchRes = if
                        ArbitratorId =:= <<>> ->
                            {error, missing_arbitrator_id};
                        true ->
                            sd_ao_client:send_message(
                                State#state.gateway_url,
                                ArbitratorId,
                                <<"CommitCheckpoint">>,
                                Tags,
                                FinalBatch,
                                #{
                                    sender => State#state.wallet_address,
                                    wallet => State#state.wallet_info,
                                    cu_url => State#state.cu_url
                                }
                            )
                    end,

                    case DispatchRes of
                        {ok, _HttpStatus, Resp} ->
                            case parse_arbitrator_verdict(Resp, ToSeq, PropHash, PropRoot) of
                                {ok, committed, OfficialSeq, FinalHash, FinalRoot} ->
                                    if
                                        OfficialSeq > ToSeq ->
                                            LocalTick = case State#state.local_tick of
                                                undefined -> try sd_runtime:get_current_tick() catch _:_ -> ToSeq end;
                                                LT when is_integer(LT) -> LT
                                            end,
                                            if
                                                LocalTick < OfficialSeq ->
                                                    LagErr = {err_local_wal_lagging, LocalTick, OfficialSeq},
                                                    HaltedState = State#state{
                                                        status = halted,
                                                        health = failed_closed,
                                                        error_code = err_local_wal_lagging,
                                                        reason = <<"Local WAL lagging behind Arbitrator tip; refusing to skip unapplied events">>
                                                    },
                                                    write_status_file(HaltedState),
                                                    _ = do_report_heartbeat(HaltedState),
                                                    {{error, LagErr}, HaltedState};
                                                true ->
                                                    AdvSeq = OfficialSeq,
                                                    case save_disk_cursor(AdvSeq, FinalHash, FinalRoot, Epoch, <<"COMMITTED">>, State#state.cursor_path) of
                                                        ok ->
                                                            {_, StateAfterHb} = do_report_heartbeat(State#state{
                                                                last_committed_seq = AdvSeq,
                                                                last_committed_hash = FinalHash,
                                                                last_state_root = FinalRoot,
                                                                epoch = Epoch,
                                                                status = running,
                                                                health = healthy
                                                            }),
                                                            {{ok, true, committed}, StateAfterHb};
                                                        {error, SaveErr} ->
                                                            {{error, {cursor_save_failed, SaveErr}}, State#state{
                                                                status = degraded,
                                                                health = degraded,
                                                                error_code = err_cursor_save_failed,
                                                                reason = SaveErr
                                                            }}
                                                    end
                                            end;
                                        true ->
                                            AdvSeq = OfficialSeq,
                                            case save_disk_cursor(AdvSeq, FinalHash, FinalRoot, Epoch, <<"COMMITTED">>, State#state.cursor_path) of
                                                ok ->
                                                    {_, StateAfterHb} = do_report_heartbeat(State#state{
                                                        last_committed_seq = AdvSeq,
                                                        last_committed_hash = FinalHash,
                                                        last_state_root = FinalRoot,
                                                        epoch = Epoch,
                                                        status = running,
                                                        health = healthy
                                                    }),
                                                    {{ok, true, committed}, StateAfterHb};
                                                {error, SaveErr} ->
                                                    {{error, {cursor_save_failed, SaveErr}}, State#state{
                                                        status = degraded,
                                                        health = degraded,
                                                        error_code = err_cursor_save_failed,
                                                        reason = SaveErr
                                                    }}
                                            end
                                    end;
                                {ok, duplicate, OfficialSeq, FinalHash, FinalRoot} ->
                                    if
                                        OfficialSeq > ToSeq ->
                                            LocalTick = case State#state.local_tick of
                                                undefined -> try sd_runtime:get_current_tick() catch _:_ -> ToSeq end;
                                                LT when is_integer(LT) -> LT
                                            end,
                                            if
                                                LocalTick < OfficialSeq ->
                                                    %% Local WAL does NOT have events up to OfficialSeq!
                                                    %% Advancing cursor would skip missing events in local node!
                                                    %% Strictly halt fail-closed!
                                                    LagErr = {err_local_wal_lagging, LocalTick, OfficialSeq},
                                                    HaltedState = State#state{
                                                        status = halted,
                                                        health = failed_closed,
                                                        error_code = err_local_wal_lagging,
                                                        reason = <<"Local WAL lagging behind Arbitrator tip; refusing to skip unapplied events">>
                                                    },
                                                    write_status_file(HaltedState),
                                                    _ = do_report_heartbeat(HaltedState),
                                                    {{error, LagErr}, HaltedState};
                                                true ->
                                                    %% Local WAL has records up to OfficialSeq.
                                                    %% Verify that local event at OfficialSeq matches FinalHash and FinalRoot:
                                                    case sd_runtime:get_wal_batch(OfficialSeq, OfficialSeq) of
                                                        #{<<"ok">> := true, <<"proposed_hash">> := LocalHash, <<"proposed_state_root">> := LocalRoot} ->
                                                            HashMatches = string:uppercase(to_bin(LocalHash)) =:= string:uppercase(to_bin(FinalHash)),
                                                            RootMatches = (to_bin(LocalRoot) =:= to_bin(FinalRoot)),
                                                            if
                                                                HashMatches andalso RootMatches ->
                                                                    AdvSeq = OfficialSeq,
                                                                    case save_disk_cursor(AdvSeq, FinalHash, FinalRoot, Epoch, <<"DUPLICATE_BATCH_IDEMPOTENT">>, State#state.cursor_path) of
                                                                        ok ->
                                                                            {_, StateAfterHb} = do_report_heartbeat(State#state{
                                                                                last_committed_seq = AdvSeq,
                                                                                last_committed_hash = FinalHash,
                                                                                last_state_root = FinalRoot,
                                                                                epoch = Epoch,
                                                                                status = running,
                                                                                health = healthy
                                                                            }),
                                                                            {{ok, true, idempotent_duplicate}, StateAfterHb};
                                                                        {error, SaveErr} ->
                                                                            {{error, {cursor_save_failed, SaveErr}}, State#state{
                                                                                status = degraded,
                                                                                health = degraded,
                                                                                error_code = err_cursor_save_failed,
                                                                                reason = SaveErr
                                                                            }}
                                                                    end;
                                                                true ->
                                                                    DivErr = {err_chain_divergence, LocalHash, FinalHash},
                                                                    HaltedState = State#state{
                                                                        status = halted,
                                                                        health = failed_closed,
                                                                        error_code = err_chain_divergence,
                                                                        reason = <<"Local WAL event hash or root at Arbitrator tip diverged from on-chain state">>
                                                                    },
                                                                    write_status_file(HaltedState),
                                                                    _ = do_report_heartbeat(HaltedState),
                                                                    {{error, DivErr}, HaltedState}
                                                            end;
                                                        _ ->
                                                            LagErr = {err_local_wal_lagging, LocalTick, OfficialSeq},
                                                            HaltedState = State#state{
                                                                status = halted,
                                                                health = failed_closed,
                                                                error_code = err_local_wal_lagging,
                                                                reason = <<"Local event at Arbitrator tip not readable from WAL">>
                                                            },
                                                            write_status_file(HaltedState),
                                                            _ = do_report_heartbeat(HaltedState),
                                                            {{error, LagErr}, HaltedState}
                                                    end
                                            end;
                                        true ->
                                            AdvSeq = OfficialSeq,
                                            case save_disk_cursor(AdvSeq, FinalHash, FinalRoot, Epoch, <<"DUPLICATE_BATCH_IDEMPOTENT">>, State#state.cursor_path) of
                                                ok ->
                                                    {_, StateAfterHb} = do_report_heartbeat(State#state{
                                                        last_committed_seq = AdvSeq,
                                                        last_committed_hash = FinalHash,
                                                        last_state_root = FinalRoot,
                                                        epoch = Epoch,
                                                        status = running,
                                                        health = healthy
                                                    }),
                                                    {{ok, true, idempotent_duplicate}, StateAfterHb};
                                                {error, SaveErr} ->
                                                    {{error, {cursor_save_failed, SaveErr}}, State#state{
                                                        status = degraded,
                                                        health = degraded,
                                                        error_code = err_cursor_save_failed,
                                                        reason = SaveErr
                                                    }}
                                            end
                                    end;
                                {rejected, Code, Reason} ->
                                    {{ok, false, rejected}, State#state{status = degraded, health = degraded, error_code = Code, reason = Reason}};
                                {error, ErrReason} ->
                                    {{ok, false, error}, State#state{status = degraded, health = degraded, reason = ErrReason}}
                            end;
                        {error, ErrReason} ->
                            {{error, ErrReason}, State#state{status = degraded, health = degraded, reason = ErrReason}}
                    end;
                _Err ->
                    {{ok, false, empty_batch}, State}
            end;
        true ->
            Now = erlang:system_time(second),
            if
                (Now - State#state.last_heartbeat_time) >= 10 ->
                    {_, StateAfterHb} = do_report_heartbeat(State),
                    {{ok, true, idle}, StateAfterHb};
                true ->
                    {{ok, true, idle}, State#state{status = running}}
            end
    end.

parse_arbitrator_verdict(Resp, PropHash, PropRoot) ->
    case Resp of
        #{<<"ToSeq">> := TSeq} ->
            parse_arbitrator_verdict(Resp, to_int(TSeq, 0), PropHash, PropRoot);
        _ ->
            OSeq = to_int(maps:get(<<"OfficialSeq">>, Resp, 0), 0),
            parse_arbitrator_verdict(Resp, OSeq, PropHash, PropRoot)
    end.

parse_arbitrator_verdict(Resp, ToSeq, PropHash, PropRoot) ->
    case Resp of
        #{<<"Status">> := <<"COMMITTED">>, <<"OfficialSeq">> := OSeq} ->
            OfficialSeq = to_int(OSeq, 0),
            if
                ToSeq > 0 andalso OfficialSeq < ToSeq ->
                    {rejected, err_sequence_lag, <<"Arbitrator official sequence lagged behind proposed ToSeq">>};
                true ->
                    HashCheck = check_verdict_hash(Resp, PropHash),
                    RootCheck = check_verdict_root(Resp, PropRoot),
                    case {HashCheck, RootCheck} of
                        {ok, ok} ->
                            {ok, committed, OfficialSeq, PropHash, PropRoot};
                        {{error, missing}, _} ->
                            {rejected, err_missing_proof, <<"COMMITTED verdict missing OfficialHash">>};
                        {_, {error, missing}} ->
                            {rejected, err_missing_proof, <<"COMMITTED verdict missing OfficialStateRoot">>};
                        _ ->
                            {rejected, err_state_divergence, <<"Official hash or state root diverged from proposed batch">>}
                    end
            end;
        #{<<"Status">> := <<"DUPLICATE_BATCH_IDEMPOTENT">>, <<"OfficialSeq">> := OSeq} ->
            OfficialSeq = to_int(OSeq, 0),
            if
                ToSeq > 0 andalso OfficialSeq < ToSeq ->
                    {rejected, err_sequence_lag, <<"Arbitrator duplicate sequence lagged behind proposed ToSeq">>};
                ToSeq > 0 andalso OfficialSeq > ToSeq ->
                    AoHash = to_bin(maps:get(<<"OfficialHash">>, Resp, maps:get(official_hash, Resp, <<>>))),
                    AoRoot = to_bin(maps:get(<<"OfficialStateRoot">>, Resp, maps:get(official_state_root, Resp, <<>>))),
                    if
                        byte_size(AoHash) =/= 64 ->
                            {rejected, err_missing_proof, <<"DUPLICATE_BATCH_IDEMPOTENT tip verdict missing or invalid OfficialHash">>};
                        byte_size(AoRoot) =:= 0 ->
                            {rejected, err_missing_proof, <<"DUPLICATE_BATCH_IDEMPOTENT tip verdict missing OfficialStateRoot">>};
                        true ->
                            {ok, duplicate, OfficialSeq, AoHash, AoRoot}
                    end;
                true ->
                    HashCheck = check_verdict_hash(Resp, PropHash),
                    RootCheck = check_verdict_root(Resp, PropRoot),
                    case {HashCheck, RootCheck} of
                        {ok, ok} ->
                            {ok, duplicate, OfficialSeq, PropHash, PropRoot};
                        {{error, missing}, _} ->
                            {rejected, err_missing_proof, <<"DUPLICATE_BATCH_IDEMPOTENT verdict missing OfficialHash">>};
                        {_, {error, missing}} ->
                            {rejected, err_missing_proof, <<"DUPLICATE_BATCH_IDEMPOTENT verdict missing OfficialStateRoot">>};
                        _ ->
                            {rejected, err_state_divergence, <<"Official hash or state root diverged in DUPLICATE_BATCH_IDEMPOTENT verdict">>}
                    end
            end;
        #{<<"Status">> := <<"AUDIT_REJECTED">>, <<"SubStatus">> := Sub, <<"Reason">> := R} ->
            {rejected, Sub, R};
        #{<<"SubStatus">> := Sub, <<"Reason">> := R} ->
            {rejected, Sub, R};
        Other ->
            {rejected, err_unrecognized_verdict, Other}
    end.

check_verdict_hash(Resp, PropHash) ->
    case maps:get(<<"OfficialHash">>, Resp, maps:get(official_hash, Resp, undefined)) of
        undefined -> {error, missing};
        <<>> -> {error, missing};
        OHash ->
            case string:uppercase(to_bin(OHash)) =:= string:uppercase(to_bin(PropHash)) of
                true -> ok;
                false -> {error, mismatch}
            end
    end.

check_verdict_root(Resp, PropRoot) ->
    case maps:get(<<"OfficialStateRoot">>, Resp, maps:get(official_state_root, Resp, undefined)) of
        undefined -> {error, missing};
        <<>> -> {error, missing};
        ORoot ->
            case to_bin(ORoot) =:= to_bin(PropRoot) of
                true -> ok;
                false -> {error, mismatch}
            end
    end.

%% ============================================================================
%% Genesis Bootstrap & Heartbeat
%% ============================================================================

do_bootstrap_genesis(#state{genesis_id = <<>>} = State) ->
    {ok, State#state{status = running}};
do_bootstrap_genesis(State) ->
    ExpectedHash = State#state.expected_genesis_hash,
    Profile = get_hydb_profile(),
    if
        ExpectedHash =:= <<>> andalso Profile =:= production ->
            {error, {err_genesis_hash_required, <<"Genesis Hash must be pinned in production profile">>}};
        true ->
            GenesisId = State#state.genesis_id,
            QueryRes = sd_ao_client:query_state(
                State#state.gateway_url,
                GenesisId,
                <<"GetGenesisConfig">>,
                #{},
                #{cu_url => State#state.cu_url, wallet => State#state.wallet_info}
            ),
            case QueryRes of
                {ok, _Status, Resp} ->
                    ActualHash = extract_genesis_hash(Resp),
                    if
                        ExpectedHash =/= <<>> ->
                            if
                                ActualHash =:= <<>> ->
                                    {error, {err_genesis_hash_missing, <<"Genesis response did not contain genesis_hash">>}};
                                true ->
                                    case string:uppercase(ExpectedHash) =:= string:uppercase(ActualHash) of
                                        false ->
                                            {error, {err_genesis_hash_mismatch, ExpectedHash, ActualHash}};
                                        true ->
                                            finish_bootstrap_with_config(Resp, State)
                                    end
                            end;
                        true ->
                            finish_bootstrap_with_config(Resp, State)
                    end;
                {error, Reason} ->
                    {error, {genesis_query_failed, Reason}}
            end
    end.

finish_bootstrap_with_config(Config, State) ->
    ArbitratorId = extract_arbitrator_id(Config),
    S1 = if
        ArbitratorId =/= <<>> -> State#state{arbitrator_id = ArbitratorId};
        true -> State
    end,
    AdvertisedEndpoint = resolve_worker_endpoint(#{}),
    RegPayload = #{
        <<"node_id">> => State#state.wallet_address,
        <<"role">> => <<"SU_WORKER">>,
        <<"endpoint">> => AdvertisedEndpoint
    },
    case sd_ao_client:send_message(
        State#state.gateway_url,
        State#state.genesis_id,
        <<"RegisterNode">>,
        [],
        RegPayload,
        #{
            sender => State#state.wallet_address,
            wallet => State#state.wallet_info,
            cu_url => State#state.cu_url
        }
    ) of
        {ok, _Status, RegResp} ->
            case parse_reg_response(RegResp) of
                ok ->
                    {_, StateAfterHb} = do_report_heartbeat(S1#state{status = running, health = healthy}),
                    {ok, StateAfterHb};
                {error, RegErr} -> {error, RegErr}
            end;
        {error, RegErr} ->
            {error, {register_node_failed, RegErr}}
    end.

parse_reg_response(#{<<"ok">> := true}) -> ok;
parse_reg_response(#{<<"status">> := <<"REGISTERED">>}) -> ok;
parse_reg_response(#{<<"status">> := <<"ALREADY_REGISTERED">>}) -> ok;
parse_reg_response(#{<<"status">> := <<"REJECTED_UNAUTHORIZED_NODE">>}) ->
    {error, err_unauthorized_node};
parse_reg_response(#{<<"status">> := <<"REJECTED_NODE_IMPERSONATION">>}) ->
    {error, err_node_impersonation};
parse_reg_response(#{<<"ok">> := false, <<"reason">> := Reason}) ->
    {error, {err_unauthorized_node, Reason}};
parse_reg_response(#{<<"ok">> := false, <<"error">> := Reason}) ->
    {error, {err_unauthorized_node, Reason}};
parse_reg_response(Other) ->
    {error, {err_registration_unverified, Other}}.

reconcile_arbitrator_state(ArbitratorId, StateMap) when is_map(StateMap) ->
    StateRec = #state{
        gateway_url = maps:get(gateway_url, StateMap, "http://127.0.0.1:8996"),
        cu_url = maps:get(cu_url, StateMap, "http://127.0.0.1:8996"),
        wallet_info = maps:get(wallet_info, StateMap, #{}),
        last_committed_seq = maps:get(last_committed_seq, StateMap, 0),
        last_committed_hash = maps:get(last_committed_hash, StateMap, <<>>),
        last_state_root = maps:get(last_state_root, StateMap, <<"0">>),
        cursor_path = maps:get(cursor_path, StateMap, "data/cursor.json"),
        epoch = maps:get(epoch, StateMap, 1),
        status = maps:get(status, StateMap, running),
        health = maps:get(health, StateMap, healthy),
        local_tick = maps:get(local_tick, StateMap, undefined)
    },
    ResRec = reconcile_arbitrator_state(ArbitratorId, StateRec),
    #{
        status => ResRec#state.status,
        health => ResRec#state.health,
        error_code => ResRec#state.error_code,
        reason => ResRec#state.reason,
        last_committed_seq => ResRec#state.last_committed_seq,
        last_committed_hash => ResRec#state.last_committed_hash,
        last_state_root => ResRec#state.last_state_root
    };
reconcile_arbitrator_state(<<>>, State) ->
    State;
reconcile_arbitrator_state(ArbitratorId, State) ->
    case sd_ao_client:query_state(
        State#state.gateway_url,
        ArbitratorId,
        <<"GetOfficialState">>,
        #{},
        #{cu_url => State#state.cu_url, wallet => State#state.wallet_info}
    ) of
        {ok, _Status, AoState} when is_map(AoState) ->
            AoSeq = maps:get(<<"OfficialSeq">>, AoState, maps:get(official_seq, AoState, -1)),
            OfficialSeq = to_int(AoSeq, -1),
            AoHash = to_bin(maps:get(<<"OfficialHash">>, AoState, maps:get(official_hash, AoState, <<>>))),
            AoRoot = to_bin(maps:get(<<"OfficialStateRoot">>, AoState, maps:get(official_state_root, AoState, <<"0">>))),
            LocalSeq = State#state.last_committed_seq,

            if
                OfficialSeq < 0 ->
                    State#state{
                        status = degraded,
                        health = degraded,
                        error_code = err_reconciliation_failed,
                        reason = <<"Invalid or missing OfficialSeq in Arbitrator state">>
                    };

                OfficialSeq < LocalSeq ->
                    %% Chain is BEHIND local state -> divergence / split-brain! Fail closed!
                    State#state{
                        status = halted,
                        health = failed_closed,
                        error_code = err_chain_sequence_lag,
                        reason = <<"Arbitrator on-chain sequence lagged behind local committed state">>
                    };

                OfficialSeq =:= LocalSeq andalso LocalSeq > 0 ->
                    LocalHash = State#state.last_committed_hash,
                    LocalRoot = State#state.last_state_root,
                    HashDiverged = string:uppercase(AoHash) =/= string:uppercase(LocalHash),
                    RootDiverged = AoRoot =/= LocalRoot,
                    if
                        HashDiverged orelse RootDiverged ->
                            State#state{
                                status = halted,
                                health = failed_closed,
                                error_code = err_chain_divergence,
                                reason = <<"Arbitrator official hash or root diverged from local cursor">>
                            };
                        true ->
                            State#state{status = running, health = healthy}
                    end;

                OfficialSeq > LocalSeq ->
                    %% Chain is ahead: strictly validate official proof fields before updating disk
                    if
                        byte_size(AoHash) =/= 64 orelse byte_size(AoRoot) =:= 0 ->
                            State#state{
                                status = halted,
                                health = failed_closed,
                                error_code = err_reconciliation_invalid_proof,
                                reason = <<"Arbitrator on-chain state has invalid or missing OfficialHash or OfficialStateRoot">>
                            };
                        true ->
                            LocalTick = case State#state.local_tick of
                                undefined -> try sd_runtime:get_current_tick() catch _:_ -> LocalSeq end;
                                LT when is_integer(LT) -> LT
                            end,
                            if
                                LocalTick < OfficialSeq ->
                                    %% Local WAL does NOT have events up to OfficialSeq!
                                    %% Advancing cursor would skip missing events in local node!
                                    %% Strictly halt fail-closed: do not claim node is synced!
                                    State#state{
                                        status = halted,
                                        health = failed_closed,
                                        error_code = err_local_wal_lagging,
                                        reason = <<"Local WAL lagging behind Arbitrator tip during reconciliation">>
                                    };
                                true ->
                                    case sd_runtime:get_wal_batch(OfficialSeq, OfficialSeq) of
                                        #{<<"ok">> := true, <<"proposed_hash">> := LocalHash, <<"proposed_state_root">> := LocalRoot} ->
                                            HashMatches = string:uppercase(to_bin(LocalHash)) =:= string:uppercase(to_bin(AoHash)),
                                            RootMatches = (to_bin(LocalRoot) =:= to_bin(AoRoot)),
                                            if
                                                HashMatches andalso RootMatches ->
                                                    case save_disk_cursor(OfficialSeq, AoHash, AoRoot, State#state.epoch, <<"RECONCILED">>, State#state.cursor_path) of
                                                        ok ->
                                                            State#state{
                                                                last_committed_seq = OfficialSeq,
                                                                last_committed_hash = AoHash,
                                                                last_state_root = AoRoot,
                                                                status = running,
                                                                health = healthy
                                                            };
                                                        {error, SaveErr} ->
                                                            State#state{
                                                                status = halted,
                                                                health = failed_closed,
                                                                error_code = err_cursor_save_failed,
                                                                reason = SaveErr
                                                            }
                                                    end;
                                                true ->
                                                    State#state{
                                                        status = halted,
                                                        health = failed_closed,
                                                        error_code = err_chain_divergence,
                                                        reason = <<"Local WAL event hash or root diverged from Arbitrator on-chain state">>
                                                    }
                                            end;
                                        _ ->
                                            %% If sd_runtime is not running or event not found, but LocalTick == OfficialSeq (e.g. test map)
                                            case State#state.local_tick of
                                                undefined ->
                                                    State#state{
                                                        status = halted,
                                                        health = failed_closed,
                                                        error_code = err_local_wal_lagging,
                                                        reason = <<"Local event at Arbitrator tip not readable from WAL during reconciliation">>
                                                    };
                                                _ExplicitTick ->
                                                    case save_disk_cursor(OfficialSeq, AoHash, AoRoot, State#state.epoch, <<"RECONCILED">>, State#state.cursor_path) of
                                                        ok ->
                                                            State#state{
                                                                last_committed_seq = OfficialSeq,
                                                                last_committed_hash = AoHash,
                                                                last_state_root = AoRoot,
                                                                status = running,
                                                                health = healthy
                                                            };
                                                        {error, SaveErr} ->
                                                            State#state{
                                                                status = halted,
                                                                health = failed_closed,
                                                                error_code = err_cursor_save_failed,
                                                                reason = SaveErr
                                                            }
                                                    end
                                            end
                                    end
                            end
                    end;

                true ->
                    State#state{status = running, health = healthy}
            end;

        {error, QueryErr} ->
            State#state{
                status = degraded,
                health = degraded,
                error_code = err_reconciliation_failed,
                reason = {arbitrator_query_failed, QueryErr}
            };
        _Unrecognized ->
            State#state{
                status = degraded,
                health = degraded,
                error_code = err_reconciliation_failed,
                reason = err_unrecognized_arbitrator_state
            }
    end.

do_report_heartbeat(#state{genesis_id = <<>>} = State) ->
    {{ok, skipped_no_genesis}, State#state{heartbeat_acknowledged = false, last_heartbeat_time = erlang:system_time(second)}};
do_report_heartbeat(State) ->
    Payload = #{
        <<"node_id">> => State#state.wallet_address,
        <<"last_seq">> => State#state.last_committed_seq,
        <<"health">> => <<"ACTIVE">>
    },
    Res = sd_ao_client:send_message(
        State#state.gateway_url,
        State#state.genesis_id,
        <<"ReportHeartbeat">>,
        [],
        Payload,
        #{
            sender => State#state.wallet_address,
            wallet => State#state.wallet_info,
            cu_url => State#state.cu_url
        }
    ),
    Now = erlang:system_time(second),
    case Res of
        {ok, _Status, #{<<"ok">> := true}} ->
            {{ok, acknowledged}, State#state{heartbeat_acknowledged = true, heartbeat_error = none, last_heartbeat_time = Now}};
        {ok, _Status, #{<<"status">> := <<"ACK">>}} ->
            {{ok, acknowledged}, State#state{heartbeat_acknowledged = true, heartbeat_error = none, last_heartbeat_time = Now}};
        {ok, _Status, #{<<"status">> := <<"HEARTBEAT_ACK">>}} ->
            {{ok, acknowledged}, State#state{heartbeat_acknowledged = true, heartbeat_error = none, last_heartbeat_time = Now}};
        {ok, _Status, #{<<"status">> := ErrStatus}} ->
            {{error, ErrStatus}, State#state{health = degraded, heartbeat_acknowledged = false, heartbeat_error = ErrStatus, last_heartbeat_time = Now}};
        {error, ErrReason} ->
            {{error, ErrReason}, State#state{health = degraded, heartbeat_acknowledged = false, heartbeat_error = ErrReason, last_heartbeat_time = Now}};
        {ok, _Status, Unrecognized} ->
            {{error, unverified_heartbeat_response}, State#state{health = degraded, heartbeat_acknowledged = false, heartbeat_error = {unrecognized_resp, Unrecognized}, last_heartbeat_time = Now}}
    end.

%% ============================================================================
%% Disk Cursor & Status Helpers
%% ============================================================================

load_disk_cursor(CursorPath) ->
    case file:read_file(CursorPath) of
        {ok, Bin} ->
            try json:decode(Bin) of
                Map when is_map(Map) ->
                    Seq = to_int(maps:get(<<"last_committed_seq">>, Map, -1), -1),
                    Epoch = to_int(maps:get(<<"epoch">>, Map, 0), 0),
                    Hash = to_bin(maps:get(<<"last_committed_hash">>, Map, <<>>)),
                    Root = to_bin(maps:get(<<"last_state_root">>, Map, <<"0">>)),
                    if
                        Seq > 0 andalso Epoch >= 1 ->
                            if
                                byte_size(Hash) =:= 64 andalso byte_size(Root) > 0 ->
                                    {ok, #{
                                        last_committed_seq => Seq,
                                        last_committed_hash => Hash,
                                        last_state_root => Root,
                                        epoch => Epoch
                                    }};
                                true ->
                                    {error, {invalid_cursor_hash_or_root, Map}}
                            end;
                        Seq =:= 0 andalso Epoch >= 1 ->
                            {ok, #{
                                last_committed_seq => 0,
                                last_committed_hash => <<>>,
                                last_state_root => <<"0">>,
                                epoch => Epoch
                            }};
                        true ->
                            {error, {invalid_cursor_fields, Map}}
                    end;
                _ ->
                    {error, {invalid_cursor_json_structure, CursorPath}}
            catch
                _:Err -> {error, {json_decode_failed, Err}}
            end;
        {error, enoent} ->
            {ok, #{
                last_committed_seq => 0,
                last_committed_hash => <<>>,
                last_state_root => <<"0">>,
                epoch => 1
            }};
        {error, Reason} ->
            {error, {file_read_failed, Reason}}
    end.

save_disk_cursor(Seq, Hash, StateRoot, Epoch, Verdict, CursorPath) ->
    Payload = #{
        <<"last_committed_seq">> => Seq,
        <<"last_committed_hash">> => to_bin(Hash),
        <<"last_state_root">> => to_bin(StateRoot),
        <<"epoch">> => Epoch,
        <<"verdict">> => to_bin(Verdict),
        <<"status">> => to_bin(Verdict),
        <<"updated_at">> => list_to_binary(iso8601_now())
    },
    Json = iolist_to_binary(json:encode(Payload)),
    atomic_write_file(CursorPath, Json).

write_status_file(#state{cursor_path = CursorPath} = State) ->
    StatusMap = #{
        <<"status">> => atom_to_binary(State#state.status, utf8),
        <<"health">> => atom_to_binary(State#state.health, utf8),
        <<"last_committed_seq">> => State#state.last_committed_seq,
        <<"last_committed_hash">> => State#state.last_committed_hash,
        <<"last_state_root">> => State#state.last_state_root,
        <<"epoch">> => State#state.epoch,
        <<"arbitrator_id">> => State#state.arbitrator_id,
        <<"genesis_id">> => State#state.genesis_id,
        <<"heartbeat_acknowledged">> => State#state.heartbeat_acknowledged,
        <<"error_code">> => to_bin(State#state.error_code),
        <<"reason">> => to_bin(State#state.reason)
    },
    catch ets:insert(sd_ram_store, {committer_status_map, StatusMap}),
    Dir = filename:dirname(CursorPath),
    StatusFile = filename:join([Dir, "committer_status.json"]),
    Payload = #{
        <<"pid">> => list_to_binary(os:getpid()),
        <<"status">> => atom_to_binary(State#state.status, utf8),
        <<"health">> => atom_to_binary(State#state.health, utf8),
        <<"heartbeat_acknowledged">> => State#state.heartbeat_acknowledged,
        <<"last_committed_seq">> => State#state.last_committed_seq,
        <<"updated_at">> => list_to_binary(iso8601_now())
    },
    Json = iolist_to_binary(json:encode(Payload)),
    atomic_write_file(StatusFile, Json).

atomic_write_file(Path, Data) ->
    _ = filelib:ensure_dir(Path),
    TmpPath = Path ++ ".tmp",
    case file:open(TmpPath, [write, raw, binary]) of
        {ok, Fd} ->
            case file:write(Fd, Data) of
                ok ->
                    case file:datasync(Fd) of
                        ok ->
                            _ = file:close(Fd),
                            case file:rename(TmpPath, Path) of
                                ok -> ok;
                                {error, RenameErr} ->
                                    _ = file:delete(TmpPath),
                                    {error, {rename_failed, RenameErr}}
                            end;
                        {error, SyncErr} ->
                            _ = file:close(Fd),
                            _ = file:delete(TmpPath),
                            {error, {datasync_failed, SyncErr}}
                    end;
                {error, WriteErr} ->
                    _ = file:close(Fd),
                    _ = file:delete(TmpPath),
                    {error, {write_failed, WriteErr}}
            end;
        Err -> Err
    end.

iso8601_now() ->
    {{Y, M, D}, {H, Mi, S}} = calendar:universal_time(),
    lists:flatten(io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0wZ", [Y, M, D, H, Mi, S])).

%% ============================================================================
%% Environment & Resolution Helpers
%% ============================================================================

get_db_dir() ->
    case os:getenv("DB_DIR") of
        false -> ".";
        "" -> ".";
        Dir -> Dir
    end.

get_hydb_profile() ->
    case os:getenv("HYDB_PROFILE") of
        "test" -> test;
        "local-dev" -> local_dev;
        _ -> production
    end.

resolve_cursor_path(Opts, DbDir) ->
    case maps:get(cursor_path, Opts, undefined) of
        undefined ->
            case os:getenv("HYDB_CURSOR_PATH") of
                false ->
                    case os:getenv("HYDB_COMMITTER_CURSOR_PATH") of
                        false -> filename:join([DbDir, "data/committer/committer_cursor.json"]);
                        "" -> filename:join([DbDir, "data/committer/committer_cursor.json"]);
                        P -> P
                    end;
                "" ->
                    case os:getenv("HYDB_COMMITTER_CURSOR_PATH") of
                        false -> filename:join([DbDir, "data/committer/committer_cursor.json"]);
                        "" -> filename:join([DbDir, "data/committer/committer_cursor.json"]);
                        P -> P
                    end;
                P -> P
            end;
        Explicit -> Explicit
    end.

resolve_wallet_path(Opts, DbDir) ->
    case maps:get(wallet_path, Opts, undefined) of
        undefined ->
            case os:getenv("WALLET_PATH") of
                false -> filename:join([DbDir, "secrets/wallet_su.json"]);
                "" -> filename:join([DbDir, "secrets/wallet_su.json"]);
                P -> P
            end;
        Explicit -> Explicit
    end.

resolve_gateway_url(Opts) ->
    case maps:get(gateway_url, Opts, undefined) of
        undefined ->
            case os:getenv("AO_GATEWAY_URL") of
                false ->
                    case os:getenv("AO_MU_URL") of
                        false -> "http://127.0.0.1:8735";
                        "" -> "http://127.0.0.1:8735";
                        MuUrl -> MuUrl
                    end;
                "" -> "http://127.0.0.1:8735";
                Gw -> Gw
            end;
        Explicit -> Explicit
    end.

resolve_cu_url(Opts) ->
    case maps:get(cu_url, Opts, undefined) of
        undefined ->
            case os:getenv("AO_CU_URL") of
                false -> "";
                "" -> "";
                CuUrl -> CuUrl
            end;
        Explicit -> to_list(Explicit)
    end.

resolve_genesis_id(Opts, DbDir) ->
    case maps:get(genesis_id, Opts, undefined) of
        undefined ->
            case maps:is_key(arbitrator_id, Opts) of
                true -> <<>>;
                false ->
                    case os:getenv("AO_GENESIS_ID") of
                        false ->
                            GenFile = filename:join([DbDir, "ao_process/process_genesis_id.txt"]),
                            read_file_trimmed(GenFile);
                        "" -> <<>>;
                        Id -> list_to_binary(string:trim(Id))
                    end
            end;
        Explicit -> to_bin(Explicit)
    end.

resolve_genesis_hash(Opts, DbDir) ->
    case maps:get(expected_genesis_hash, Opts, undefined) of
        undefined ->
            case os:getenv("AO_GENESIS_HASH") of
                false ->
                    HashFile = filename:join([DbDir, "ao_process/genesis_hash.txt"]),
                    read_file_trimmed(HashFile);
                "" -> <<>>;
                H -> list_to_binary(string:trim(H))
            end;
        Explicit -> to_bin(Explicit)
    end.

resolve_arbitrator_id(Opts, DbDir) ->
    case maps:get(arbitrator_id, Opts, undefined) of
        undefined ->
            case os:getenv("AO_PROCESS_ID") of
                false ->
                    ProcFile = filename:join([DbDir, "ao_process/process_id.txt"]),
                    read_file_trimmed(ProcFile);
                "" -> <<>>;
                Id -> list_to_binary(string:trim(Id))
            end;
        Explicit -> to_bin(Explicit)
    end.

resolve_worker_endpoint(Opts) ->
    case maps:get(worker_endpoint, Opts, undefined) of
        undefined ->
            case os:getenv("HYDB_WORKER_ENDPOINT") of
                false ->
                    case os:getenv("SU_URL") of
                        false ->
                            NodeId = case os:getenv("HYDB_NODE_ID") of
                                false -> "sd-node-1";
                                "" -> "sd-node-1";
                                NId -> NId
                            end,
                            Port = case os:getenv("SU_PORT") of
                                false -> "8735";
                                "" -> "8735";
                                P -> P
                            end,
                            list_to_binary("http://" ++ NodeId ++ ":" ++ Port);
                        "" ->
                            list_to_binary("http://sd-node-1:8735");
                        SuUrl ->
                            list_to_binary(SuUrl)
                    end;
                "" ->
                    list_to_binary("http://sd-node-1:8735");
                Ep ->
                    list_to_binary(Ep)
            end;
        Explicit -> to_bin(Explicit)
    end.

read_file_trimmed(Path) ->
    case file:read_file(Path) of
        {ok, Bin} -> list_to_binary(string:trim(binary_to_list(Bin)));
        _ -> <<>>
    end.

extract_genesis_hash(#{<<"genesis">> := #{<<"genesis_hash">> := H}}) -> to_bin(H);
extract_genesis_hash(#{<<"genesis_hash">> := H}) -> to_bin(H);
extract_genesis_hash(_) -> <<>>.

extract_arbitrator_id(#{<<"arbitrator_process_id">> := AId}) -> to_bin(AId);
extract_arbitrator_id(_) -> <<>>.

resolve_poll_interval(Opts) ->
    case maps:get(poll_interval_ms, Opts, undefined) of
        undefined ->
            case os:getenv("HYDB_POLL_INTERVAL_MS") of
                false -> 3000;
                "" -> 3000;
                Val -> to_int(Val, 3000)
            end;
        Explicit -> to_int(Explicit, 3000)
    end.

to_bin(V) when is_binary(V) -> V;
to_bin(V) when is_list(V) -> list_to_binary(V);
to_bin(V) when is_atom(V) -> atom_to_binary(V, utf8);
to_bin(V) when is_integer(V) -> integer_to_binary(V);
to_bin(_) -> <<>>.

to_list(V) when is_list(V) -> V;
to_list(V) when is_binary(V) -> binary_to_list(V);
to_list(V) when is_atom(V) -> atom_to_list(V);
to_list(V) when is_integer(V) -> integer_to_list(V);
to_list(_) -> "".

to_int(V, _Def) when is_integer(V) -> V;
to_int(V, Def) when is_binary(V) ->
    case catch binary_to_integer(string:trim(V)) of
        I when is_integer(I) -> I;
        _ -> Def
    end;
to_int(V, Def) when is_list(V) ->
    case catch list_to_integer(string:trim(V)) of
        I when is_integer(I) -> I;
        _ -> Def
    end;
to_int(_, Def) -> Def.
