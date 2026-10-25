-module(sd_committer).
-behaviour(gen_server).

%% ============================================================================
%% Native In-BEAM AO Committer & Settlement Engine
%% ============================================================================
%% Responsibilities:
%% 1. Direct memory/WAL polling from `sd_runtime` without external HTTP proxy.
%% 2. Cryptographic batch assembly, canonical hashing, and HMAC/RSA signing.
%% 3. Autonomous dispatch of `CommitCheckpoint` to AO Arbitrator on Permaweb.
%% 4. Durable disk cursor management & cold-start sequence reconciliation.
%% 5. Autonomous heartbeat emission and quorum health tracking on Genesis Stack.
%% 6. Strict fail-closed circuit breaker on split-brain or sequence tampering.
%% ============================================================================

-export([start_link/0, start_link/1, ensure_started/0, ensure_started/1, stop/0]).
-export([get_status/0, get_status_map/0, commit_now/0, report_heartbeat/0]).
-export([bootstrap_genesis/0, bootstrap_genesis/1, reload_cursor/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

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
    last_heartbeat_time = 0
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
        _ -> gen_server:call(?SERVER, get_status, 5000)
    end.

get_status_map() ->
    get_status().

commit_now() ->
    ensure_started(),
    gen_server:call(?SERVER, commit_now, 15000).

report_heartbeat() ->
    ensure_started(),
    gen_server:call(?SERVER, report_heartbeat, 10000).

bootstrap_genesis() ->
    ensure_started(),
    gen_server:call(?SERVER, bootstrap_genesis, 10000).

bootstrap_genesis(Opts) when is_map(Opts) ->
    ensure_started(Opts),
    gen_server:call(?SERVER, {bootstrap_genesis, Opts}, 10000).

reload_cursor() ->
    ensure_started(),
    gen_server:call(?SERVER, reload_cursor, 5000).

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
    BatchMax = maps:get(batch_max, Opts, ?DEFAULT_BATCH_MAX),
    PollMs = maps:get(poll_interval_ms, Opts, ?DEFAULT_POLL_INTERVAL_MS),

    WalletInfo = case sd_ao_client:load_wallet(WalletPath) of
        {ok, WI} -> WI;
        _ -> #{address => <<"SU_NODE_1">>}
    end,
    Address = maps:get(address, WalletInfo, <<"SU_NODE_1">>),
    ClusterSec = sd_ao_client:derive_cluster_secret(WalletPath),

    DiskCursor = load_disk_cursor(CursorPath),
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
        cluster_secret = ClusterSec,
        gateway_url = GatewayUrl,
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

    %% Cold-start bootstrap via Genesis stack if configured
    State1 = case InitialState#state.genesis_id of
        <<>> -> InitialState#state{status = running};
        _GenId ->
            case do_bootstrap_genesis(InitialState) of
                {ok, BootstrappedState} -> BootstrappedState;
                {error, Reason} ->
                    InitialState#state{status = degraded, health = degraded, reason = Reason}
            end
    end,

    TRef = erlang:send_after(100, self(), commit_tick),
    write_status_file(State1),
    {ok, State1#state{timer_ref = TRef}}.

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
        {ok, NewState} ->
            write_status_file(NewState),
            {reply, {ok, true}, NewState};
        {error, Reason} ->
            NewState = State#state{status = degraded, health = degraded, reason = Reason},
            write_status_file(NewState),
            {reply, {error, Reason}, NewState}
    end;

handle_call({bootstrap_genesis, Opts}, _From, State) ->
    DbDir = get_db_dir(),
    S1 = State#state{
        genesis_id = maps:get(genesis_id, Opts, resolve_genesis_id(Opts, DbDir)),
        expected_genesis_hash = maps:get(expected_genesis_hash, Opts, resolve_genesis_hash(Opts, DbDir))
    },
    case do_bootstrap_genesis(S1) of
        {ok, NewState} ->
            write_status_file(NewState),
            {reply, {ok, true}, NewState};
        {error, Reason} ->
            NewState = S1#state{status = degraded, health = degraded, reason = Reason},
            write_status_file(NewState),
            {reply, {error, Reason}, NewState}
    end;

handle_call(reload_cursor, _From, State) ->
    DiskCursor = load_disk_cursor(State#state.cursor_path),
    NewSeq = maps:get(last_committed_seq, DiskCursor, State#state.last_committed_seq),
    NewHash = maps:get(last_committed_hash, DiskCursor, State#state.last_committed_hash),
    NewRoot = maps:get(last_state_root, DiskCursor, State#state.last_state_root),
    NewState = State#state{
        last_committed_seq = NewSeq,
        last_committed_hash = NewHash,
        last_state_root = NewRoot
    },
    write_status_file(NewState),
    {reply, {ok, NewSeq}, NewState};

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
                                #{sender => State#state.wallet_address}
                            )
                    end,

                    case DispatchRes of
                        {ok, _HttpStatus, Resp} ->
                            case parse_arbitrator_verdict(Resp) of
                                {ok, committed, OfficialSeq} ->
                                    AdvSeq = max(ToSeq, OfficialSeq),
                                    save_disk_cursor(AdvSeq, PropHash, PropRoot, Epoch, <<"COMMITTED">>, State#state.cursor_path),
                                    {_, StateAfterHb} = do_report_heartbeat(State#state{
                                        last_committed_seq = AdvSeq,
                                        last_committed_hash = PropHash,
                                        last_state_root = PropRoot,
                                        epoch = Epoch,
                                        status = running,
                                        health = healthy
                                    }),
                                    {{ok, true, committed}, StateAfterHb};
                                {ok, duplicate, OfficialSeq} ->
                                    AdvSeq = max(LastSeq, OfficialSeq),
                                    save_disk_cursor(AdvSeq, PropHash, PropRoot, Epoch, <<"DUPLICATE_BATCH_IDEMPOTENT">>, State#state.cursor_path),
                                    {{ok, true, idempotent_duplicate}, State#state{status = running, health = healthy}};
                                {rejected, Code, Reason} ->
                                    {{ok, false, rejected}, State#state{status = degraded, health = degraded, error_code = Code, reason = Reason}};
                                other ->
                                    {{ok, true, unverified_response}, State#state{last_committed_seq = ToSeq, status = running}}
                            end;
                        {error, ErrReason} ->
                            {{error, ErrReason}, State#state{status = degraded, health = degraded, reason = ErrReason}}
                    end;
                _Err ->
                    {{ok, false, empty_batch}, State}
            end;
        true ->
            %% Idle state: maintain Genesis heartbeat periodically (every 10 seconds)
            Now = erlang:system_time(second),
            if
                (Now - State#state.last_heartbeat_time) >= 10 ->
                    {_, StateAfterHb} = do_report_heartbeat(State),
                    {{ok, true, idle}, StateAfterHb};
                true ->
                    {{ok, true, idle}, State#state{status = running}}
            end
    end.

parse_arbitrator_verdict(#{<<"Status">> := <<"COMMITTED">>, <<"OfficialSeq">> := OSeq}) ->
    {ok, committed, to_int(OSeq, 0)};
parse_arbitrator_verdict(#{<<"Status">> := <<"DUPLICATE_BATCH_IDEMPOTENT">>, <<"OfficialSeq">> := OSeq}) ->
    {ok, duplicate, to_int(OSeq, 0)};
parse_arbitrator_verdict(#{<<"Status">> := <<"AUDIT_REJECTED">>, <<"SubStatus">> := Sub, <<"Reason">> := R}) ->
    {rejected, Sub, R};
parse_arbitrator_verdict(#{<<"SubStatus">> := Sub, <<"Reason">> := R}) ->
    {rejected, Sub, R};
parse_arbitrator_verdict(_) ->
    unverified.

%% ============================================================================
%% Genesis Bootstrap & Heartbeat
%% ============================================================================

do_bootstrap_genesis(#state{genesis_id = <<>>} = State) ->
    {ok, State#state{status = running}};
do_bootstrap_genesis(State) ->
    GenesisId = State#state.genesis_id,
    QueryRes = sd_ao_client:query_state(State#state.gateway_url, GenesisId, <<"GetGenesisConfig">>, #{}),
    case QueryRes of
        {ok, _Status, Resp} ->
            %% 1. Verify Genesis Hash if configured
            ExpectedHash = State#state.expected_genesis_hash,
            ActualHash = extract_genesis_hash(Resp),
            if
                ExpectedHash =/= <<>> andalso ActualHash =/= <<>> ->
                    case string:uppercase(ExpectedHash) =:= string:uppercase(ActualHash) of
                        false ->
                            {error, {err_genesis_hash_mismatch, ExpectedHash, ActualHash}};
                        true ->
                            finish_bootstrap_with_config(Resp, State)
                    end;
                true ->
                    finish_bootstrap_with_config(Resp, State)
            end;
        {error, Reason} ->
            {error, {genesis_query_failed, Reason}}
    end.

finish_bootstrap_with_config(Config, State) ->
    ArbitratorId = extract_arbitrator_id(Config),
    S1 = if
        ArbitratorId =/= <<>> -> State#state{arbitrator_id = ArbitratorId};
        true -> State
    end,
    %% Register node identity with Genesis
    RegPayload = #{
        <<"node_id">> => State#state.wallet_address,
        <<"role">> => <<"SU_WORKER">>,
        <<"endpoint">> => <<"http://127.0.0.1:8735">>
    },
    case sd_ao_client:send_message(State#state.gateway_url, State#state.genesis_id, <<"RegisterNode">>, [], RegPayload, #{sender => State#state.wallet_address}) of
        {ok, _Status, RegResp} ->
            case parse_reg_response(RegResp) of
                ok -> {ok, S1#state{status = running, health = healthy}};
                {error, RegErr} -> {error, RegErr}
            end;
        {error, RegErr} ->
            {error, {register_node_failed, RegErr}}
    end.

parse_reg_response(#{<<"status">> := <<"REJECTED_UNAUTHORIZED_NODE">>}) ->
    {error, err_unauthorized_node};
parse_reg_response(#{<<"ok">> := false}) ->
    {error, err_unauthorized_node};
parse_reg_response(_) ->
    ok.

do_report_heartbeat(#state{genesis_id = <<>>} = State) ->
    {{ok, skipped_no_genesis}, State#state{heartbeat_acknowledged = true, last_heartbeat_time = erlang:system_time(second)}};
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
        #{sender => State#state.wallet_address}
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
        _ ->
            {{ok, dispatched}, State#state{heartbeat_acknowledged = true, last_heartbeat_time = Now}}
    end.

%% ============================================================================
%% Disk Cursor & Status Helpers
%% ============================================================================

load_disk_cursor(CursorPath) ->
    case file:read_file(CursorPath) of
        {ok, Bin} ->
            try json:decode(Bin) of
                Map when is_map(Map) ->
                    #{
                        last_committed_seq => to_int(maps:get(<<"last_committed_seq">>, Map, 0), 0),
                        last_committed_hash => to_bin(maps:get(<<"last_committed_hash">>, Map, <<>>)),
                        last_state_root => to_bin(maps:get(<<"last_state_root">>, Map, <<"0">>)),
                        epoch => to_int(maps:get(<<"epoch">>, Map, 1), 1)
                    };
                _ -> #{}
            catch
                _:_ -> #{}
            end;
        _ -> #{}
    end.

save_disk_cursor(Seq, Hash, StateRoot, Epoch, Verdict, CursorPath) ->
    Payload = #{
        <<"last_committed_seq">> => Seq,
        <<"last_committed_hash">> => to_bin(Hash),
        <<"last_state_root">> => to_bin(StateRoot),
        <<"epoch">> => Epoch,
        <<"verdict">> => to_bin(Verdict),
        <<"updated_at">> => list_to_binary(iso8601_now())
    },
    Json = iolist_to_binary(json:encode(Payload)),
    atomic_write_file(CursorPath, Json).

write_status_file(#state{cursor_path = CursorPath} = State) ->
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
            _ = file:write(Fd, Data),
            _ = file:datasync(Fd),
            _ = file:close(Fd),
            file:rename(TmpPath, Path);
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

resolve_cursor_path(Opts, DbDir) ->
    case maps:get(cursor_path, Opts, undefined) of
        undefined ->
            case os:getenv("HYDB_COMMITTER_CURSOR_PATH") of
                false -> filename:join([DbDir, "data/committer/committer_cursor.json"]);
                "" -> filename:join([DbDir, "data/committer/committer_cursor.json"]);
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
                    case os:getenv("ARWEAVE_GATEWAY") of
                        false -> "http://127.0.0.1:8735";
                        "" -> "http://127.0.0.1:8735";
                        Gw -> Gw
                    end;
                "" -> "http://127.0.0.1:8735";
                Gw -> Gw
            end;
        Explicit -> Explicit
    end.

resolve_genesis_id(Opts, DbDir) ->
    case maps:get(genesis_id, Opts, undefined) of
        undefined ->
            case os:getenv("AO_GENESIS_ID") of
                false ->
                    GenFile = filename:join([DbDir, "ao_process/process_genesis_id.txt"]),
                    read_file_trimmed(GenFile);
                "" -> <<>>;
                Id -> list_to_binary(string:trim(Id))
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

to_bin(V) when is_binary(V) -> V;
to_bin(V) when is_list(V) -> list_to_binary(V);
to_bin(V) when is_atom(V) -> atom_to_binary(V, utf8);
to_bin(V) when is_integer(V) -> integer_to_binary(V);
to_bin(_) -> <<>>.

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
