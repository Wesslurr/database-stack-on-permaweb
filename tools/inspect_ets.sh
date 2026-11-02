#!/bin/bash
tmux send-keys -t hb-primary '
LargeT = hd(lists:reverse(lists:sort([T || T <- ets:all(), is_reference(T), ets:info(T, size) > 1000]))).
F = fun Next(0, _, Acc) -> Acc; Next(N, K, Acc) -> case ets:next(LargeT, K) of "$end" -> Acc; K2 -> Next(N-1, K2, [K2 | Acc]) end end.
io:format("Sample 10 keys: ~p~n", [F(10, ets:first(LargeT), [ets:first(LargeT)])]).
' C-m
sleep 0.8
tmux capture-pane -pt hb-primary -S -20
