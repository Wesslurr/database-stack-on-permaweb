#!/bin/bash
tmux send-keys -t hb-primary '
LargeT = hd(lists:reverse(lists:sort([T || T <- ets:all(), is_reference(T), ets:info(T, size) > 1000]))).
Before = ets:info(LargeT, size).
Keys = ets:select(LargeT, [{{{<<"$1">>, "_"}, "_"}, [], ["$1"]}]).
io:format("Total keys before: ~p~n", [Before]).
' C-m
sleep 0.8
tmux capture-pane -pt hb-primary -S -10
