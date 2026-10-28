begin
  W1 = ar_wallet:load_keyfile("/home/weslur/database/secrets/wallet_genesis.json"),
  A1 = hb_util:human_id(ar_wallet:to_address(W1)),
  W2 = ar_wallet:load_keyfile("/home/weslur/database/secrets/wallet.json"),
  A2 = hb_util:human_id(ar_wallet:to_address(W2)),
  W3 = ar_wallet:load_keyfile("/home/weslur/database/secrets/wallet_su.json"),
  A3 = hb_util:human_id(ar_wallet:to_address(W3)),
  io:format("Genesis: ~s~nHot: ~s~nSD1: ~s~n", [A1, A2, A3])
end.
