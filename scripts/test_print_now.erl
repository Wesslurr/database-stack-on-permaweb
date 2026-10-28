begin
  NowData = hb_ao:resolve(Base, <<"now/results/data">>, Opts),
  binary_to_list(element(2, NowData))
end.
