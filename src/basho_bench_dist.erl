-module(basho_bench_dist).

-export([main/1]).

-record(state, { ops,
                 start_time = now(),
                 last_write_time = now(),
                 report_interval,
                 errors_since_last_report = false,
                 summary_file,
                 errors_file}).

main([]) ->
    io:format("Usage: basho_bench_dist DURATION (in mins) OUT_DIR IN_DIRS..~n");

main(Configs) ->
    [DurationStr,OutDir|InDirs] = Configs,
    {Duration, _} = string:to_integer(DurationStr),
    Files = lists:flatmap(fun(D) ->
                                  filelib:wildcard(D ++ "/*histogram.bin")
                              end, InDirs),

    OpsDict = lists:foldl(
            fun(F, Dict) ->
                {ok, HistBin} = file:read_file(F),
                HUE = binary_to_term(HistBin), % {Hist, Units, Errors}
                Parts = string:tokens(F, "/_"),
                Op = lists:nth(length(Parts) - 1, Parts),
                dict:append(Op, HUE, Dict)
            end, dict:new(), Files),

    Agg = dict:map(
            fun(_Op, HUE) ->
                {Hists, Units, Errors} = lists:unzip3(HUE),
                {basho_stats_histogram:merge(Hists),
                 lists:sum(Units),
                 lists:sum(Errors)} 
            end, OpsDict),

    {ok, SummaryFile} = file:open(OutDir ++ "/summary.csv", [raw, binary, write]),
    file:write(SummaryFile, <<"elapsed, window, total, successful, failed\n">>),

    {ok, ErrorsFile} = file:open(OutDir ++ "/errors.csv", [raw, binary, write]),
    file:write(ErrorsFile, <<"\"error\",\"count\"\n">>),

    State = #state { ops = dict:fetch_keys(OpsDict),
                     start_time = 0,
                     last_write_time = 0,
                     report_interval = 0,
                     errors_since_last_report = false,
                     summary_file = SummaryFile,
                     errors_file = ErrorsFile},
    process_stats(Duration * 60 + 1, State, Agg, OutDir).

%% copied from basho_bench, with some slight modifications
process_stats(Elapsed = Window, State, Agg, OutDir) ->
    %% Time to report latency data to our CSV files
    {Oks, Errors} = lists:foldl(fun(Op, {TotalOks, TotalErrors}) ->
                                  {Oks, Errors} = report_latency(Elapsed, Window, Op, Agg, OutDir),
                                        {TotalOks + Oks, TotalErrors + Errors}
                                end, {0,0}, State#state.ops),

    %% Write summary
    file:write(State#state.summary_file,
               io_lib:format("~w, ~w, ~w, ~w, ~w\n",
                             [Elapsed,
                              Window,
                              Oks + Errors,
                              Oks,
                              Errors])).

%%
%% Write latency info for a given op to the appropriate CSV. Returns the
%% number of successful and failed ops in this window of time.
%%
report_latency(Elapsed, Window, Op, Agg, OutDir) ->
    {Hist, Units, Errors} = dict:fetch(Op, Agg),
    case basho_stats_histogram:observations(Hist) > 0 of
        true ->
            {Min, Mean, Max, _, _} = basho_stats_histogram:summary_stats(Hist),
            Line = io_lib:format("~w, ~w, ~w, ~w, ~.1f, ~.1f, ~.1f, ~.1f, ~.1f, ~w, ~w\n",
                                 [Elapsed,
                                  Window,
                                  Units,
                                  Min,
                                  float(Mean),
                                  float(basho_stats_histogram:quantile(0.500, Hist)),
                                  float(basho_stats_histogram:quantile(0.950, Hist)),
                                  float(basho_stats_histogram:quantile(0.990, Hist)),
                                  float(basho_stats_histogram:quantile(0.999, Hist)),
                                  Max,
                                  Errors]),
            Perc = lists:map(fun(P) ->
                                 float(basho_stats_histogram:quantile(P, Hist))
                             end,
                             [ F / 100 || F <- lists:seq(1, 99) ] ++ [0.999]),
            LinePerc = io_lib:format(
                           string:join(lists:duplicate(100, "~.1f"), ", ") ++ "\n",
                           Perc);
        false ->
            io:format("No data for op: ~p\n", [Op]),
            Line = io_lib:format("~w, ~w, 0, 0, 0, 0, 0, 0, 0, 0, ~w\n",
                                 [Elapsed,
                                  Window,
                                  Errors]),
            LinePerc = ""
    end,
    ok = file:write(op_csv_file({Op, Op}, OutDir), Line),
    ok = file:write(op_perc_file({Op, Op}, OutDir), LinePerc),
    {Units, Errors}.

op_csv_file({Label, _Op}, OutDir) ->
    Fname = OutDir ++ "/" ++ normalize_label(Label) ++ "_latencies.csv",
    {ok, F} = file:open(Fname, [raw, binary, write]),
    ok = file:write(F, <<"elapsed, window, n, min, mean, median, 95th, 99th, 99_9th, max, errors\n">>),
    F.

op_perc_file({Label, _Op}, OutDir) ->
    Fname = OutDir ++ "/" ++ normalize_label(Label) ++ "_percentiles.csv",
    {ok, F} = file:open(Fname, [raw, binary, write]),
    Header = lists:map(fun(N) -> "p" ++ integer_to_list(N) end,
                       lists:seq(1, 99)) ++ ["99_9"],
    ok = file:write(F, string:concat(string:join(Header, ", "), "\n")),
    F.

normalize_label(Label) when is_list(Label) ->
    replace_special_chars(Label);
normalize_label(Label) when is_binary(Label) ->
    normalize_label(binary_to_list(Label));
normalize_label(Label) when is_integer(Label) ->
    normalize_label(integer_to_list(Label));
normalize_label(Label) when is_atom(Label) ->
    normalize_label(atom_to_list(Label));
normalize_label(Label) when is_tuple(Label) ->
    Parts = [normalize_label(X) || X <- tuple_to_list(Label)],
    string:join(Parts, "-").

replace_special_chars([H|T]) when
      (H >= $0 andalso H =< $9) orelse
      (H >= $A andalso H =< $Z) orelse
      (H >= $a andalso H =< $z) ->
    [H|replace_special_chars(T)];
replace_special_chars([_|T]) ->
    [$-|replace_special_chars(T)];
replace_special_chars([]) ->
    [].

