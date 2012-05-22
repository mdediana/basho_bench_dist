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

    Contents = lists:map(fun(F) ->
                             {ok, HistBin} = file:read_file(F),
                             {Hist, Units, Errors} = binary_to_term(HistBin),
                             Parts = string:tokens(F, "/_"),
                             Op = lists:nth(length(Parts) - 1, Parts),
                             {Op, Hist, Units, Errors}
                         end, Files),

    Ops = lists:usort([ Op || {Op, _, _, _} <- Contents]),
    Agg = lists:map(
           fun(Op) ->
               L = lists:filter(fun(E) -> element(1, E) =:= Op end, Contents),
               Hists  = [ H || {_, H, _, _} <- L],
               Units  = [ U || {_, _, U, _} <- L],
               Errors  = [ E || {_, _, _, E} <- L],
               {Op,
                basho_stats_histogram:merge(Hists),
                lists:sum(Units),
                lists:sum(Errors)} 
           end, Ops),

    {ok, SummaryFile} = file:open(OutDir ++ "/summary.csv", [raw, binary, write]),
    file:write(SummaryFile, <<"elapsed, window, total, successful, failed\n">>),

    {ok, ErrorsFile} = file:open(OutDir ++ "/errors.csv", [raw, binary, write]),
    file:write(ErrorsFile, <<"\"error\",\"count\"\n">>),

    State = #state { ops = Ops,
                     start_time = 0,
                     last_write_time = 0,
                     report_interval = 0,
                     errors_since_last_report = false,
                     summary_file = SummaryFile,
                     errors_file = ErrorsFile},
    process_stats(Duration * 60 + 1, State, Agg).

%% copied from basho_bench, with some slight modifications
process_stats(Elapsed = Window, State, Agg) ->
    %% Time to report latency data to our CSV files
    {Oks, Errors} = lists:foldl(fun(Op, {TotalOks, TotalErrors}) ->
                                  {Oks, Errors} = report_latency(Elapsed, Window, Op, Agg),
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
report_latency(Elapsed, Window, Op, Agg) ->
    {Op, Hist, Units, Errors} = lists:keyfind(Op, 1, Agg),
    case basho_stats_histogram:observations(Hist) > 0 of
        true ->
            {Min, Mean, Max, _, _} = basho_stats_histogram:summary_stats(Hist),
            Line = io_lib:format("~w, ~w, ~w, ~w, ~.1f, ~.1f, ~.1f, ~.1f, ~.1f, ~w, ~w\n",
                                 [Elapsed,
                                  Window,
                                  Units,
                                  Min,
                                  Mean,
                                  basho_stats_histogram:quantile(0.500, Hist),
                                  basho_stats_histogram:quantile(0.950, Hist),
                                  basho_stats_histogram:quantile(0.990, Hist),
                                  basho_stats_histogram:quantile(0.999, Hist),
                                  Max,
                                  Errors]);
        false ->
            io:format("No data for op: ~p\n", [Op]),
            Line = io_lib:format("~w, ~w, 0, 0, 0, 0, 0, 0, 0, 0, ~w\n",
                                 [Elapsed,
                                  Window,
                                  Errors])
    end,
    ok = file:write(op_csv_file({Op, Op}), Line),
    {Units, Errors}.

op_csv_file({Label, _Op}) ->
    Fname = normalize_label(Label) ++ "_latencies.csv",
    {ok, F} = file:open(Fname, [raw, binary, write]),
    ok = file:write(F, <<"elapsed, window, n, min, mean, median, 95th, 99th, 99_9th, max, errors\n">>),
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

