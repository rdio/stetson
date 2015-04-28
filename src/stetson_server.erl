%% This Source Code Form is subject to the terms of
%% the Mozilla Public License, v. 2.0.
%% A copy of the MPL can be found in the LICENSE file or
%% you can obtain it at http://mozilla.org/MPL/2.0/.
%%
%% @author Brendan Hay
%% @copyright (c) 2012 Brendan Hay <brendan@soundcloud.com>
%% @doc
%%

-module(stetson_server).

-behaviour(gen_server).

-include("include/stetson.hrl").

-ifdef(TEST).
-compile(export_all).
-endif.

%% API
-export([start_link/2,
         cast/1,
         cast_spawned/1]).

%% Callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-type message() :: {connect, pid(), inet:socket(), erlang:timestamp()} |
                   {establish, pid(), node()} |
                   {band_ms | counter | gauge | timer, atom() | string(), integer()} |
                   {counter | gauge | timer, atom() | string(), integer(), float()}.

-export_type([message/0]).

-define(STETSON_INFO, stetson_info).
%%
%% API
%%

-spec start_link(string(), string()) -> ignore | {error, _} | {ok, pid()}.
%% @doc Start the stats process
start_link(Uri, Ns) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, {Uri, Ns}, []).

%% @doc Wrapper for minimizing the impact in the process registering a stat.
%% It will start a process that will construct the specfic stat to be sent
%% by the gen_server.
-spec cast(message()) -> ok.
cast(Msg) ->
    spawn(?MODULE, cast_spawned, [Msg]),
    ok.

%% @doc Real worker, will process the state and send it to the gen_server process
%% so it can be sent by udp to the statsd server.
-spec cast_spawned(message()) -> ok.
cast_spawned(Msg) -> 
    case whereis(?SERVER) of
        undefined ->
            %% Server hasn't started yet
            ok;
        _ ->
            stat(Msg)
    end.

%%
%% Callbacks
%%

-spec init({string(), string()}) -> {ok, #s{}} | {stop, cant_resolve_statsd_host}.
%% @hidden
init({Uri, Ns}) ->
    random:seed(now()),
    {Host, Port} = split_uri(Uri, 8126),
    %% We save lots of calls into inet_gethost_native by looking this up once:
    case convert_to_ip_tuple(Host) of
        undefined ->
            {stop, cant_resolve_statsd_host};
        IP ->
            error_logger:info_msg("stetson using statsd at ~s:~B (resolved to: ~w)", [Host, Port, IP]),
            {ok, Sock} = gen_udp:open(0, [binary]),
            set_ets(Ns, Sock, IP, Port),
            {ok, #s{sock = Sock, host = IP, port = Port}}
    end.

-spec handle_call(message(), _, #s{}) -> {reply, ok, #s{}}.
%% @hidden
handle_call(_Msg, _From, State) -> {reply, ok, State}.

-spec handle_cast(any(), #s{}) -> {noreply, #s{}}.
handle_cast(Msg, State) ->
    error_logger:warning_msg("Unhandled cast to stetson_server: ~p",[Msg]),
    {noreply, State}.

-spec handle_info(_Info, #s{}) -> {noreply, #s{}}.
%% @hidden
handle_info(_Info, State) -> {noreply, State}.

-spec terminate(_, #s{}) -> ok.
%% @hidden
terminate(_Reason, #s{sock = Sock}) ->
    ok = gen_udp:close(Sock),
    ok.

-spec code_change(_, #s{}, _) -> {ok, #s{}}.
%% @hidden
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%
%% Private
%%

-spec band_for(pos_integer()) -> atom().
band_for(Ms) when Ms <     50 -> '50ms';
band_for(Ms) when Ms <    250 -> '250ms';
band_for(Ms) when Ms <   1000 -> '1s';
band_for(Ms) when Ms <  10000 -> '10s';
band_for(Ms) when Ms <  60000 -> '1m';
band_for(Ms) when Ms < 300000 -> '5m';
band_for(_Ms) -> inf.

-spec key(string() | atom()) -> string().
key(Bucket) when is_list(Bucket)->
    case hd(Bucket) of
        Foo when is_list(Foo); is_atom(Foo) ->
            lists:concat([get_prefix() | Bucket]);
        _ ->
            lists:concat([get_prefix() | [Bucket]])
    end;

key(Bucket) when is_atom(Bucket)->
    lists:concat([get_prefix() | [Bucket]]).

-spec stat({counter | gauge | timer, string() | atom(), integer(), float()}|
           {band_ms | counter | gauge | timer, string() | atom(), integer()}) -> ok.
%% @private Create a statistic entry with or without a sample rate
stat({Type, Bucket, N, Rate}) when Rate < 1.0 ->
    case {Type, random:uniform() =< Rate} of
        {counter, true} -> translate("~s:~p|c|@~p",  [key(Bucket), N, Rate]);
        {gauge, true}   -> translate("~s:~p|g|@~p",  [key(Bucket), N, Rate]);
        {timer, true}   -> translate("~s:~p|ms|@~p", [key(Bucket), N, Rate]);
        _               -> ok
    end;
stat({band_ms, Bucket, N}) ->
    translate("~s:~p|ms", [key(Bucket), N]),
    translate("~s.~s:1|c", [key(Bucket), band_for(N)]);
stat({counter, Bucket, N}) ->
    translate("~s:~p|c", [key(Bucket), N]);
stat({gauge, Bucket, N}) ->
    translate("~s:~p|g", [key(Bucket), N]);
stat({timer, Bucket, N}) ->
    translate("~s:~p|ms", [key(Bucket), N]);
stat({_Type, _Bucket, _N}) ->
    ok;
stat(Msg) ->
    error_logger:warning_msg("Unhandled cast to stetson_server: ~p",[Msg]).

-spec translate(string(), [atom() | non_neg_integer()]) -> {ok, iolist()}.
%% @private Returns the formatted iolist packet, prepending the ns, so it can
%% be sent over the udp socket in the gen_server process.
translate(Format, Args) ->
    send(io_lib:format("~s." ++ Format, [get_ns()|Args])).

-spec send(iolist()) -> ok.
%% @private Sends a formatted iolist packet over the udp socket.
send(Msg) ->
    {Socket, IP, Port} = get_socket(),
    gen_udp:send(Socket, IP, Port, Msg).

-spec split_uri(string(), inet:port_number()) -> {nonempty_string(), inet:port_number()}.
%% @private
split_uri(Uri, Default) ->
    case string:tokens(Uri, ":") of
        [H|P] when length(P) > 0 -> {H, list_to_integer(lists:flatten(P))};
        [H|_]                    -> {H, Default}
    end.

-spec convert_to_ip_tuple(inet:ip_address()) -> inet:ip_address();
                         (string()) -> inet:ip_address() | string() | undefined.
%% They provided an erlang tuple already:
convert_to_ip_tuple({_,_,_,_} = IPv4)          -> IPv4;
convert_to_ip_tuple({_,_,_,_,_,_,_,_} = IPv6)  -> IPv6;
%% Maybe they provided an IP as a string, otherwise do a DNS lookup
convert_to_ip_tuple(Hostname)                  ->
    case inet_parse:address(Hostname) of
        {ok, IP}   -> IP;
        {error, _} -> dns_lookup(Hostname)
    end.

%% We need an option to bind the UDP socket to a v6 addr before it's worth
%% trying to lookup AAAA records for the statsd host. Just v4 for now:
dns_lookup(Hostname) -> resolve_hostname_by_family(Hostname, inet).

%%dns_lookup(Hostname) ->
%%    case resolve_hostname_by_family(Hostname, inet6) of
%%        undefined -> resolve_hostname_by_family(Hostname, inet);
%%        IP        -> IP
%%    end.

resolve_hostname_by_family(Hostname, Family) ->
    case inet:getaddrs(Hostname, Family) of
        {ok, L}   -> random_element(L);
        {error,_} -> undefined
    end.

random_element(L) when is_list(L) ->
    lists:nth(random:uniform(length(L)), L).

%%
%%
%%

%% @private Generates the default prefix.
-spec default_prefix() -> string().
default_prefix() ->
    {ok, Host} = inet:gethostname(),
    lists:concat(["servers.", Host, "."]).

%% @private Refactored functionality that will initialize the shared ETS
%% for allowing external processes to build keys without impacting the gen_server
-spec set_ets(string(), port(), inet:ip_address(), inet:port_number()) -> ok.
set_ets(Ns, Sock, IP, Port) ->
    ets:new(?STETSON_INFO, [named_table, protected, {read_concurrency, true}]),
    Prefix = default_prefix(),
    ets:insert(?STETSON_INFO, {prefix, Prefix}),
    ets:insert(?STETSON_INFO, {ns, Ns}),
    ets:insert(?STETSON_INFO, {socket, {Sock, IP, Port}}).

%% @private Lookups for the configured prefix in the shared ETS.
-spec get_prefix() -> string().
get_prefix() ->
    [{prefix, Prefix}] = ets:lookup(?STETSON_INFO, prefix),
    Prefix.

%% @private Lookups for the configured ns in the shared ETS.
-spec get_ns() -> string().
get_ns() ->
    [{ns, Ns}] = ets:lookup(?STETSON_INFO, ns),
    Ns.

%% @private Lookups for the configured socket and remote statsd server info.
-spec get_socket() -> {port(), inet:ip_address(), inet:port_number()}.
get_socket() ->
    [{socket, Info}] = ets:lookup(?STETSON_INFO, socket),
    Info.
