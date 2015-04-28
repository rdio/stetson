%% This Source Code Form is subject to the terms of
%% the Mozilla Public License, v. 2.0.
%% A copy of the MPL can be found in the LICENSE file or
%% you can obtain it at http://mozilla.org/MPL/2.0/.
%%
%% @author Brendan Hay
%% @copyright (c) 2012 Brendan Hay <brendan@soundcloud.com>
%% @doc
%%

-module(stetson).

-behaviour(application).

-include("include/stetson.hrl").

%% API
-export([start/0,
         stop/0,
         counter/2,
         counter/3,
         gauge/2,
         gauge/3,
         tc/2,
         tc/4,
         timer/2,
         timer/3,
         tc_band/2,
         tc_band/4]).

%% Callbacks
-export([start/2,
         stop/1]).

%%
%% API
%%

-spec start() -> ok.
%% @doc
start() -> application:start(?MODULE).

-spec stop() -> ok.
%% @doc
stop() -> application:stop(?MODULE).

-spec counter(atom() | string(), integer()) -> ok.
%% @doc
counter(Stat, Step) -> stetson_server:cast({counter, Stat, Step}).

-spec counter(atom() | string(), integer(), float()) -> ok.
%% @doc
counter(Bucket, Step, Rate) -> stetson_server:cast({counter, Bucket, Step, Rate}).

-spec gauge(atom() | string(), integer()) -> ok.
%% @doc
gauge(Stat, Step) -> stetson_server:cast({gauge, Stat, Step}).

-spec gauge(atom() | string(), integer(), float()) -> ok.
%% @doc
gauge(Bucket, Step, Rate) -> stetson_server:cast({gauge, Bucket, Step, Rate}).

-spec timer(atom() | string(), pos_integer()) -> ok.
%% @doc
timer(Bucket, Ms) -> stetson_server:cast({timer, Bucket, Ms}).

-spec timer(atom() | string(), pos_integer(), float()) -> ok.
%% @doc
timer(Bucket, Ms, Rate) -> stetson_server:cast({timer, Bucket, Ms, Rate}).

-spec band_ms(atom() | string(), pos_integer()) -> ok.
band_ms(Bucket, Ms) ->
    stetson_server:cast({band_ms, Bucket, Ms}).


-spec tc(string() | atom(), fun(() -> any())) -> any().
tc(Bucket, Fun) ->
    {MicroS, R} = timer:tc(Fun),
    timer(Bucket, MicroS div 1000),
    R.

-spec tc(string() | atom(), atom(), atom(), list()) -> any().
tc(Bucket, M, F, A) ->
    {MicroS, R} = timer:tc(M, F, A),
    timer(Bucket, MicroS div 1000),
    R.

-spec tc_band(string() | atom(), fun(() -> any())) -> any().
tc_band(Bucket, Fun) ->
    {MicroS, R} = timer:tc(Fun),
    band_ms(Bucket, MicroS div 1000),
    R.

-spec tc_band(string() | atom(), atom(), atom(), list()) -> any().
tc_band(Bucket, M, F, A) ->
    {MicroS, R} = timer:tc(M, F, A),
    band_ms(Bucket, MicroS div 1000),
    R.

%%
%% Callbacks
%%

-spec start(normal, _Args) -> {ok, pid()} | {error, _}.
%% @hidden
start(normal, _Args) ->
    case stetson_sup:start_link(env('statsd.uri'), env('graphite.ns')) of
        ignore -> {error, sup_returned_ignore};
        Ret    -> Ret
    end.

-spec stop(_) -> ok.
%% @hidden
stop(_Args) -> ok.

%%
%% Private
%%

-spec env(atom()) -> any().
%% @doc
env(Key) ->
    application:load(?MODULE),
    case application:get_env(?MODULE, Key) of
        undefined   -> error({config_not_found, Key});
        {ok, Value} -> os(Value)
    end.

-spec os(atom() | string()) -> string().
%% @doc
os(Value) when is_atom(Value) ->
    case os:getenv(atom_to_list(Value)) of
        false -> error({env_not_set, Value});
        Env   -> Env
    end;
os(Value) ->
    Value.
