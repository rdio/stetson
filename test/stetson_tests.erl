-module(stetson_tests).
-include_lib("eunit/include/eunit.hrl").
-include("include/stetson.hrl").

fake_s() ->
    #s{ prefix = "test." }.

% since a string is a list the gaurd thinks it can handle this...
stats_key_list_string_test() ->
    ?assertEqual("test.foo", stetson_server:key(fake_s(), ["foo"])).

stats_key_list_atom_test() ->
    ?assertEqual("test.foo", stetson_server:key(fake_s(), [foo])).

stats_key_atom_test() ->
    ?assertEqual("test.foo", stetson_server:key(fake_s(), foo)).

stats_key_string_test() ->
    ?assertEqual("test.foo", stetson_server:key(fake_s(), "foo")).

stats_key_list_test() ->
    ?assertEqual("test.foo.bar", stetson_server:key(fake_s(), ["foo.", bar])).

builds_default_prefix() ->
    ?assert(erlang:is_string(stetson_server:default_prefix())).


timer_test_() ->
    {timeout, 1000, {setup, fun() ->
        ok = application:start(stetson)
    end,
    fun(_) ->
        application:stop(stetson)
    end,
    fun(_) -> [
        {"tc fun", fun() ->
            ?assertEqual(works, stetson:tc("foo", fun() -> works end))
        end},
        {"tc MFA", fun() ->
            ?assertEqual(true, stetson:tc("foo", erlang, is_atom, [works]))
        end}
    ] end } }.

