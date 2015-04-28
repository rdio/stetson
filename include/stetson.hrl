%% This Source Code Form is subject to the terms of
%% the Mozilla Public License, v. 2.0.
%% A copy of the MPL can be found in the LICENSE file or
%% you can obtain it at http://mozilla.org/MPL/2.0/.
%%
%% @author Brendan Hay
%% @copyright (c) 2012 Brendan Hay <brendan@soundcloud.com>
%% @doc
%%

-define(SERVER, stetson_server).

-record(s, {sock               :: gen_udp:socket(),
            host = "localhost" :: string() | inet:ip_address(),
            prefix = ""        :: string(),
            port = 8126        :: inet:port_number(),
            ns = ""            :: string()}).
