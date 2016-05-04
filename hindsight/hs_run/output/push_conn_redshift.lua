-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

local clf = require "common_log_format"
local ds = require "derived_stream"

local function get_uuid()
    return string.format("%X%X%X%X-%X%X-%X%X-%X%X-%X%X%X%X%X",
                         string.byte(read_message("Uuid"), 1, 16))
end

local browser, browser_os, browser_version

local function get_browser_os()
    return browser_os
end

local function get_browser_version()
    return browser_version
end

local name = read_config("table_prefix") or "push_conn_requests"
local schema = {
--   column name                   field type   length  attributes   field name
    {"timestamp",                  "TIMESTAMP", nil,    "SORTKEY",   "Timestamp"},
    {"uuid",                       "VARCHAR",   36,     nil,         get_uuid},
    {"remote_ip",                  "VARCHAR",   45,     nil,         "Fields[remote_ip]"},
    {"uaid_hash",                  "VARCHAR",   56,     nil,         "Fields[uaid_hash]"},
    {"message",                    "VARCHAR",   200,    nil,         "Fields[message]"},
    {"error",                      "BOOLEAN",   nil,    nil,         "Fields[error]"},
    {"user_agent",                 "VARCHAR",   1000,   nil,         "Fields[user_agent]"},
    {"channel_id",                 "VARCHAR",   36,     nil,         "Fields[channelID]"},
    {"message_id",                 "VARCHAR",   200,    nil,         "Fields[message_id]"},
    {"message_size",               "INTEGER",   nil,    nil,         "Fields[message_size]"},
    {"message_source",             "VARCHAR",   40,     nil,         "Fields[message_source]"},
    {"browser_os",                 "VARCHAR",   150,    nil,         get_browser_os},
    {"browser_version",            "VARCHAR",   50,     nil,         get_browser_version}
}

local derived_process_message
derived_process_message, timer_event = ds.load_schema(name, schema)

function process_message()
    local ua = read_message("Fields[user_agent]")
    browser, browser_version, browser_os = clf.normalize_user_agent(ua)
    return derived_process_message()
end
