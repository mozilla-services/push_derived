-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

local ds = require "derived_stream"

local name = "push_endpoint_requests"
local schema = {
--   column name                   field type   length  attributes   field name
    {"timestamp",                  "TIMESTAMP", nil,    "SORTKEY",   "Timestamp"},
    {"remote_ip",                  "VARCHAR",   45,     nil,         "Fields[remote-ip]"},
    {"uaid_hash",                  "VARCHAR",   56,     nil,         "Fields[uaid_hash]"},
    {"message",                    "VARCHAR",   200,    nil,         "Fields[message]"},
    {"error",                      "BOOL",      nil,    nil,         "Fields[error]"},
    {"user_agent",                 "VARCHAR",   1000,   nil,         "Fields[user-agent]"},
    {"task_uuid",                  "VARCHAR",   36,     nil,         "Fields[task_uuid]"},
    {"channel_id",                 "VARCHAR",   36,     nil,         "Fields[channel_id]"},
    {"system_name",                "VARCHAR",   200,    nil,         "Fields[system]"}
}

process_message, timer_event = ds.load_schema(name, schema)

