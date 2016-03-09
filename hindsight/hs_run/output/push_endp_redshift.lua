-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

local ds = require "derived_stream"

local function get_uuid()
    return string.format("%X%X%X%X-%X%X-%X%X-%X%X-%X%X%X%X%X",
                         string.byte(read_message("Uuid"), 1, 16))
end

local name = read_config("table_prefix") or "push_endpoint_requests"
local schema = {
--   column name                   field type   length  attributes   field name
    {"timestamp",                  "TIMESTAMP", nil,    "SORTKEY",   "Timestamp"},
    {"uuid",                       "VARCHAR",   36,     nil,         get_uuid},
    {"remote_ip",                  "VARCHAR",   45,     nil,         "Fields[remote_ip]"},
    {"uaid_hash",                  "VARCHAR",   56,     nil,         "Fields[uaid_hash]"},
    {"message",                    "VARCHAR",   200,    nil,         "Fields[message]"},
    {"error",                      "BOOLEAN",   nil,    nil,         "Fields[error]"},
    {"user_agent",                 "VARCHAR",   1000,   nil,         "Fields[user_agent]"},
    {"task_uuid",                  "VARCHAR",   36,     nil,         "Fields[task_uuid]"},
    {"channel_id",                 "VARCHAR",   36,     nil,         "Fields[channelID]"},
    {"system_name",                "VARCHAR",   200,    nil,         "Fields[system]"}
}

process_message, timer_event = ds.load_schema(name, schema)
