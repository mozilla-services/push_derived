-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

local io = require "io"
local os = require "os"
local string = require "string"
local heka_stream_reader = require "heka_stream_reader"

local hsr = heka_stream_reader.new("push_logs")
local infile = io.stdin

local base_dir = read_config("base_dir") or "/opt/push_load"
local bucket = read_config("bucket") or "heka-logs"
local region = read_config("region") or "us-east-1"
local num_days = read_config("num_days") or 1

local function process_day(date)
    local s3_prefix = os.date("shared/%Y-%m", date)
    local fname_date = os.date("%Y%m%d", date)
    local s3_fname_head = string.format("autopush-app.log-%s", fname_date)
    local s3_fname_match_head = string.format("autopush%%-app%%.log%%-%s", fname_date)
    local ls_cmd = string.format("aws s3 ls s3://heka-logs/%s/%s", s3_prefix, s3_fname_head)
    local ls_fd = io.popen(ls_cmd)
    local ls_output = ls_fd:read("*a")
    local s3_fname = string.match(ls_output, s3_fname_match_head .. "[%a%d%_%-]*%.gz")

    local s3cat_cmd = string.format("%s/bin/s3cat -read_timeout=3600 -bucket='%s' -aws-region='%s' %s/%s -",
                                    base_dir, bucket, region, s3_prefix, s3_fname)
    local infile = io.popen(s3cat_cmd)
    local found, consumed, read
    repeat
        repeat
            found, consumed, read = hsr:find_message(infile)
            if found then
                inject_message(hsr)
            end
        until not found
    until read == 0
end

function process_message()
    for i = num_days, 1, -1 do
        local date = os.time()-(24*60*60*i)
        process_day(date)
    end
    return 0
end
