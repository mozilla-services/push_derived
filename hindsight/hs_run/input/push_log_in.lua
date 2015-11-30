-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

local io = require "io"
local os = require "os"
local string = require "string"
local heka_stream_reader = require "heka_stream_reader"

local hsr = heka_stream_reader.new("push_logs")
local infile = io.stdin

function process_message()
    local yesterday = os.time()-24*60*60
    local s3_prefix = os.date("shared/%Y-%m", yesterday)
    local fname_date = os.date("%Y%m%d", yesterday)
    local s3_fname_head = string.format("autopush-app.log-%s", fname_date)
    local s3_fname_match_head = string.format("autopush%%-app%%.log%%-%s", fname_date)
    local ls_cmd = string.format("aws s3 ls s3://heka-logs/%s/%s", s3_prefix, s3_fname_head)
    local ls_fd = io.popen(ls_cmd)
    local ls_output = ls_fd:read("*a")
    local s3_fname = string.match(ls_output, s3_fname_match_head .. "[%a%d%_%-]*%.gz")

    local s3cat_cmd = string.format("/home/ec2-user/push_load/bin/s3cat -bucket='heka-logs' -aws-region='us-east-1' %s/%s -", s3_prefix, s3_fname)
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
    return 0
end
