-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

-- This should be run with the directory in which the source code lives as the
-- current working directory.
package.path = package.path .. ';../hindsight/io_modules/?.lua'
package.cpath = package.cpath .. ';../hindsight/io_modules/?.so'

local driver = require "luasql.postgres"
local io = require "io"
local math = require "math"
local os = require "os"
local string = require "string"
local table = require "table"

-- This is pretty dirty, it loads the redshift config (and a bunch of extra
-- noise from the output plugin configuration) and the report config into this
-- script's global namespace.

dofile("./report.cfg")
-- Redshift settings are in the `db_config` table.
dofile("../hindsight/hs_run/output/push_endp_redshift.cfg")

local debug = false

local env = assert(driver.postgres())
local con = assert(env:connect(db_config.dbname, db_config.user, db_config._password,
                               db_config.host, db_config.port))
local now = os.time()

local function run_query(query)
    if type(query) == "table" then
        query = table.concat(query)
    end
    if debug then print("RUNNING QUERY: " .. query) end
    local ok, cur, err = pcall(con.execute, con, query)
    if err or not ok then
        print(string.format("Query failed\nQUERY: %s\nERROR: %s", query, tostring(err)))
        return nil, err
    end
    local rows = {}
    local res
    repeat
        local row = {}
        res = cur:fetch(row, "a")
        if not res then break end
        table.insert(rows, row)
    until not res
    return rows
end

local function build_inner(days, inner_tmpl, joiner)
    local inner = {}
    local days_ago, table_name, inner_piece
    for i = days, 1, -1 do
        days_ago = now-(24*60*60*(i+1))
        table_name = os.date("push_endpoint_requests_%Y%m%d", days_ago)
        inner_piece = string.format(inner_tmpl, table_name)
        table.insert(inner, inner_piece)
    end
    if joiner then
        return table.concat(inner, joiner)
    end
    return inner
end

local function push_count(days)
    local inner = build_inner(days, "(SELECT count(*) FROM %s)", " + ")
    local pieces = {"SELECT ", inner, " AS push_count"}
    return run_query(pieces)
end

local function endpoint_count(days)
    local inner = build_inner(days, "(SELECT DISTINCT channel_id FROM %s)", " UNION ALL ")
    local pieces = {"SELECT count(DISTINCT channel_id) AS endpoint_count FROM (",
                    inner, ")"}
    return run_query(pieces)
end

local function endpoint_count_per_day(days)
    local inner = build_inner(days, "(SELECT count(DISTINCT channel_id) FROM %s)", " + ")
    local pieces = {"SELECT ", inner, "AS endpoint_count_per_day"}
    return run_query(pieces)
end

local function receiver_count(days)
    local inner = build_inner(days, "(SELECT DISTINCT uaid_hash FROM %s)", " UNION ALL ")
    local pieces = {"SELECT count(DISTINCT uaid_hash) AS receiver_count FROM (",
                    inner, ")"}
    return run_query(pieces)
end

local function receiver_count_per_day(days)
    local inner = build_inner(days, "(SELECT count(DISTINCT uaid_hash) FROM %s)", " + ")
    local pieces = {"SELECT ", inner, "AS receiver_count_per_day"}
    return run_query(pieces)
end

local function top_receivers(days, count)
    local inner = build_inner(days, "(SELECT uaid_hash FROM %s)", " UNION ALL ")
    local pieces = {"SELECT uaid_hash, count(1) AS count FROM (", inner,
                    ") GROUP BY uaid_hash ORDER BY count DESC LIMIT ", tostring(count)}
    return run_query(pieces)
end

local query_fns = {
    push_count = push_count,
    endpoint_count = endpoint_count,
    endpoint_count_per_day = endpoint_count_per_day,
    receiver_count = receiver_count,
    receiver_count_per_day = receiver_count_per_day
}

local query_fns_with_count = {
    top_receivers = top_receivers
}

local function format_top_count(top_count_table, days, field_name)
    local rows = {}
    local count_per_day
    for i, values in ipairs(top_count_table) do
        local row = string.format("%s   %s", values[field_name], values.count)
        count_per_day = tonumber(values.count)
        if days > 1 then
            count_per_day = math.floor(count_per_day / days)
        end
        row = string.format("%s   %d", row, count_per_day)
        table.insert(rows, row)
    end
    return table.concat(rows, "\n")
end

local function print_results(rows)
    for i, row in ipairs(rows) do
        print(string.format("--Row %d--", i))
        for k, v in pairs(row) do
            print(string.format("  %s: %s", k, v))
        end
    end
    print("--------------------------")
end

local function file_exists(name)
    local f = io.open(name, "r")
    if f == nil then
        return false
    end
    io.close(f)
    return true
end

local function main()
    local results = {}

    -- Iterate through all of the query functions, calling each one once for
    -- every days interval defined in report.cfg.
    local rows, var_name
    for name, fn in pairs(query_fns) do
        for _, days in ipairs(day_counts) do -- `day_counts` comes from report.cfg.
            var_name = string.format("%s_%d", name, days)
            rows = fn(days)
            if debug then print_results(rows) end
            results[var_name] = tonumber(rows[1][name])
            -- Divide for the per_day values.
            if string.find(name, "per_day") and days > 1 then
                results[var_name] = math.floor(results[var_name] / days)
            end
            -- Create push_count_per_day value.
            if name == "push_count" then
                local count = results[var_name]
                var_name = string.format("push_count_per_day_%d", days)
                if days > 1 then
                    count = math.floor(count / days)
                end
                results[var_name] = count
            end
        end
    end

    -- Iterate again for the functions that return a "top count" amount.
    for name, fn in pairs(query_fns_with_count) do
        for _, days in ipairs(day_counts) do
            var_name = string.format("%s_%d", name, days)
            results[var_name] = fn(days, top_count) -- `top_count` comes from report.cfg.
            if debug then print_results(results[var_name]) end
        end
    end

    -- Now further massage and add to the results table so it can be
    -- substituted directly into the email template. Start by filling in the
    -- date values.
    local days_ago
    for _, days in ipairs(day_counts) do
        if days == 1 then
            var_name = "days_ago_last"
        else
            var_name = string.format("days_ago_%d", days)
        end
        days_ago = now-(24*60*60*(days+1))
        results[var_name] = os.date("%Y/%m/%d", days_ago)
    end

    -- Add some values directly from report.cfg.
    results.top_count = top_count

    -- Create the top receiver count strings.
    for _, days in ipairs(day_counts) do
        var_name = string.format("top_receivers_%d", days)
        results[var_name] = format_top_count(results[var_name], days, "uaid_hash")
    end

    -- Define the substitution function.
    local function sub_value(value)
        value = results[value]
        if not value then
            return "nil"
        end
        return value
    end

    -- Load the email template, substitute in the results, and write it out to
    -- a temp file.
    io.input("report.txt")
    local email = io.read("*all")
    email = string.gsub(email, "%${([%a%d_]+)}", sub_value)
    local outfile_tmpl = "report-%Y%m%d.eml"
    local outfile_name = os.date(outfile_tmpl, now)
    io.output(outfile_name)
    io.write(email)

    -- Send the email.
    local sendmail_cmd = string.format("/usr/sbin/sendmail -vtF '%s' -r '%s' < '%s'", from_name, from_email, outfile_name)
    io.popen(sendmail_cmd)

    -- Delete file from 3 days ago, if it exists.
    outfile_name = os.date(outfile_tmpl, now-(24*60*60*3))
    if file_exists(outfile_name) then
        os.remove(outfile_name)
    end
end

main()
