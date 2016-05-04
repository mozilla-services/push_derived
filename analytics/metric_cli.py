from __future__ import print_function

import click

from analytics.db import Database


pass_database = click.make_pass_decorator(Database)


def db_string_option(f):
    def callback(ctx, param, value):
        db = ctx.ensure_object(Database)
        db.setup(value)
        return value
    return click.argument('db_string', expose_value=False,
                          envvar="ROLLUP_DB_STRING",
                          callback=callback)(f)


def common_options(f):
    f = db_string_option(f)
    return f


class MetricCLI(object):

    @click.group()
    @click.pass_context
    def cli(ctx):
        ctx.obj = Database()

    @cli.command()
    @common_options
    @click.argument("table_prefix", envvar="ROLLUP_TABLE_PREFIX")
    @click.argument("days_ago", envvar="ROLLUP_DAYS_AGO", type=int)
    @pass_database
    def daily_rollups(db, table_prefix, days_ago):
        db.create_daily_rollups(table_prefix, days_ago)

    @cli.command()
    @common_options
    @click.argument("table_prefix", envvar="ROLLUP_TABLE_PREFIX")
    @click.argument("days_ago", envvar="ROLLUP_DAYS_AGO", type=int)
    @pass_database
    def monthly_rollups(db, table_prefix, days_ago):
        db.create_monthly_rollups(table_prefix, days_ago)

    @cli.command()
    @common_options
    @click.argument("table_prefix", envvar="ROLLUP_TABLE_PREFIX")
    @click.argument("days_ago", envvar="ROLLUP_DAYS_AGO", type=int)
    @pass_database
    def process_logs(db, table_prefix, days_ago):
        db.create_daily_rollups(table_prefix, days_ago)
        db.create_monthly_rollups(table_prefix, days_ago)
