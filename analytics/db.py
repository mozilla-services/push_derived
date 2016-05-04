from __future__ import print_function

import datetime
from decimal import Decimal

import click

from sqlalchemy import (
    bindparam,
    create_engine,
    text,
    Date,
    Numeric,
    MetaData,
    Table,
    Column,
    Integer,
    String,
)
from sqlalchemy.sql import (
    cast,
    func,
    select,
    union_all,
)


meta = MetaData()


# Daily Unique Rollups
daily_rollup = Table(
    "daily_rollup", meta,
    Column("date", Date),
    Column("count", Integer),
    Column("browser_os", String(length=50)),
    Column("browser_version", String(length=5)),
)

# Daily Statistics
# Note: these avg are avg notifications per user, not users
daily_stats = Table(
    "daily_stats", meta,
    Column("date", Date),
    Column("daily_avg", Numeric(10, 3)),
    Column("weekly_avg", Numeric(10, 3)),
    Column("dau", Numeric),
    Column("mau", Numeric),
    Column("engagement", Numeric(5, 3)),
)


# Monthly Unique Rollups
monthly_rollup = Table(
    "monthly_rollup", meta,
    Column("date", Date),
    Column("count", Integer),
    Column("browser_os", String(length=50)),
    Column("browser_version", String(length=5)),
)


class Database(object):
    def __init__(self):
        self._engine = None
        self._conn = None

    def setup(self, db_string):
        self._engine = create_engine(db_string, isolation_level="AUTOCOMMIT")
        self._conn = self._engine.connect()
        meta.create_all(bind=self._engine)
        click.echo("Reflecting tables, this might take a minute...", nl=False)
        meta.reflect(bind=self._engine)
        click.secho("Done.", fg="green")

    def _get_filtered_tables(self, table_prefix, days_ago):
        # Note we skip the latest day since its data is generally not
        # complete
        return filter(lambda x: x.name.startswith(table_prefix),
                      meta.sorted_tables)[-days_ago:-1]

    def _generate_daily_uniques(self, table):
        s = select([
            func.count(func.distinct(table.c.uaid_hash)).
            label("unique_count"),
            cast(table.c.timestamp, Date).label("date"),
            table.c.browser_os,
            table.c.browser_version,
        ]).\
            where(table.c.message == "Ack").\
            group_by("date", table.c.browser_os,
                     table.c.browser_version)

        results = self._conn.execute(s).fetchall()
        if not results:
            return

        # Now build the insert for the rollup table, and insert
        self._conn.execute(daily_rollup.insert(), [
            dict(count=x.unique_count,
                 date=x.date,
                 browser_os=x.browser_os,
                 browser_version=x.browser_version)
            for x in results
        ])

    def _query_daily_average(self, table):
        per_user_counts = select([
            table.c.uaid_hash,
            func.count(table.c.uaid_hash).label("msg_count"),
        ]).\
            where(table.c.message == "Ack").\
            group_by(table.c.uaid_hash)

        ranked_counts = select([
            per_user_counts.c.uaid_hash,
            per_user_counts.c.msg_count,
            func.ntile(100).over(order_by=text("msg_count ASC")).label("rank"),
        ])

        daily_avg = select([
            func.sum(ranked_counts.c.msg_count),
            func.count(ranked_counts.c.uaid_hash),
        ]).\
            where(ranked_counts.c.rank > 5).\
            where(ranked_counts.c.rank <= 95)
        sums, count = self._conn.execute(daily_avg).fetchone()
        daily_avg = Decimal(sums) / Decimal(count)
        return daily_avg

    def _query_weekly_average(self, table, tables):
        # First see if we can find 6 days prior for a full week
        idx = tables.index(table)

        # For Python list math, 6 has 6 numbers before it as zero index
        # based, so 6 or larger is needed
        if idx < 6:
            return None

        # Get our weekly set together
        # Note that we add one to idx since list splicing needs one higher than
        # the index for right-side inclusive
        week_tables = union_all(*[
            select([tbl]).where(tbl.c.message == "Ack")
            for tbl in tables[idx-6:idx+1]
        ])

        # Calculate channels per user for the past week
        chans_per_user = select([
            week_tables.c.uaid_hash,
            func.count(func.distinct(week_tables.c.channel_id)).label("count")
        ]).\
            group_by(week_tables.c.uaid_hash)

        # Rank them into ntiles
        ranked = select([
            chans_per_user.c.uaid_hash,
            chans_per_user.c.count,
            func.ntile(100).over(order_by=text("count ASC")).label("rank"),
        ])

        # Remove the bottom/upper 5%, get sum/count for avg
        weekly_channels_stats = select([
            func.sum(ranked.c.count),
            func.count(ranked.c.uaid_hash),
        ]).\
            where(ranked.c.rank > 5).\
            where(ranked.c.rank <= 95)
        sums, count = self._conn.execute(weekly_channels_stats).fetchone()
        weekly_avg = Decimal(sums) / Decimal(count)
        return weekly_avg

    def _update_missing_daily_stats(self, dailies):
        # Query existing stats to find latest value
        last_date = select([daily_stats]).order_by(daily_stats.c.date.desc())
        last = self._conn.execute(last_date).fetchone()

        # Drop all stats older than our latest
        if last:
            update_stats = filter(lambda x: x["date"] > last.date, dailies)
        else:
            update_stats = dailies

        # Update the remaining stats
        self._conn.execute(daily_stats.insert(), [
            dict(date=x["date"],
                 daily_avg=x["daily_avg"],
                 weekly_avg=x["weekly_avg"])
            for x in update_stats
        ])

    def _generate_daily_stats(self, table, tables):
        return dict(daily_avg=self._query_daily_average(table),
                    weekly_avg=self._query_weekly_average(table, tables))

    def _add_missing_dau(self, table_prefix, days_ago):
        # Locate beginning table
        tables = self._get_filtered_tables(table_prefix, days_ago+6)
        first_date = self._date_from_tablename(tables[0].name)

        # Group all the daily unique counts by date
        grouped_daily_unique = select([
            daily_rollup.c.date,
            func.sum(daily_rollup.c.count).label("count")
        ]).\
            where(daily_rollup.c.date >= first_date).\
            group_by(daily_rollup.c.date)

        # Average them over 6 prior days including today for DAU's
        daus = select([
            grouped_daily_unique.c.date,
            func.avg(grouped_daily_unique.c.count).over(
                order_by=text("date ROWS 6 PRECEDING")
            ).label("dau")
        ])
        results = self._conn.execute(daus).fetchall()

        # Update appropriate rows
        stmt = daily_stats.update().\
            where(daily_stats.c.date == bindparam("update_date")).\
            values(dau=bindparam("dau"))
        self._conn.execute(stmt, [
            {"update_date": x.date, "dau": x.dau}
            for x in results
        ])

    def _add_missing_mau(self, table_prefix, days_ago):
        # Locate beginning table
        tables = self._get_filtered_tables(table_prefix, days_ago+30)
        first_date = self._date_from_tablename(tables[0].name)

        grouped_monthly_unique = select([
            monthly_rollup.c.date,
            func.sum(monthly_rollup.c.count).label("count")
        ]).\
            where(monthly_rollup.c.date >= first_date).\
            group_by(monthly_rollup.c.date)

        # Average them over 6 days prior inclding today for MAU's
        maus = select([
            grouped_monthly_unique.c.date,
            func.avg(grouped_monthly_unique.c.count).over(
                order_by=text("date ROWS 6 PRECEDING")
            ).label("mau")
        ])
        results = self._conn.execute(maus).fetchall()

        # Update appropriate rows
        stmt = daily_stats.update().\
            where(daily_stats.c.date == bindparam("update_date")).\
            values(mau=bindparam("mau"))
        self._conn.execute(stmt, [
            {"update_date": x.date, "mau": x.mau}
            for x in results
        ])

    def _add_missing_engagement(self, tables):
        # Locate beginning table
        first_date = self._date_from_tablename(tables[0].name)

        eng = select([
            daily_stats.c.date,
            (daily_stats.c.dau / daily_stats.c.mau).label("engagement")
        ]).\
            where(daily_stats.c.date >= first_date)
        results = self._conn.execute(eng)

        # Update engagement
        stmt = daily_stats.update().\
            where(daily_stats.c.date == bindparam("update_date")).\
            values(engagement=bindparam("engagement"))
        self._conn.execute(stmt, [
            {"update_date": x.date, "engagement": x.engagement}
            for x in results
        ])

    def _date_from_tablename(self, tablename):
        year, month, day = [tablename[-8:-4], tablename[-4:-2], tablename[-2:]]
        return datetime.date(year=int(year), month=int(month), day=int(day))

    def _generate_tablename(self, table_prefix, date):
        return table_prefix + "_" + "".join(
            map(str, [date.year, date.month, date.day])
        )

    def _generate_monthly_uniques(self, table, tables):
        idx = tables.index(table)

        # Join them all
        at = union_all(*[
            select([tbl]).where(tbl.c.message == "Ack")
            for tbl in tables[idx-29:idx+1]
        ])

        # Get uniques
        s = select([
            func.count(func.distinct(at.c.uaid_hash)).
            label("unique_count"),
            at.c.browser_os,
            at.c.browser_version,
        ]).\
            group_by(at.c.browser_os, at.c.browser_version)
        results = self._conn.execute(s).fetchall()
        if not results:
            return

        # Determine the date for this entry
        tname = table.name
        date_parts = [tname[-8:-4], tname[-4:-2], tname[-2:]]
        insert_date = "-".join(date_parts)

        self._conn.execute(monthly_rollup.insert(), [
            dict(date=insert_date,
                 count=x.unique_count,
                 browser_os=x.browser_os,
                 browser_version=x.browser_version)
            for x in results
        ])

    def create_daily_rollups(self, table_prefix, days_ago):
        # Locate all the tables needed for these queries
        tables = self._get_filtered_tables(table_prefix, days_ago)

        # Grab the oldest daily_rollup date
        oldest_date = select([daily_rollup]).\
            order_by(daily_rollup.c.date.desc())
        oldest = self._conn.execute(oldest_date).fetchone()
        if oldest:
            oldest = self._generate_tablename(table_prefix, oldest.date)

        # Generate the rollups of daily uniques
        lbl = "Processing {} days for daily unique rollups...".format(
            len(tables))
        with click.progressbar(tables, label=lbl) as all_tables:
            for table in all_tables:
                # Skip processed tables
                if oldest and table.name <= oldest:
                    continue
                self._generate_daily_uniques(table)

        # Query existing stats to find latest value
        last_date = select([daily_stats.c.date]).\
            order_by(daily_stats.c.date.desc()).limit(1)
        last = self._conn.execute(last_date).fetchone()
        if last:
            last = self._generate_tablename(table_prefix, last.date)

        # Generate the daily/weekly avg stats
        lbl = "Processing {} days for daily avg stats...".format(len(tables))
        dailies = []
        with click.progressbar(tables, label=lbl) as all_tables:
            for table in all_tables:
                if last and table.name <= last:
                    continue
                stats = self._generate_daily_stats(table, tables)
                stats["date"] = self._date_from_tablename(table.name)
                dailies.append(stats)

        # Populate any missing daily stats
        print("Processing missing daily stats")
        self._update_missing_daily_stats(dailies)

        # Add missing DAU's
        print("Processing missing DAU's")
        self._add_missing_dau(table_prefix, days_ago)

    def create_monthly_rollups(self, table_prefix, days_ago):
        tables = self._get_filtered_tables(table_prefix, days_ago+30)

        if len(tables) < 30:
            return

        # Query existing to find oldest already processed
        oldest_date = select([monthly_rollup]).\
            order_by(monthly_rollup.c.date.desc()).\
            limit(1)
        oldest = self._conn.execute(oldest_date).fetchone()
        if oldest:
            oldest = self._generate_tablename(table_prefix, oldest.date)

        lbl = "Processing {} days of monthly logs...".format(len(tables)-30)
        with click.progressbar(tables[30:], label=lbl) as all_tables:
            for table in all_tables:
                if oldest and table.name <= oldest:
                    continue
                self._generate_monthly_uniques(table, tables)

        print("Processing missing MAU's")
        self._add_missing_mau(table_prefix, days_ago)

        print("Processing missing engagement")
        self._add_missing_engagement(tables)
