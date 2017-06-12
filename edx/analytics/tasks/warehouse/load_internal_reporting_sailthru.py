
import datetime
import json
import logging

import luigi
# from luigi.configuration import get_config
from luigi import date_interval
from sailthru.sailthru_client import SailthruClient

from edx.analytics.tasks.util.hive import HivePartition, WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import Record, StringField, IntegerField
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join

log = logging.getLogger(__name__)


# Examples:

# DailyPullFromCybersourceTask
# class DailyProcessFromCybersourceTask(PullFromCybersourceTaskMixin, luigi.Task):
# class IntervalPullFromCybersourceTask(PullFromCybersourceTaskMixin, WarehouseMixin, luigi.WrapperTask):


# class PaypalTransactionsByDayTask(PaypalTaskMixin, luigi.Task):
# class PaypalTransactionsIntervalTask(PaypalTaskMixin, WarehouseMixin, luigi.WrapperTask):


# # This is not incremental.
# class DailyPullCatalogTask(PullCatalogMixin, luigi.Task):
# class DailyProcessFromCatalogSubjectTask(PullCatalogMixin, luigi.Task):
# class DailyLoadSubjectsToVerticaTask(PullCatalogMixin, VerticaCopyTask):
# class CourseCatalogWorkflow(PullCatalogMixin, VerticaCopyTaskMixin, luigi.WrapperTask):




class PullFromSailthruTaskMixin(OverwriteOutputMixin):
    """Define common parameters for Sailthru pull and downstream tasks."""

    api_key = luigi.Parameter(
        default_from_config={'section': 'sailthru', 'name': 'api_key'},
        significant=False,
        description='Sailthru API key.',
    )
    api_secret = luigi.Parameter(
        default_from_config={'section': 'sailthru', 'name': 'api_secret'},
        significant=False,
        description='Sailthru API secret.',
    )
    output_root = luigi.Parameter(
        description='URL of location to write output.',
    )


class DailyPullFromSailthruTask(PullFromSailthruTaskMixin, luigi.Task):
    """
    A task that reads out of a remote Sailthru account and writes to a file.

    A complication is that this needs to be performed with more than one account
    (or merchant_id), with potentially different credentials.  If possible, create
    the same credentials (username, password) for each account.

    Pulls are made for only a single day.  This is what Sailthru
    supports for these reports, and it allows runs to performed
    incrementally on a daily tempo.

    """
    # Date to fetch Sailthru report.
    run_date = luigi.DateParameter(
        default=datetime.date.today(),
        description='Default is today.',
    )

    REPORT_FORMAT = 'json'

    def requires(self):
        pass

    def run(self):
        self.remove_output_on_overwrite()
        sailthru_client = SailthruClient(self.api_key, self.api_secret)
        end_date = self.run_date + datetime.timedelta(days=1)
        request_data = {
            'status': 'sent',
            'start_date': self.run_date.isoformat(),
            'end_date': end_date.isoformat(),
        }
        response = sailthru_client.api_get('blast', request_data)

        if not response.is_ok():
            msg = "Encountered status {} on request to Sailthru for {}".format(
                response.get_status_code(), self.run_date
            )
            raise Exception(msg)

        # TODO: decide whether to insert additional information about when the record was pulled.
        with self.output().open('w') as output_file:
            output_file.write(response.get_body(as_dictionary=False))

    def output(self):
        """Output is in the form {output_root}/sailthru_raw/{CCYY-mm}/sailthru_blast_{CCYYmmdd}.json"""
        month_year_string = self.run_date.strftime('%Y-%m')  # pylint: disable=no-member
        date_string = self.run_date.strftime('%Y%m%d')  # pylint: disable=no-member
        filename = "sailthru_{type}_{date_string}.{report_format}".format(
            type='blast',
            date_string=date_string,
            report_format=self.REPORT_FORMAT,
        )
        url_with_filename = url_path_join(self.output_root, "sailthru_raw", month_year_string, filename)
        return get_target_from_url(url_with_filename)


class SailthruBlastStatsRecord(Record):
    blast_id = IntegerField(nullable=False, description='Blast identifier.')
    email_subject = StringField(length=564, nullable=False, description='Blast identifier.')
    email_list = StringField(length=564, nullable=False, description='Blast identifier.')
    email_campaign_name = StringField(length=564, nullable=False, description='Blast identifier.')
    email_abtest_name = StringField(length=564, nullable=True, description='Blast identifier.')
    email_abtest_segment = StringField(length=564, nullable=True, description='Blast identifier.')
    email_start_time = StringField(length=564, nullable=False, description='Blast identifier.')
    email_sent_cnt = IntegerField(nullable=False, description='Blast identifier.')
    email_unsubscribe_cnt = IntegerField(nullable=False, description='Blast identifier.')
    email_open_cnt = IntegerField(nullable=False, description='Blast identifier.')
    email_click_cnt = IntegerField(nullable=False, description='Blast identifier.')


class DailyProcessFromSailthruTask(PullFromSailthruTaskMixin, luigi.Task):
    """
    A task that reads a local file generated from a daily Sailthru pull, and writes to a TSV file.

    The output file should be readable by Hive, and be in a common format across
    other payment accounts.

    """
    run_date = luigi.DateParameter(
        default=datetime.date.today(),
        description='Date to fetch Sailthru report. Default is today.',
    )
    output_root = luigi.Parameter(
        description='URL of location to write output.',
    )

    def requires(self):
        args = {
            'api_key': self.api_key,
            'api_secret': self.api_secret,
            'run_date': self.run_date,
            'output_root': self.output_root,
            'overwrite': self.overwrite,
        }
        return DailyPullFromSailthruTask(**args)

    def run(self):
        # Read from input and reformat for output.
        self.remove_output_on_overwrite()
        with self.input().open('r') as input_file:
            info = json.loads(input_file.read())
            output_lines = self.get_output_from_info(info)
            with self.output().open('w') as output_file:
                for output_line in output_lines:
                    output_file.write(output_line)
                    output_file.write('\n')

    def get_output_from_info(self, info):
        output_lines = []
        blasts = info.get('blasts')
        for blast in blasts:
            output_entry = {}

            output_entry['blast_id'] = blast.get('blast_id')  # or 'final_blast_id'?  Looks like copy_blast_id is different.
            output_entry['email_subject'] = blast.get('subject')
            output_entry['email_list'] = blast.get('list')
            output_entry['email_campaign_name'] = blast.get('name')
            output_entry['email_abtest_name'] = blast.get('abtest')
            output_entry['email_abtest_segment'] = blast.get('abtest_segment')
            output_entry['email_start_time'] = blast.get('start_time')

            stats = blast.get('stats', {}).get('total', {})
            # ISSUE: don't these change over time?  And if so, do we need separate entries for them, by date when
            # they were fetched?
            output_entry['email_sent_cnt'] = stats.get('count', 0)
            output_entry['email_unsubscribe_cnt'] = stats.get('optout', 0)
            output_entry['email_open_cnt'] = stats.get('open_total', 0)
            output_entry['email_click_cnt'] = stats.get('click_total', 0)

            record = SailthruBlastStatsRecord(**output_entry)

            output_lines.append(record.to_separated_values())

        return output_lines

    def output(self):
        """
        Output is set up so it can be read in as a Hive table with partitions.

        The form is {output_root}/sailthru_blast_stats/dt={CCYY-mm-dd}/sailthru_blast.tsv
        """
        date_string = self.run_date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        partition_path_spec = HivePartition('dt', date_string).path_spec
        filename = "sailthru_blast.tsv"
        url_with_filename = url_path_join(self.output_root, "sailthru_blast_stats", partition_path_spec, filename)
        return get_target_from_url(url_with_filename)


class IntervalPullFromSailthruTask(PullFromSailthruTaskMixin, WarehouseMixin, luigi.WrapperTask):
    """Determines a set of dates to pull, and requires them."""

    date = None
    interval = luigi.DateIntervalParameter(default=None)
    interval_start = luigi.DateParameter(
        default_from_config={'section': 'sailthru', 'name': 'interval_start'},
        significant=False,
    )
    interval_end = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        significant=False,
        description='Default is today, UTC.',
    )

    # Overwrite parameter definition to make it optional.
    output_root = luigi.Parameter(
        default=None,
        description='URL of location to write output.',
    )

    def __init__(self, *args, **kwargs):
        super(IntervalPullFromSailthruTask, self).__init__(*args, **kwargs)
        # Provide default for output_root at this level.
        if self.output_root is None:
            self.output_root = self.warehouse_path
        if self.interval is None:
            self.interval = date_interval.Custom(self.interval_start, self.interval_end)

    def requires(self):
        """Internal method to actually calculate required tasks once."""
        args = {
            'api_key': self.api_key,
            'api_secret': self.api_secret,
            'output_root': self.warehouse_path,
            'overwrite': self.overwrite,
        }

        for run_date in self.interval:
            args['run_date'] = run_date
            yield DailyProcessFromSailthruTask(**args)

    def output(self):
        return [task.output() for task in self.requires()]
