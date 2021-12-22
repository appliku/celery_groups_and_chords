import random
import time
import arrow
from celery import shared_task

from reports.models import ReportGroup, ReportChord
from reports.tuples import REPORT_STATUS


@shared_task(
    name='report_group_short',
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=500,
    retry_jitter=True,
)
def report_group_short(self, report_id):
    try:
        report = ReportGroup.objects.get(pk=report_id)
    except ReportGroup.DoesNotExist:
        return False
    log = report.reportgrouptasklog_set.create(
        task_name='report_group_short',
        status=REPORT_STATUS.processing,
        retry=self.request.retries
    )
    complexity = random.randint(1, 10)
    time.sleep(complexity)
    try:
        if random.random() < 0.1:
            raise ValueError("API Connectivity Failure")
    except Exception as e:
        log.finished_dt = arrow.now().datetime
        log.status = REPORT_STATUS.error
        log.save(update_fields=['status', 'finished_dt', ])
        raise e
    log.finished_dt = arrow.now().datetime
    log.status = REPORT_STATUS.finished
    log.save(update_fields=['status', 'finished_dt', ])
    return complexity


@shared_task(
    name='report_group_medium',
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=500,
    retry_jitter=True,
)
def report_group_medium(self, report_id):
    try:
        report = ReportGroup.objects.get(pk=report_id)
    except ReportGroup.DoesNotExist:
        return False
    log = report.reportgrouptasklog_set.create(
        task_name='report_group_medium',
        status=REPORT_STATUS.processing,
        retry=self.request.retries
    )
    complexity = random.randint(10, 30)
    time.sleep(complexity)
    try:
        if random.random() < 0.1:
            raise ValueError("API Connectivity Failure")
    except Exception as e:
        log.finished_dt = arrow.now().datetime
        log.status = REPORT_STATUS.error
        log.save(update_fields=['status', 'finished_dt', ])
        raise e
    log.status = REPORT_STATUS.finished
    log.finished_dt = arrow.now().datetime
    log.save(update_fields=['status', 'finished_dt', ])
    return complexity


@shared_task(
    name='report_group_long',
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=500,
    retry_jitter=True,
)
def report_group_long(self, report_id):
    try:
        report = ReportGroup.objects.get(pk=report_id)
    except ReportGroup.DoesNotExist:
        return False
    log = report.reportgrouptasklog_set.create(
        task_name='report_group_medium',
        status=REPORT_STATUS.processing,
        retry=self.request.retries
    )
    complexity = random.randint(10, 100)
    time.sleep(complexity)
    try:
        if random.random() < 0.1:
            raise ValueError("API Connectivity Failure")
        if report.id % 5 == 0:
            raise ValueError("Unrecoverable API error")
    except Exception as e:
        log.finished_dt = arrow.now().datetime
        log.status = REPORT_STATUS.error
        log.save(update_fields=['status', 'finished_dt', ])
        raise e
    log.status = REPORT_STATUS.finished
    log.finished_dt = arrow.now().datetime
    log.save(update_fields=['status', 'finished_dt', ])
    return complexity


@shared_task(
    name="report_group_finished",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=500,
    retry_jitter=True,
)
def report_group_finished(self, report_id):
    try:
        report = ReportGroup.objects.get(pk=report_id)
    except ReportGroup.DoesNotExist:
        return False
    log = report.reportgrouptasklog_set.create(
        task_name='report_group_finished',
        status=REPORT_STATUS.finished,
        retry=self.request.retries
    )
    report.status = REPORT_STATUS.finished
    report.save(update_fields=['status', ])
    log.finished_dt = arrow.now().datetime
    log.save(update_fields=['status', 'finished_dt', ])
    return True


@shared_task(
    name="report_group_error",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=500,
    retry_jitter=True,
)
def report_group_error(self, report_id):
    try:
        report = ReportGroup.objects.get(pk=report_id)
    except ReportGroup.DoesNotExist:
        return False
    log = report.reportgrouptasklog_set.create(
        task_name='report_group_error',
        status=REPORT_STATUS.finished,
        retry=self.request.retries
    )
    report.status = REPORT_STATUS.error
    report.save(update_fields=['status', ])
    log.finished_dt = arrow.now().datetime
    log.save(update_fields=['status', 'finished_dt', ])
    return True


@shared_task(
    name='report_chord_short',
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=500,
    retry_jitter=True,
)
def report_chord_short(self, report_id):
    try:
        report = ReportChord.objects.get(pk=report_id)
    except ReportChord.DoesNotExist:
        return False
    log = report.reportchordtasklog_set.create(
        task_name='report_chord_short',
        status=REPORT_STATUS.processing,
        retry=self.request.retries
    )
    complexity = random.randint(1, 10)
    time.sleep(complexity)
    try:
        if random.random() < 0.1:
            raise ValueError("API Connectivity Failure")
    except Exception as e:
        log.finished_dt = arrow.now().datetime
        log.status = REPORT_STATUS.error
        log.save(update_fields=['status', 'finished_dt', ])
        raise e
    log.finished_dt = arrow.now().datetime
    log.status = REPORT_STATUS.finished
    log.save(update_fields=['status', 'finished_dt', ])
    return complexity


@shared_task(
    name='report_chord_medium',
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=500,
    retry_jitter=True,
)
def report_chord_medium(self, report_id):
    try:
        report = ReportChord.objects.get(pk=report_id)
    except ReportChord.DoesNotExist:
        return False
    log = report.reportchordtasklog_set.create(
        task_name='report_chord_medium',
        status=REPORT_STATUS.processing,
        retry=self.request.retries
    )
    complexity = random.randint(10, 30)
    time.sleep(complexity)
    try:
        if random.random() < 0.1:
            raise ValueError("API Connectivity Failure")
    except Exception as e:
        log.finished_dt = arrow.now().datetime
        log.status = REPORT_STATUS.error
        log.save(update_fields=['status', 'finished_dt', ])
        raise e
    log.status = REPORT_STATUS.finished
    log.finished_dt = arrow.now().datetime
    log.save(update_fields=['status', 'finished_dt', ])
    return complexity


@shared_task(
    name='report_chord_long',
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=500,
    retry_jitter=True,
)
def report_chord_long(self, report_id):
    try:
        report = ReportChord.objects.get(pk=report_id)
    except ReportChord.DoesNotExist:
        return False
    log = report.reportchordtasklog_set.create(
        task_name='report_chord_long',
        status=REPORT_STATUS.processing,
        retry=self.request.retries
    )
    complexity = random.randint(10, 100)
    time.sleep(complexity)
    try:
        if random.random() < 0.1:
            raise ValueError("API Connectivity Failure")
        if report.id % 5 == 0:
            raise ValueError("Unrecoverable API error")
    except Exception as e:
        log.finished_dt = arrow.now().datetime
        log.status = REPORT_STATUS.error
        log.save(update_fields=['status', 'finished_dt', ])
        raise e

    log.status = REPORT_STATUS.finished
    log.finished_dt = arrow.now().datetime
    log.save(update_fields=['status', 'finished_dt', ])
    return complexity


@shared_task(
    name="report_chord_finished",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=500,
    retry_jitter=True,
)
def report_chord_finished(self, report_id):
    try:
        report = ReportChord.objects.get(pk=report_id)
    except ReportChord.DoesNotExist:
        return False
    log = report.reportchordtasklog_set.create(
        task_name='report_chord_finished',
        status=REPORT_STATUS.finished,
        retry=self.request.retries
    )
    report.status = REPORT_STATUS.finished
    report.save(update_fields=['status', ])
    log.finished_dt = arrow.now().datetime
    log.save(update_fields=['status', 'finished_dt', ])
    return True


@shared_task(
    name="report_chord_error",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=5,
    retry_backoff=True,
    retry_backoff_max=500,
    retry_jitter=True,
)
def report_chord_error(self, report_id):
    try:
        report = ReportChord.objects.get(pk=report_id)
    except ReportChord.DoesNotExist:
        return False
    log = report.reportchordtasklog_set.create(
        task_name='report_chord_error',
        status=REPORT_STATUS.finished,
        retry=self.request.retries
    )
    report.status = REPORT_STATUS.error
    report.save(update_fields=['status', ])
    log.finished_dt = arrow.now().datetime
    log.save(update_fields=['status', 'finished_dt', ])
    return True
