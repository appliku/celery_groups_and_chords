from celery import signature, chord, group
from django.db import models, transaction

from reports.tuples import REPORT_STATUS


class ReportGroup(models.Model):
    STATUS_CHOICES = (
        (REPORT_STATUS.processing, 'Processing'),
        (REPORT_STATUS.error, 'Error'),
        (REPORT_STATUS.finished, 'Finished'),
    )
    created_dt = models.DateTimeField(auto_now_add=True)
    status = models.IntegerField(choices=STATUS_CHOICES, default=REPORT_STATUS.processing)
    is_celery_task_sent = models.BooleanField(default=False)

    def __str__(self):
        return str(self.pk)

    class Meta:
        verbose_name = "Group Report"
        verbose_name_plural = "Group Reports"
        ordering = ('-pk',)

    def do_celery(self):
        s_short = signature("report_group_short", kwargs={"report_id": self.pk})
        s_medium = signature("report_group_medium", kwargs={"report_id": self.pk})
        s_long = signature("report_group_long", kwargs={"report_id": self.pk})
        tasks = [s_short, s_medium, s_long]
        s_error = signature("report_group_error", kwargs={"report_id": self.pk}, immutable=True)
        s_finished = signature("report_group_finished", kwargs={"report_id": self.pk}, immutable=True)
        job = group(*tasks)
        job.link(s_finished)
        job.link_error(s_error)
        transaction.on_commit(lambda: job.apply_async())

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        if not self.is_celery_task_sent:
            self.is_celery_task_sent = True
            self.do_celery()
            self.save(update_fields=['is_celery_task_sent', ])


class ReportGroupTaskLog(models.Model):
    STATUS_CHOICES = (
        (REPORT_STATUS.processing, 'Processing'),
        (REPORT_STATUS.error, 'Error'),
        (REPORT_STATUS.finished, 'Finished'),
    )
    report = models.ForeignKey(ReportGroup, on_delete=models.CASCADE)
    task_name = models.CharField(max_length=255)
    status = models.IntegerField(choices=STATUS_CHOICES, default=REPORT_STATUS.processing)
    retry = models.IntegerField(default=0)
    created_dt = models.DateTimeField(auto_now_add=True)
    finished_dt = models.DateTimeField(null=True)

    def __str__(self):
        return f"{self.report_id} {self.task_name}"

    class Meta:
        verbose_name = "Group Report Task Log"
        verbose_name_plural = "Group Reports Task Log"
        ordering = ('created_dt',)
