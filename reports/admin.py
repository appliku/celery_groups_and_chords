from django.contrib import admin

from reports.models import ReportGroupTaskLog, ReportGroup, ReportChordTaskLog, ReportChord


class ReportGroupTaskLogInline(admin.TabularInline):
    model = ReportGroupTaskLog
    extra = 0
    can_delete = False
    readonly_fields = ('task_name', 'status', 'created_dt_precise', 'finished_dt_precise', 'retry',)
    fields = ('task_name', 'status', 'created_dt_precise', 'finished_dt_precise', 'retry')

    def created_dt_precise(self, obj: ReportGroupTaskLog):
        return obj.created_dt.strftime("%Y-%m-%d %H:%M:%S")

    def finished_dt_precise(self, obj: ReportGroupTaskLog):
        if not obj.finished_dt:
            return "---"
        return obj.finished_dt.strftime("%Y-%m-%d %H:%M:%S")


class ReportGroupAdmin(admin.ModelAdmin):
    inlines = [ReportGroupTaskLogInline]
    list_display = ['id', 'status', 'created_dt', ]
    list_filter = ['status', ]
    readonly_fields = ('id', 'status', 'is_celery_task_sent', 'created_dt_precise',)
    fields = ('id', 'status', 'is_celery_task_sent', 'created_dt_precise',)

    def created_dt_precise(self, obj: ReportGroup):
        return obj.created_dt.strftime("%Y-%m-%d %H:%M:%S")


admin.site.register(ReportGroup, ReportGroupAdmin)


class ReportChordTaskLogInline(admin.TabularInline):
    model = ReportChordTaskLog
    extra = 0
    can_delete = False
    readonly_fields = ('task_name', 'status', 'created_dt_precise', 'finished_dt_precise', 'retry',)
    fields = ('task_name', 'status', 'created_dt_precise', 'finished_dt_precise', 'retry')

    def created_dt_precise(self, obj: ReportGroupTaskLog):
        return obj.created_dt.strftime("%Y-%m-%d %H:%M:%S")

    def finished_dt_precise(self, obj: ReportGroupTaskLog):
        if not obj.finished_dt:
            return "---"
        return obj.finished_dt.strftime("%Y-%m-%d %H:%M:%S")


class ReportChordAdmin(admin.ModelAdmin):
    inlines = [ReportChordTaskLogInline]
    list_display = ['id', 'status', 'created_dt', ]
    list_filter = ['status', ]
    readonly_fields = ('id', 'status', 'is_celery_task_sent', 'created_dt_precise',)
    fields = ('id', 'status', 'is_celery_task_sent', 'created_dt_precise',)

    def created_dt_precise(self, obj: ReportGroup):
        return obj.created_dt.strftime("%Y-%m-%d %H:%M:%S")


admin.site.register(ReportChord, ReportChordAdmin)
