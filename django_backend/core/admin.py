from django.contrib import admin
from .models import CommitEvent, InverseOperation, Snapshot, SnapshotPolicy


@admin.register(CommitEvent)
class CommitEventAdmin(admin.ModelAdmin):
    list_display = ('version_id', 'seq', 'user', 'timestamp', 'status', 'connection_profile')
    list_filter = ('user', 'status', 'timestamp')
    search_fields = ('sql_command', 'version_id')


@admin.register(InverseOperation)
class InverseOperationAdmin(admin.ModelAdmin):
    list_display = ('version_id', 'commit')
    search_fields = ('version_id',)


@admin.register(Snapshot)
class SnapshotAdmin(admin.ModelAdmin):
    list_display = ('snapshot_id', 'version_id', 'created_at', 'connection_profile')
    list_filter = ('connection_profile',)
    search_fields = ('version_id',)


@admin.register(SnapshotPolicy)
class SnapshotPolicyAdmin(admin.ModelAdmin):
    list_display = ('connection_profile', 'frequency', 'last_updated')