import uuid
from django.db import models
from django.conf import settings
from connections.models import ConnectionProfile


class CommitEvent(models.Model):
    version_id = models.CharField(max_length=255, db_index=True, unique=True)
    seq = models.PositiveIntegerField(db_index=True, default=0)
    timestamp = models.DateTimeField(auto_now_add=True)
    sql_command = models.TextField()
    commit_hash = models.CharField(max_length=64, blank=True, default='')
    status = models.CharField(max_length=20)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='commit_events',
    )
    connection_profile = models.ForeignKey(
        ConnectionProfile,
        on_delete=models.CASCADE,
        related_name='commit_events',
    )

    class Meta:
        indexes = [
            models.Index(fields=['user', 'timestamp']),
            models.Index(fields=['connection_profile', 'seq']),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=['connection_profile', 'seq'],
                name='unique_seq_per_profile',
            )
        ]

    def __str__(self):
        return f"Commit {self.version_id} ({self.status})"


class InverseOperation(models.Model):
    version_id = models.CharField(max_length=255, db_index=True)
    inverse_sql = models.TextField()
    commit = models.OneToOneField(
        CommitEvent,
        on_delete=models.CASCADE,
        related_name='inverse_operation',
    )

    def __str__(self):
        return f"Inverse for {self.version_id}"


class Snapshot(models.Model):
    snapshot_id = models.UUIDField(default=uuid.uuid4, unique=True)
    version_id = models.CharField(max_length=255, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    s3_key = models.CharField(max_length=500)
    connection_profile = models.ForeignKey(
        ConnectionProfile,
        on_delete=models.CASCADE,
        related_name='snapshots',
    )

    def __str__(self):
        return f"Snapshot {self.snapshot_id} at version {self.version_id}"


class SnapshotPolicy(models.Model):
    frequency = models.IntegerField()
    last_updated = models.DateTimeField(auto_now=True)
    connection_profile = models.OneToOneField(
        ConnectionProfile,
        on_delete=models.CASCADE,
        related_name='snapshot_policy',
    )

    def __str__(self):
        return f"Policy for {self.connection_profile.name} (every {self.frequency} commits)"