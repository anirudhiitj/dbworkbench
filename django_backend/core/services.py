import hashlib

from django.db import transaction
from django.db.models import Max
from .models import CommitEvent, InverseOperation, Snapshot, SnapshotPolicy


def record_commit(version_id, sql_command, inverse_sql, user, connection_profile, status):
    with transaction.atomic():
        # Compute the next seq scoped to this connection profile.
        # select_for_update() on the aggregate is not possible, but the
        # UniqueConstraint on (connection_profile, seq) acts as the safety
        # net — a duplicate seq will raise IntegrityError and roll back.
        last_seq = CommitEvent.objects.filter(
            connection_profile=connection_profile,
        ).aggregate(Max('seq'))['seq__max']
        next_seq = (last_seq or 0) + 1

        # Hash chain: SHA-256(prev_hash : sql_command : version_id)
        prev_commit = CommitEvent.objects.filter(
            connection_profile=connection_profile,
            seq=next_seq - 1,
        ).first()
        prev_hash = prev_commit.commit_hash if prev_commit else ""
        commit_hash = hashlib.sha256(
            f"{prev_hash}:{sql_command}:{version_id}".encode()
        ).hexdigest()

        commit = CommitEvent.objects.create(
            version_id=version_id,
            seq=next_seq,
            sql_command=sql_command,
            commit_hash=commit_hash,
            status=status,
            user=user,
            connection_profile=connection_profile,
        )

        InverseOperation.objects.create(
            version_id=version_id,
            inverse_sql=inverse_sql,
            commit=commit,
        )

        policy, _ = SnapshotPolicy.objects.get_or_create(
            connection_profile=connection_profile,
            defaults={"frequency": 5},
        )

        last_snapshot = Snapshot.objects.filter(
            connection_profile=connection_profile,
        ).order_by('-created_at').first()

        if last_snapshot:
            commits_since = CommitEvent.objects.filter(
                connection_profile=connection_profile,
                timestamp__gt=last_snapshot.created_at,
            ).count()
        else:
            commits_since = CommitEvent.objects.filter(
                connection_profile=connection_profile,
            ).count()

        if commits_since >= policy.frequency:
            Snapshot.objects.create(
                version_id=version_id,
                s3_key=f"snapshots/{connection_profile.id}/{version_id}",
                connection_profile=connection_profile,
            )

        return commit