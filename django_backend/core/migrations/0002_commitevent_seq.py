from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='commitevent',
            name='seq',
            field=models.PositiveIntegerField(db_index=True, default=0),
        ),
        migrations.AddIndex(
            model_name='commitevent',
            index=models.Index(fields=['connection_profile', 'seq'], name='core_commit_connprofile_seq_idx'),
        ),
        migrations.AddConstraint(
            model_name='commitevent',
            constraint=models.UniqueConstraint(fields=['connection_profile', 'seq'], name='unique_seq_per_profile'),
        ),
    ]
