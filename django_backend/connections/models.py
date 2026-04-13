import os
from django.db import models
from django.conf import settings
from cryptography.fernet import Fernet


class ConnectionProfile(models.Model):
    name = models.CharField(max_length=255)
    host = models.CharField(max_length=255)
    port = models.IntegerField(default=5432)
    database_name = models.CharField(max_length=255)
    db_username = models.CharField(max_length=255)
    db_password = models.CharField(max_length=500)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='connection_profiles',
    )
    created_at = models.DateTimeField(auto_now_add=True)

    def save(self, *args, **kwargs):
        if not self.db_password.startswith('gAAAAA'):
            f = Fernet(os.environ['FERNET_KEY'].encode())
            self.db_password = f.encrypt(self.db_password.encode()).decode()
        super().save(*args, **kwargs)

    def get_decrypted_password(self):
        f = Fernet(os.environ['FERNET_KEY'].encode())
        return f.decrypt(self.db_password.encode()).decode()

    def __str__(self):
        return f"{self.name} ({self.host}/{self.database_name})"