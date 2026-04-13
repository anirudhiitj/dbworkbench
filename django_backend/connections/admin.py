from django.contrib import admin
from .models import ConnectionProfile


@admin.register(ConnectionProfile)
class ConnectionProfileAdmin(admin.ModelAdmin):
    list_display = ('name', 'host', 'database_name', 'user', 'created_at')
    list_filter = ('user',)
    search_fields = ('host', 'database_name')
    def get_readonly_fields(self, request, obj=None):
        # On edit: db_password is read-only (encrypted value must not be overwritten directly)
        # On create: db_password is editable so the password can be set
        if obj is not None:
            return ('db_password',)
        return ()