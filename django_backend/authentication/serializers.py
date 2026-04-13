from rest_framework_simplejwt.serializers import TokenObtainPairSerializer, TokenRefreshSerializer
from rest_framework_simplejwt.tokens import RefreshToken, AccessToken
from django.contrib.auth import get_user_model


class CustomTokenObtainPairSerializer(TokenObtainPairSerializer):
    @classmethod
    def get_token(cls, user):
        token = super().get_token(user)
        token['username'] = user.username
        return token


class CustomTokenRefreshSerializer(TokenRefreshSerializer):
    """Inject ``username`` into the refreshed access token so FastAPI
    can identify the caller without a DB lookup."""

    def validate(self, attrs):
        data = super().validate(attrs)
        refresh = RefreshToken(attrs["refresh"])
        User = get_user_model()
        user = User.objects.get(id=refresh["user_id"])
        access = AccessToken(data["access"])
        access["username"] = user.username
        data["access"] = str(access)
        return data