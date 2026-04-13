from django.contrib.auth import get_user_model
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView

from .serializers import CustomTokenObtainPairSerializer, CustomTokenRefreshSerializer

User = get_user_model()


class CustomTokenObtainPairView(TokenObtainPairView):
    serializer_class = CustomTokenObtainPairSerializer


class CustomTokenRefreshView(TokenRefreshView):
    serializer_class = CustomTokenRefreshSerializer


@api_view(["POST"])
@permission_classes([AllowAny])
def register(request):
    """Create a new user account and return the user id and username."""
    username = request.data.get("username", "").strip()
    password = request.data.get("password", "")
    email = request.data.get("email", "").strip()

    if not username or not password:
        return Response(
            {"detail": "username and password are required"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    if User.objects.filter(username=username).exists():
        return Response(
            {"detail": "Username already taken"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    user = User.objects.create_user(username=username, password=password, email=email)
    return Response(
        {"id": user.id, "username": user.username},
        status=status.HTTP_201_CREATED,
    )
