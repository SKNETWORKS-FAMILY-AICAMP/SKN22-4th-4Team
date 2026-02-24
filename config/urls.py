from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path("admin/", admin.site.urls),
    path(
        "accounts/", include("django.contrib.auth.urls")
    ),  # Provides login, logout, etc.
    path("", include("finance_app.urls")),
]
