from django.db import models
from django.contrib.auth.models import User


class Watchlist(models.Model):
    """사용자별 관심 기업(티커) 관리 모델"""

    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="watchlist")
    ticker = models.CharField(max_length=10)
    added_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("user", "ticker")
        ordering = ["-added_at"]

    def __str__(self):
        return f"{self.user.username} - {self.ticker}"
