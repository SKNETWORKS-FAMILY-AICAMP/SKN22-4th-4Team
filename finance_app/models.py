from django.db import models
from django.contrib.auth.models import User


class Watchlist(models.Model):
    """사용자별 관심 기업(티커) 관리 모델"""

    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="watchlist")
    ticker = models.CharField(max_length=10)
    added_at = models.DateTimeField(auto_now_add=True)
    alert_threshold_percent = models.FloatField(
        default=5.0, help_text="상하락 알림 임계값(%)"
    )

    class Meta:
        unique_together = ("user", "ticker")
        ordering = ["-added_at"]

    def __str__(self):
        return f"{self.user.username} - {self.ticker}"


class Notification(models.Model):
    """사용자에게 발송되는 인앱 알림 내역 모델"""

    user = models.ForeignKey(
        User, on_delete=models.CASCADE, related_name="notifications"
    )
    ticker = models.CharField(
        max_length=10, blank=True, null=True, help_text="관련 티커"
    )
    title = models.CharField(max_length=200, help_text="알림 제목")
    message = models.TextField(help_text="알림 상세 내용")
    notification_type = models.CharField(
        max_length=50,
        default="price_alert",
        help_text="알림 유형 (예: price_alert, news)",
    )
    is_read = models.BooleanField(default=False, help_text="읽음 여부")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        read_status = "Read" if self.is_read else "Unread"
        return f"[{read_status}] {self.user.username} - {self.title}"
