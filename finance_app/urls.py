from django.urls import path
from . import views
from . import report_views

app_name = "finance_app"

urlpatterns = [
    path("", views.home, name="home"),
    path("signup/", views.SignUpView.as_view(), name="signup"),
    path("profile/", views.profile_view, name="profile"),
    path("chat/", views.chat, name="chat"),
    path("api/chat/", views.chat_api, name="chat_api"),  # Chatbot API
    path("calendar/", views.calendar_view, name="calendar"),
    path(
        "api/calendar/", views.calendar_api, name="calendar_api"
    ),  # Calendar details API
    path("report/", report_views.report_view, name="report"),
    path(
        "api/report/search_tickers/",
        report_views.search_tickers_api,
        name="search_tickers_api",
    ),
    path(
        "api/report/generate/",
        report_views.generate_report_api,
        name="generate_report_api",
    ),
    path(
        "report/download/pdf/",
        report_views.download_report_pdf,
        name="download_report_pdf",
    ),
    # Watchlist API
    path("api/watchlist/add/", views.watchlist_add, name="watchlist_add"),
    path("api/watchlist/remove/", views.watchlist_remove, name="watchlist_remove"),
    path("api/watchlist/", views.watchlist_list, name="watchlist_list"),
    # Company Search API
    path(
        "api/companies/search/", views.search_companies_api, name="search_companies_api"
    ),
]
