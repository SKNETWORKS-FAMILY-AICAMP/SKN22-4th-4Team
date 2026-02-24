from django.urls import path
from . import views

app_name = "finance_app"

urlpatterns = [
    path("", views.home, name="home"),
    path("signup/", views.SignUpView.as_view(), name="signup"),
    path("chat/", views.chat, name="chat"),
    path("api/chat/", views.chat_api, name="chat_api"),  # Chatbot API
    path("calendar/", views.calendar_view, name="calendar"),
    path(
        "api/calendar/", views.calendar_api, name="calendar_api"
    ),  # Calendar details API
    path("report/", views.report, name="report"),
    path(
        "api/report/search_tickers/",
        views.search_tickers_api,
        name="search_tickers_api",
    ),
    path("api/report/generate/", views.generate_report_api, name="generate_report_api"),
    path("report/download/pdf/", views.download_report_pdf, name="download_report_pdf"),
    # Watchlist API
    path("api/watchlist/add/", views.watchlist_add, name="watchlist_add"),
    path("api/watchlist/remove/", views.watchlist_remove, name="watchlist_remove"),
    path("api/watchlist/", views.watchlist_list, name="watchlist_list"),
    # Company Search API
    path(
        "api/companies/search/", views.search_companies_api, name="search_companies_api"
    ),
]
