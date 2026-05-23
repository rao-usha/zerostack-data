@echo off
REM Run gcloud Application Default Credentials auth flow.
REM Opens your browser; sign in with alexiusmichael@gmail.com (owner of nexdata-cloud).
REM Created by SPEC_059 follow-up session for cloud-DB access.

"%LOCALAPPDATA%\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd" auth application-default login
echo.
echo =====================================================
echo If you see "Credentials saved to file" above, you're done.
echo Close this window and tell Claude "done".
echo =====================================================
pause
