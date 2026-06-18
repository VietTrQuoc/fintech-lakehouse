@echo off
REM Khoi dong giao dien Streamlit cho Financial Transaction Data Lakehouse.
REM Chay file nay bang cach double-click hoac: run_dashboard.bat
cd /d "%~dp0"
".venv\Scripts\python.exe" -m streamlit run "app\streamlit_app.py"
pause
