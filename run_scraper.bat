@echo off
REM Change directory to the folder where this batch file is located
cd /d "%~dp0"

REM Run the Streamlit app
python -m streamlit run linkedin_scraper_ui.py

REM Keep the window open so you can see any messages or errors
pause
