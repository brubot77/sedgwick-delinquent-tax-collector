@echo off
setlocal
py -m venv venv
call venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m playwright install chromium
python collector.py --input Sedgwick_County_2024_Delinquent_Real_Estate_Raw.csv --output-dir output --limit 20 --headed
pause
