@echo off
setlocal
call venv\Scripts\activate
python collector.py --input Sedgwick_County_2024_Delinquent_Real_Estate_Raw.csv --output-dir output --retry-errors
pause
