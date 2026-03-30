@echo off
set /p RUN_DATE="Enter run date (YYYY-MM-DD): "
set RUN_DATE=%RUN_DATE%
GenerateChildAbsence.exe
pause