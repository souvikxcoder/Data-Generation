cd %~dp0
FOR /f %%p in ('where python') do SET PYTHONPATH=%%p
@echo off
%PYTHONPATH% %0\..\transaction.py

