cd %~dp0
FOR /f %%p in ('where python') do SET PYTHONPATH=%%p
@echo off
%PYTHONPATH% %0\..\user_balance.py
%PYTHONPATH% %0\..\transaction_type.py