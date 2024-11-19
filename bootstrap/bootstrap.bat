@echo off
set URL=https://raw.githubusercontent.com/jererc/savegame/refs/heads/main/bootstrap/bootstrap.py
curl -s %URL% | python
