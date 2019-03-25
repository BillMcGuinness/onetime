@echo off
setlocal
set dt=%date:~-4%%date:~4,2%%date:~7,2%.%time:~0,2%%time:~3,2%%time:~6,2%%time:~9,3%
set dt=%dt: =0%
set log_file=C:\users\william.mcguinness\scratch\atlas_log\live_info.%dt%.log
call C:\users\william.mcguinness\Envs\onetime\Scripts\activate.bat
call python C:\users\william.mcguinness\PycharmProjects\onetime\jobs\poker_atlas\live_info.py --cleanup True > %log_file% 2>&1
end setlocal