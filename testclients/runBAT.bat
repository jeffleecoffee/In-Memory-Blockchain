echo off
set arg1=%1

for %%f in (%1\*.go) do (
	echo %%f
	start cmd /k go run %%f ClientToNodeWin.txt
	)
