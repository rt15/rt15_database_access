@echo off

rmdir /S /Q rda\obj\Release
rmdir /S /Q rda\obj\Debug
rmdir /S /Q rda\build
del /Q rda\lib\*rda*
rmdir /S /Q rda\project\.vs
del /Q rda\project\*.user

for /D %%I IN (rda_tests) DO call :clean_project %%I

goto :eof

:clean_project

rmdir /S /Q samples\%1\obj\Release
rmdir /S /Q samples\%1\obj\Debug
rmdir /S /Q samples\%1\build
del /Q samples\%1\bin\*%1*
rmdir /S /Q samples\%1\project\*.vs
del /Q samples\%1\project\*.user
