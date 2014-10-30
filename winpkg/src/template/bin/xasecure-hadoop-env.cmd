@echo off
if not defined HADOOP_HOME (
	set HADOOP_HOME=%~dp0
)

for /f "usebackq delims=|" %%G in (`dir /b "%HADOOP_HOME%\share\hadoop\common\lib" ^| findstr /i "^hdfs-plugin-.*\.jar"`) do (

    set "XASECURE_PLUGIN_PATH=%HADOOP_HOME%\share\hadoop\common\lib\%%~G"

)

if exist %XASECURE_PLUGIN_PATH% (

	set XASECURE_PLUGIN_OPTS= -javaagent:%XASECURE_PLUGIN_PATH%=authagent
	rem Convert \\ to \ in path since its causing problem with findstr below
	Echo.%HADOOP_NAMENODE_OPTS:\\=\% | findstr /C:"%XASECURE_PLUGIN_OPTS:\\=\%">nul && (
		REM OPTIONS already set continue
	) || (
		set HADOOP_NAMENODE_OPTS= %XASECURE_PLUGIN_OPTS% %HADOOP_NAMENODE_OPTS%
		set HADOOP_SECONDARYNAMENODE_OPTS= %XASECURE_PLUGIN_OPTS% %HADOOP_SECONDARYNAMENODE_OPTS%
    )
) else (
    rem %XASECURE_PLUGIN_PATH% file doesn't exist
)
