@echo off
set /p NXLPath=Enter Nextlabs install directory : 

if exist "%NXLPath%\Policy Server" (
    echo Found Policy Server folder.
	
	REM SET PATH=%PATH%;%SystemRoot%\System32;%SystemRoot%;%SystemRoot%\System32\Wbem
	echo Stopping CC. This may take a while...
	NET stop EnterpriseDLPServer
	echo Done.
	
	if exist "%NXLPath%\Policy Server\server\plugins\config" (
		echo config directory exists.
	) else (
		echo config directory does not exist. Creating.
		mkdir "%NXLPath%\Policy Server\server\plugins\config"
	)
	
	if exist "%NXLPath%\Policy Server\server\plugins\jar\dow" (
		echo dow directory exists.
	) else (
		echo dow directory does not exist. Creating.
		mkdir "%NXLPath%\Policy Server\server\plugins\jar\dow"
	)
	
	if exist "%NXLPath%\Policy Server\server\plugins\jar\dow\data" (
		echo data directory exists.
	) else (
		echo data directory does not exist. Creating.
		mkdir "%NXLPath%\Policy Server\server\plugins\jar\dow\data"
	)
	
	echo Copying DowHeartbeatPluginServer_HB file.
	copy config\DowHeartbeatPluginServer_HB.properties "%NXLPath%\Policy Server\server\plugins\config"
	set basePath=%NXLPath:\=/%
	echo Adding CSV file path to DowHeartbeatPluginServer_HB.properties. [csv_file_path=%basePath%/Policy Server/server/plugins/jar/dow/data/licloaeccn.csv]
	echo csv_file_path=%basePath%/Policy Server/server/plugins/jar/dow/data/licloaeccn.csv >> "%NXLPath%\Policy Server\server\plugins\config\DowHeartbeatPluginServer_HB.properties"
	echo Done.
	
	echo Copying DowHeartbeatPluginServer JAR file.
	copy jars\DowHeartbeatPluginServer.jar "%NXLPath%\Policy Server\server\plugins\jar\dow"
	echo Done.
	
	echo Copying Common_HB file.
	copy config\Common_HB.properties "%NXLPath%\Policy Server\server\plugins\jar\dow"
	echo Done.
	
	echo Copying sample CSV file.
	copy /-Y licloaeccn.csv "%NXLPath%\Policy Server\server\plugins\jar\dow\data"
	echo Done.
	
	NET start EnterpriseDLPServer
	
	echo Waiting 120 seconds for CC to start.
	ping 192.0.2.2 -n 1 -w 120000 > nul
	echo Done.
	
	echo Adding properties.
	set OLDDIR=%CD%
	cd "%NXLPath%\Policy Server\tools\enrollment\"
	propertymgr.bat  -u Administrator -add -e USER -l loa -i "USER LOA" -t STRING
	propertymgr.bat  -u Administrator -add -e USER -l licenses -i "USER LICENSES" -t STRING
	cd %OLDDIR%
	echo Done.
	
) else (
    echo Could not find Policy Server folder. Quitting.
)
