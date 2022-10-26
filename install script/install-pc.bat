@echo off
set /p NXLPath=Enter Nextlabs install directory : 

if exist "%NXLPath%\Policy Controller" (
    echo Found Policy Controller folder.
	
	REM SET PATH=%PATH%;%SystemRoot%\System32;%SystemRoot%;%SystemRoot%\System32\Wbem
	echo Stopping PC. This may take a while...
	"%NXLPath%\Policy Controller\bin\StopEnforcer.exe"
	echo Done.
	
	if exist "%NXLPath%\Policy Controller\jservice\config" (
		echo config directory exists.
	) else (
		echo config directory does not exist. Creating.
		mkdir "%NXLPath%\Policy Controller\jservice\config"
	)
	
	if exist "%NXLPath%\Policy Controller\jservice\jar\dow\data" (
		echo data directory exists.
	) else (
		echo data directory does not exist. Creating.
		mkdir "%NXLPath%\Policy Controller\jservice\jar\dow\data"
	)
	
	if exist "%NXLPath%\Policy Controller\jre\lib\ext" (
		echo ext directory exists.
	) else (
		echo ext directory does not exist. Creating.
		mkdir "%NXLPath%\Policy Controller\jre\lib\ext"
	)
	
	echo Copying DowHeartbeatPluginClient_HB file.
	copy config\DowHeartbeatPluginClient_HB.properties "%NXLPath%\Policy Controller\jservice\config"
	echo Done.
	
	echo Copying DowHeartbeatPluginClient JAR file.
	copy jars\DowHeartbeatPluginClient.jar "%NXLPath%\Policy Controller\jservice\jar\dow"
	echo Done.
	
	echo Copying Common_HB file.
	copy config\Common_HB.properties "%NXLPath%\Policy Controller\jservice\jar\dow"
	echo Done.
	
	echo Copying HSQL DB Script.
	copy config\DowLicenseDB.script "%NXLPath%\Policy Controller\jservice\jar\dow\data"
	echo Done.
	
	echo Copying libs.
	copy xlib\*.jar "%NXLPath%\Policy Controller\jre\lib\ext"
	echo Done.
	
	NET start ComplianceEnforcerService
	
) else (
    echo Could not find Policy Controller folder. Quitting.
)
