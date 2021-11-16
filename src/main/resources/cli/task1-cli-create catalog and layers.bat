@echo off
set CATALOG_ID=testmdctlgcli
set CATALOG_HRN=hrn:here:data::olp-here:%CATALOG_ID%

echo catalog creation
CALL olp catalog create %CATALOG_ID% "Test MD catalog CLI" ^
	--summary "Test catalog created via CLI by Mykola Drozdov as part of Scala bootcamp programm" ^
	--description "Test catalog created via CLI by Mykola Drozdov as part of Scala bootcamp programm"

echo layers creation
CALL olp catalog layer add %CATALOG_HRN% versionlayercli "Test version layer CLI" ^
	--versioned --summary "Test version layer CLI" --description "Test version layer CLI"
CALL olp catalog layer add %CATALOG_HRN% volatilelayercli "Test volatile layer CLI" ^
	--volatile --summary "Test volatile layer CLI" --description "Test volatile layer CLI"
CALL olp catalog layer add %CATALOG_HRN% streamlayercli "Test stream layer CLI" ^
	--stream --summary "Test stream layer CLI" --description "Test stream layer CLI"
CALL olp catalog layer add %CATALOG_HRN% objectstorelayercli "Test object store layer CLI" ^
	--objectstore --summary "Test object store layer CLI" --description "Test object store layer CLI"
CALL olp catalog layer add %CATALOG_HRN% indexlayercli "Test index layer CLI" ^
	--index --index-definitions testattr:timewindow:600000 --summary "Test index layer CLI" --description "Test index layer CLI"

pause