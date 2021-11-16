@echo off

set CATALOG_ID=testmdctlgcli
set CATALOG_HRN=hrn:here:data::olp-here:%CATALOG_ID%

::data reading
echo version layer data
CALL olp catalog layer partition get %CATALOG_HRN% versionlayercli --partitions partition0

echo volatile layer data
CALL olp catalog layer partition get %CATALOG_HRN% volatilelayercli --partitions partition0

echo object store layer data
CALL olp catalog layer object get %CATALOG_HRN% objectstorelayercli --key test/partition0

echo index layer data
CALL olp catalog layer partition get %CATALOG_HRN% indexlayercli --filter "testattr=gt=0"

pause