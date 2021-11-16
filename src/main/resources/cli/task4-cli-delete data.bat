@echo off
set CATALOG_ID=testmdctlgcli
set CATALOG_HRN=hrn:here:data::olp-here:%CATALOG_ID%

echo data deletion from object store and index layers
CALL olp catalog layer object delete %CATALOG_HRN% objectstorelayercli --key test/partition0
CALL olp catalog layer partition delete %CATALOG_HRN% indexlayercli --filter "testattr=gt=0"

pause