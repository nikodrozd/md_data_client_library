@echo off

set CATALOG_ID=testmdctlgcli
set CATALOG_HRN=hrn:here:data::olp-here:%CATALOG_ID%

echo data publishing to layers
CALL olp catalog layer partition put %CATALOG_HRN% versionlayercli ^
	--partitions partition0:partition-0.csv
CALL olp catalog layer partition put %CATALOG_HRN% volatilelayercli ^
	--partitions partition0:partition-0.csv
CALL olp catalog layer object put %CATALOG_HRN% objectstorelayercli ^
	--key test/partition0 --data partition-0.csv --content-type application/x-protobuf
CALL olp catalog layer partition put %CATALOG_HRN% indexlayercli ^
	--partitions partition0:partition-0.csv --index-fields timewindow:testattr:600000
	
pause