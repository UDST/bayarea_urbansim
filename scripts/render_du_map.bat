CALL qgis.bat --project ^
"D:\temp\bayarea_urbansim\scripts\zoned_du_by_parcel.qgs" --snapshot zoned_du_by_parcel.png --width 15000 --height 10000
ECHO "image rendered"
gdal2tiles zoned_du_by_parcel.png -z 10-13 --s_srs epsg:26910

