REM this is more of a rough guide than an actual how to, since QGIS/batch files together just doesn't play well
qgis --project effective_max_dua.qgs --snapshot effective_max_dua.png --width 1500 --height 1000
gdal2tiles effective_max_dua.png -z 9-14 --s_srs epsg:26910

