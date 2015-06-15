del /q MassiveSort.zip
zip.exe -9j MassiveSort.zip bin\Release\*.* 
zip.exe -u MassiveSort.zip LICENSE.txt MassiveSort.url
zip.exe MassiveSort.zip -d MassiveSort.vshost.exe* 
pause