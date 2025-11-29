rem Rebuild
dotnet clean -c Release
dotnet build -c Release

rem Publish for .NET 10.0
dotnet publish MassiveSort.csproj -c Release -f net10.0

rem Build ZIP file for .NET 10.0
del /q MassiveSort.zip
cd bin\Release\net10.0\publish
..\..\..\..\zip.exe -9r ..\..\..\..\MassiveSort.zip *.*
pause