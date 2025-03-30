rem Rebuild
dotnet clean -c Release
dotnet build -c Release

rem Publish for .NET 8.0
dotnet publish MassiveSort.csproj -c Release -f net8.0

rem Build ZIP file for .NET 8.0
del /q MassiveSort.zip
cd bin\Release\net8.0\publish
..\..\..\..\zip.exe -9r ..\..\..\..\MassiveSort.zip *.*
pause