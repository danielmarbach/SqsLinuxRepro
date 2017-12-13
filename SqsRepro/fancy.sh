#
#!/bin/bash
echo "Bash version ${BASH_VERSION}..."
for i in {1..5} 
do
   dotnet run SqsRepro.csproj -c Release --no-rebuild --no-restore
done
