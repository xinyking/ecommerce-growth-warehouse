param([string]$JavaHome = $env:JAVA_HOME)

$ErrorActionPreference = 'Stop'
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $ProjectRoot '.venv/Scripts/python.exe'
if (-not (Test-Path -LiteralPath $Python)) {
    throw "The project Python virtual environment has not been prepared."
}
if (-not $JavaHome) {
    $JavaHome = (Get-ChildItem -Path (Join-Path $ProjectRoot '.tools') -Directory -Filter 'jdk-17*' -ErrorAction SilentlyContinue | Select-Object -First 1).FullName
}
if (-not $JavaHome -or -not (Test-Path -LiteralPath (Join-Path $JavaHome 'bin/java.exe'))) {
    throw "Set -JavaHome to the project's JDK 17 directory."
}

$PreviousJava = $env:JAVA_HOME
$PreviousPython = $env:PYSPARK_PYTHON
$PreviousHostname = $env:SPARK_LOCAL_HOSTNAME
try {
    $env:JAVA_HOME = $JavaHome
    $env:PYSPARK_PYTHON = $Python
    $env:SPARK_LOCAL_HOSTNAME = 'localhost'
    & $Python (Join-Path $PSScriptRoot 'spark_smoke.py')
    if ($LASTEXITCODE -ne 0) {
        throw "Spark validation exited with code $LASTEXITCODE."
    }
} finally {
    $env:JAVA_HOME = $PreviousJava
    $env:PYSPARK_PYTHON = $PreviousPython
    $env:SPARK_LOCAL_HOSTNAME = $PreviousHostname
}
