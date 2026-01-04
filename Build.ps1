[CmdletBinding()]
param (
  [Parameter(HelpMessage = 'The action to execute.')]
  [ValidateSet('Build', 'Test', 'Pack')]
  [string] $Action = 'Build',

  [Parameter(HelpMessage = 'The msbuild configuration to use.')]
  [ValidateSet('Debug', 'Release')]
  [string] $Configuration = 'Debug',


  [switch] $NoRestore,

  [switch] $Clean
)

function RunCommand {
  param ([string] $CommandExpr)
  Write-Verbose "  $CommandExpr"
  Invoke-Expression $CommandExpr
}

$rootDir = $PSScriptRoot

switch ($Action) {
  { 'Pack' -eq $_ } {
    $actionDir = 'src/Leger'
  }
  default {
    $actionDir = $rootDir
  }
}

if (!$NoRestore.IsPresent) {
  RunCommand "dotnet restore $actionDir --force --force-evaluate --no-cache --nologo --verbosity quiet"
}

if ($Clean) {
  RunCommand "dotnet clean $actionDir -c $Configuration --nologo --verbosity quiet"
}

switch ($Action) {
  'Test' { RunCommand "dotnet test `"$actionDir`"" }
  'Pack' { RunCommand "dotnet pack `"$actionDir`" -c $Configuration --include-symbols --include-source" }
  default { RunCommand "dotnet build `"$actionDir`" -c $Configuration" }
}
