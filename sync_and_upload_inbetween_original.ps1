# =========================
# Two-way sync (clean build) — S3→Z discovery + Z↔S3 sync
# Hydrates each project fully (folders then files) before moving on.
# =========================

param(
    [string]$ZDriveRoot = "F:\ZDrive",          # root of your local Z drive mirror
    # [string]$ZDriveRoot = "\\3r1h4t\Filevine Folders",          # root of your local Z drive mirror
    [string]$S3Path     = "s3://two-way-sync"   # BUCKET ONLY (no folder segments, no trailing slash)
)

# -------- CONFIG (edit for your env) --------
$S3RootPrefix         = "lojedemofolder"    # top-level folder in the bucket
$OrgMarker            = "law offices of jacob emrani"  # case-insensitive matcher used for stripping
$OrgFolderName        = "Law Offices of Jacob Emrani"  # how the org folder should be titled on S3
$RootFolderId         = 0                   # TODO: set real Filevine root folder ID if uploading
$RequireResolved      = $true               # require resolved folder path on Filevine upload
$PythonExe            = "python"
$PythonUploader       = "C:\Kritagya Folder\Two way sync\fv_uploader_inbetween_original.py"
$ProjectMapPath       = "C:\Kritagya Folder\FileVineBI\project_map.json"
$TempS3List           = "$env:TEMP\s3_file_list.txt"
$EnableFilevineUpload = $true               # set $false to silence FV uploads

# Ignore patterns (filenames only)
$IgnoreGlobs = @(
  '*.placeholder','~$*','*.tmp','.DS_Store','Thumbs.db','.last_sync_state.json',
  '*.part','*.crdownload','*.temp','*.swp','*.swx','*.lnk',
  # e.g. Office/Editor temp renames like foo.docx.E8470593 or acne.jpg.8cCE6016 (case-insensitive hex)
  '*.*.[0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f]'
)

# -------- ENV LOADER (.env contains FILEVINE_TO_S3_WEBHOOK) --------
function Load-DotEnv {
    param([string]$Path = ".env")
    if (-not (Test-Path $Path)) { Write-Host "WARNING: .env not found at $Path"; return }
    Get-Content $Path | ForEach-Object {
        if ($_ -match '^\s*#') { return }
        if ($_ -match '^\s*$') { return }
        $pair = $_ -split '=', 2
        if ($pair.Length -eq 2) {
            $key = $pair[0].Trim()
            $val = $pair[1].Trim()
            if ($val.StartsWith('"') -and $val.EndsWith('"')) { $val = $val.Substring(1, $val.Length - 2) }
            Set-Item -Path "Env:$key" -Value $val
        }
    }
}
Load-DotEnv "C:\Kritagya Folder\Two way sync\.env"
$WebhookUrl = $env:FILEVINE_TO_S3_WEBHOOK
Write-Host "Webhook URL loaded: $WebhookUrl"

# -------- LOG --------
function Log { param([string]$msg) $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'; Write-Host "$ts - $msg" }

# -------- LONG PATH HELPERS --------
function Use-LongPath {
    param([string]$Path)
    if ([string]::IsNullOrWhiteSpace($Path)) { return $Path }
    if ($Path.StartsWith('\\?\')) { return $Path }              # already extended
    # 🔧 FIX: DO NOT convert UNC paths to \\?\UNC\...
    if ($Path.StartsWith('\\'))   { return $Path }              # <-- was returning \\?\UNC\...
    if ($Path -match '^[A-Za-z]:\\') { return "\\?\$Path" }     # only extend local drive letters
    return $Path
}


function Test-FileExists {
    param([string]$Path)
    $lp = Use-LongPath $Path
    return (Test-Path -Path $lp -PathType Leaf)
}
function Ensure-Directory {
    param([string]$DirPath)
    $lp = Use-LongPath $DirPath
    if (-not (Test-Path -Path $lp -PathType Container)) {
        try { [System.IO.Directory]::CreateDirectory($lp) | Out-Null } catch { throw }
    }
}

# -------- NTFS ADS + fingerprint (long-path safe) --------
function Set-FileMeta {
    param([string]$Path, [hashtable]$KV)
    $lp = Use-LongPath $Path
    foreach ($k in $KV.Keys) {
        try { Set-Content -Path $lp -Stream "sync.$k" -Value "$($KV[$k])" -ErrorAction SilentlyContinue | Out-Null } catch { throw }
    }
}
function Get-FileMeta {
    param([string]$Path, [string]$Key)
    $lp = Use-LongPath $Path
    try { return (Get-Content -Path $lp -Stream "sync.$Key" -ErrorAction Stop) } catch { return $null }
}
function Get-LocalMD5 {
    param([string]$Path)
    $lp = Use-LongPath $Path
    try { (Get-FileHash -Algorithm MD5 -Path $lp).Hash.ToLower() } catch { $null }
}
function MakeFp {
    param([string]$Path)
    $lp = Use-LongPath $Path
    try {
        $len = ([int64](Get-Item -Path $lp).Length)
        "$((Get-LocalMD5 $lp))|$len"
    } catch { $null }
}

# -------- PROJECT MAP CACHE --------
$ProjectMap = @{}
function Load-ProjectMap {
    if (Test-Path $ProjectMapPath) {
        try {
            $raw = Get-Content $ProjectMapPath -Raw | ConvertFrom-Json
            $ht = @{}
            if ($null -eq $raw) {
                $script:ProjectMap = @{}
                Log "Loaded empty project map (no entries)."
                return
            }
            if ($raw.PSObject -and $raw.PSObject.Properties) {
                foreach ($p in $raw.PSObject.Properties) {
                    $val = $p.Value
                    if ($val -is [string]) { $tmp = 0; if ([int]::TryParse($val, [ref]$tmp)) { $val = $tmp } }
                    $ht[$p.Name] = $val
                }
            } else { Log "Project map JSON was an unexpected shape. Starting with empty map." }
            $script:ProjectMap = $ht
            Log "Loaded project map with $($script:ProjectMap.Count) entries from $ProjectMapPath"
        } catch { Log "Failed to load project map: $_"; $script:ProjectMap = @{} }
    } else {
        Log "Project map not found at $ProjectMapPath"
        $script:ProjectMap = @{}
    }
}
Load-ProjectMap

function Get-ProjectId {
    param([string]$ProjectName)

    # normalize $ProjectMap to hashtable
    if ($ProjectMap -isnot [hashtable]) {
        $ht = @{}
        if ($ProjectMap -and $ProjectMap.PSObject -and $ProjectMap.PSObject.Properties) {
            foreach ($p in $ProjectMap.PSObject.Properties) { $ht[$p.Name] = $p.Value }
        }
        $script:ProjectMap = $ht
    }

    # 1) exact hit?
    if ($ProjectMap.ContainsKey($ProjectName)) { return [int]$ProjectMap[$ProjectName] }

    # 2) try sanitized key (many folders are sanitized)
    $san = Sanitize-Name $ProjectName
    if ($ProjectMap.ContainsKey($san)) { return [int]$ProjectMap[$san] }

    try {
        Log "Resolving ProjectId for $ProjectName via Python..."
        $result = & $PythonExe $PythonUploader "--lookup-project" "$ProjectName"
        $id = 0

        if ($result) {
            if ([int]::TryParse($result.Trim(), [ref]$id)) { }
            elseif ($result -match '(\d{4,})') { [void][int]::TryParse($Matches[1], [ref]$id) }
        }

        if ($id -gt 0) {
            Log "Resolved ProjectId $id for $ProjectName"

            $pmDir = Split-Path $ProjectMapPath -Parent
            if (-not (Test-Path $pmDir)) { New-Item -ItemType Directory -Path $pmDir -Force | Out-Null }

            $ProjectMap[$ProjectName] = $id
            $ProjectMap[$san] = $id

            $ProjectMap | ConvertTo-Json -Depth 5 | Out-File $ProjectMapPath -Encoding UTF8 -Force
            return $id
        } else {
            Log "Could not resolve ProjectId for $ProjectName (python returned '$result')"
            return $null
        }
    } catch {
        Log "Error resolving ProjectId for $ProjectName : $_"
        return $null
    }
}

# -------- SANITIZE --------
function Sanitize-Name {
    param([string]$name)
    if ([string]::IsNullOrWhiteSpace($name)) { return 'Unnamed' }
    $name = $name -replace '[<>:"/\\|?\x00-\x1f]', ''
    $name = ($name -replace '\s+', ' ').Trim()
    $name = $name.Trim('.')
    if ([string]::IsNullOrWhiteSpace($name)) { return 'Unnamed' }
    return $name
}

# -------- MANIFEST --------
function Load-Manifest {
    param([string]$ManifestPath)
    if (Test-Path $ManifestPath) {
        try {
            $json = Get-Content $ManifestPath -Raw | ConvertFrom-Json
            $ht = @{}
            foreach ($p in $json.PSObject.Properties) { $ht[$p.Name] = $p.Value }
            return $ht
        } catch { Log "Error loading manifest: $_"; return @{} }
    } else { return @{} }
}
function Save-Manifest {
    param([hashtable]$state, [string]$ManifestPath)
    try { $state | ConvertTo-Json -Depth 12 | Out-File $ManifestPath -Encoding UTF8 -Force }
    catch { Log "Error saving manifest: $_" }
}

# -------- IGNORE --------
function _IsIgnored { param([string]$name) foreach ($g in $IgnoreGlobs) { if ($name -like $g) { return $true } } return $false }
function Should-IgnorePath { param([string]$Path) $leaf = Split-Path $Path -Leaf; return (_IsIgnored $leaf) }

# -------- STATE BUILDERS --------
function Get-LocalState {
    param([string]$LocalPath)
    $result = @{}
    if (Test-Path $LocalPath) {
        Get-ChildItem -Path $LocalPath -Recurse -File | ForEach-Object {
            if (_IsIgnored $_.Name) { return }
            $relative = $_.FullName.Substring($LocalPath.Length + 1).Replace('\','/')
            $result[$relative] = @{ source='local'; lastModified=$_.LastWriteTimeUtc.ToString('o') }
        }
    }
    return $result
}

function Get-S3State {
    param([string]$S3Path, [string]$S3Prefix)
    $result = @{}
    $fullPrefix = "$S3Prefix/"
    try {
        aws s3 ls "$S3Path/$fullPrefix" --recursive | Out-File $TempS3List -Encoding UTF8
        Get-Content $TempS3List | ForEach-Object {
            if ($_ -match '^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+\d+\s+(.+)$') {
                $timestampStr = $Matches[1]
                $fullKey      = $Matches[2].Trim()
                if ($fullKey -eq $fullPrefix -or $fullKey -eq $S3Prefix) { return }
                $relativeOriginal = $fullKey.Substring($fullPrefix.Length)  # keep original case
                if ([string]::IsNullOrWhiteSpace($relativeOriginal)) { return }
                if (_IsIgnored (Split-Path $relativeOriginal -Leaf)) { return }
                $timestamp = [datetime]::ParseExact($timestampStr, 'yyyy-MM-dd HH:mm:ss', $null)
                $result[$relativeOriginal.ToLower()] = @{
                    source='s3'
                    lastModified=$timestamp.ToUniversalTime().ToString('o')
                    realKey=$fullKey
                    relOriginal=$relativeOriginal
                }
            }
        }
    } catch { Log "Error getting S3 state: $_" }
    return $result
}

# -------- S3 PROJECT DISCOVERY --------
function Get-S3Projects {
  param([string]$S3Path, [string]$RootPrefix)
  $projectNames = @()
  $root = "$S3Path/$RootPrefix/"
  try {
    $lines = aws s3 ls $root
    foreach ($line in $lines) {
      if ($line -match '^\s*PRE\s+(.+?)/\s*$') { $projectNames += $Matches[1] }
    }
  } catch { Log "Failed to list S3 root ($root): $_" }
  return $projectNames
}
function Ensure-LocalProjectFolder {
  param([string]$ZRoot, [string]$ProjectName)
  $safe = Sanitize-Name $ProjectName
  $localPath = Join-Path $ZRoot $safe
  if (-not (Test-Path $localPath)) {
    Log "Creating local project folder for S3 project: $ProjectName"
    Ensure-Directory -DirPath $localPath
    New-Item -ItemType File -Path (Join-Path $localPath ".placeholder") -Force | Out-Null
  }
  return (Get-Item $localPath)
}

# -------- OPTIONAL: Refresh S3 via webhook --------
function Refresh-S3-FromFilevine {
  param([int]$ProjectId)
  if (-not $ProjectId -or $ProjectId -le 0) { Log 'Refresh skipped (no ProjectId).'; return }
  if ([string]::IsNullOrWhiteSpace($WebhookUrl)) { Log 'Refresh skipped (no webhook configured).'; return }
  try {
    Log "Triggering Filevine->S3 refresh for project $ProjectId ..."
    $payload = @{ projectId = $ProjectId } | ConvertTo-Json
    $resp = Invoke-RestMethod -Method Post -Uri $WebhookUrl -Body $payload -ContentType 'application/json' -TimeoutSec 60
    Log "Webhook response: $($resp | Out-String)"
    Start-Sleep -Seconds 4
  } catch { Log "Refresh failed: $_" }
}

# -------- Upload to Filevine (optional) --------
function Invoke-FilevineUpload {
    param(
        [string]$FullLocalPath,
        [string]$RelativeKey,
        [int]$ProjectId
    )
    try {
        # Folder path inside the project (relative to the project root)
        $relFolder = (Split-Path $RelativeKey -Parent).Replace('\','/').Trim('/')

        # Do NOT strip "Documents" here; let Python resolve smartly.
        $subpath = $relFolder

        Log "FV upload: '$FullLocalPath' → Project $ProjectId | subpath='$subpath'"

        $args = @($PythonUploader,'--project-id',$ProjectId,'--file',"$FullLocalPath")
        if ($RootFolderId -gt 0) { $args += @('--root-folder-id', $RootFolderId) }
        if ($subpath)           { $args += @('--folder-path', $subpath) }
        if ($RequireResolved -and $subpath) { $args += '--require-resolved' }

        $out  = & $PythonExe @args 2>&1
        $code = $LASTEXITCODE

        foreach ($line in $out) { Log "FV: $line" }
        if ($code -ne 0) { Log "FV uploader exited with code $code" }

        Start-Sleep -Milliseconds 400
    } catch {
        Log "Filevine upload failed: $_"
    }
}

# -------- Context builder (sets the project S3 prefix) --------
function Map-ProjectContext {
    param([System.IO.DirectoryInfo]$ProjDir)

    $LocalPath   = $ProjDir.FullName
    $ProjectName = $ProjDir.Name

    $ProjectNameNormalized = Sanitize-Name $ProjectName

    # Choose org segment text: prefer pretty display name if set, else marker
    $orgRaw = if ($OrgFolderName -and $OrgFolderName.Trim()) { $OrgFolderName } else { $OrgMarker }
    $OrgSeg  = Sanitize-Name $orgRaw

    # Desired layout:
    #   <root>/<Project>/<Org>/<Project>/
    $S3Prefix = "$S3RootPrefix/$ProjectNameNormalized/$OrgSeg/$ProjectNameNormalized"

    $ManifestPath = Join-Path $LocalPath ".last_sync_state.json"
    $ProjectId    = Get-ProjectId $ProjectName

    return @{
        LocalPath    = $LocalPath
        ProjectName  = $ProjectName
        S3Prefix     = $S3Prefix
        ManifestPath = $ManifestPath
        ProjectId    = $ProjectId
    }
}

# -------- File watcher --------
function Start-FileWatcher {
    param(
        [string]$LocalPath,
        [string]$S3Prefix,
        [string]$ManifestPath,
        [int]$ProjectId,
        [string]$S3PathParam,
        [string]$ProjectName 
    )
    $watcher = New-Object System.IO.FileSystemWatcher
    $watcher.Path = $LocalPath
    $watcher.IncludeSubdirectories = $true
    $watcher.EnableRaisingEvents = $true
    $watcher.NotifyFilter = [IO.NotifyFilters]'FileName, DirectoryName, LastWrite, Size'

    $md = @{ LocalPath=$LocalPath; S3Prefix=$S3Prefix; ManifestPath=$ManifestPath; ProjectId=$ProjectId; S3Path=$S3PathParam; ProjectName=$ProjectName}

    Register-ObjectEvent -InputObject $watcher -EventName Created -MessageData $md -Action {
        param($sender,$eventArgs)
        $m = $Event.MessageData; $path = $eventArgs.FullPath
        Log "Created: $path"
        try {
            if (Should-IgnorePath $path) { Log "Ignored change: $path"; return }
            if (Test-Path $path -PathType Container) { Log "Directory event ignored: $path"; return }
            TwoWaySync -LocalPath $m.LocalPath -S3Prefix $m.S3Prefix -ManifestPath $m.ManifestPath -S3Path $m.S3Path -ChangedFile $path -ProjectId $m.ProjectId -ProjectName $m.ProjectName
        } catch { Log "Sync error on create: $_" }
    } | Out-Null

    Register-ObjectEvent -InputObject $watcher -EventName Changed -MessageData $md -Action {
        param($sender,$eventArgs)
        $m = $Event.MessageData; $path = $eventArgs.FullPath
        Log "Changed: $path"
        try {
            if (Should-IgnorePath $path) { Log "Ignored change: $path"; return }
            if (Test-Path $path -PathType Container) { Log "Directory event ignored: $path"; return }
            TwoWaySync -LocalPath $m.LocalPath -S3Prefix $m.S3Prefix -ManifestPath $m.ManifestPath -S3Path $m.S3Path -ChangedFile $path -ProjectId $m.ProjectId -ProjectName $m.ProjectName
        } catch { Log "Sync error on change: $_" }
    } | Out-Null

    Register-ObjectEvent -InputObject $watcher -EventName Deleted -MessageData $md -Action {
        param($sender,$eventArgs)
        $m = $Event.MessageData; $path = $eventArgs.FullPath
        Log "Deleted: $path"
        try {
            if (Should-IgnorePath $path) { Log "Ignored change: $path"; return }
            if (Test-Path $path -PathType Container) { Log "Directory event ignored: $path"; return }
            TwoWaySync -LocalPath $m.LocalPath -S3Prefix $m.S3Prefix -ManifestPath $m.ManifestPath -S3Path $m.S3Path -ChangedFile $path -ProjectId $m.ProjectId -ProjectName $m.ProjectName
        } catch { Log "Sync error on delete: $_" }
    } | Out-Null

    Register-ObjectEvent -InputObject $watcher -EventName Renamed -MessageData $md -Action {
        param($sender,$eventArgs)
        $m = $Event.MessageData; $oldPath = $eventArgs.OldFullPath; $newPath = $eventArgs.FullPath
        Log "Renamed: $oldPath -> $newPath"
        try {
            if (-not (Should-IgnorePath $oldPath)) {
                if (Test-Path $oldPath -PathType Container) { Log "Directory event ignored: $oldPath"; return }
                TwoWaySync -LocalPath $m.LocalPath -S3Prefix $m.S3Prefix -ManifestPath $m.ManifestPath -S3Path $m.S3Path -ChangedFile $oldPath -ProjectId $m.ProjectId -ProjectName $m.ProjectName
            }
            if (-not (Should-IgnorePath $newPath)) {
                if (Test-Path $newPath -PathType Container) { Log "Directory event ignored: $newPath"; return }
                TwoWaySync -LocalPath $m.LocalPath -S3Prefix $m.S3Prefix -ManifestPath $m.ManifestPath -S3Path $m.S3Path -ChangedFile $newPath -ProjectId $m.ProjectId -ProjectName $m.ProjectName
            }
        } catch { Log "Sync error on rename: $_" }
    } | Out-Null

    Log "Watching: $LocalPath"
}

# Normalize S3 relative keys (defensive; keeps hydration robust if listing scope changes)
function Normalize-RelForLocal {
    param(
        [string]$Rel,          # original-case relative key from S3
        [string]$ProjectName   # original project folder name
    )
    $r = $Rel
    $orgEsc  = [regex]::Escape($OrgMarker)
    $projEsc = [regex]::Escape($ProjectName)

    # A) Keys that begin with "<Org>/<anyProject>/..."
    $r = [regex]::Replace($r, "^(?i)$orgEsc/[^/]+/", "")

    # B) Keys that begin with "<Project>/<Org>/<Project>/..."
    $r = [regex]::Replace($r, "^(?i)$projEsc/$orgEsc/$projEsc/", "")

    return $r
}

# -------- Core sync --------
function TwoWaySync {
    param(
        [string]$LocalPath,
        [string]$S3Prefix,
        [string]$ManifestPath,
        [string]$S3Path,
        [int]$ProjectId,
        [string]$ProjectName,
        [string]$ChangedFile = $null,
        [switch]$HydrateOnly
    )
    Log "Sync: $LocalPath -> $S3Path/$S3Prefix"

    # Fast-path (single file change)
    if ($ChangedFile) {
        if ($HydrateOnly) { Log "HydrateOnly: fast-path upload suppressed."; return }
        if (Test-Path $ChangedFile -PathType Container) { Log "Fast-path skipped (is a directory): $ChangedFile"; return }

        $relativeKey = $ChangedFile.Substring($LocalPath.Length + 1).Replace('\','/')
        if (_IsIgnored (Split-Path $relativeKey -Leaf)) { Log "Fast-path ignored (pattern): $relativeKey"; return }

        # Echo shield: skip if content fingerprint is unchanged
        $currFp   = MakeFp $ChangedFile
        $storedFp = Get-FileMeta $ChangedFile 'fingerprint'
        if ($storedFp -and $currFp -and $storedFp -eq $currFp) {
            Log "Fast-path echo shield: fingerprint unchanged -> skip upload: $relativeKey"
            return
        }

        Log "Fast-path file: $relativeKey"
        $s3Uri = "$S3Path/$S3Prefix/$relativeKey".Replace('\','/')

        if (Test-Path $ChangedFile) {
            Log "Uploading to S3: $relativeKey"
            $out = aws s3 cp $ChangedFile $s3Uri 2>&1
            if ($LASTEXITCODE -ne 0) { Log "S3 upload FAILED (exit $LASTEXITCODE) for: $ChangedFile"; Write-Host $out }
            else {
                $fp = $currFp; if (-not $fp) { $fp = MakeFp $ChangedFile }
                if ($fp) { Set-FileMeta -Path $ChangedFile -KV @{ origin='local'; fingerprint=$fp; markedAt=(Get-Date).ToString('o') } }
                if ($EnableFilevineUpload -and $ProjectId -gt 0) { Invoke-FilevineUpload -FullLocalPath $ChangedFile -RelativeKey $relativeKey -ProjectId $ProjectId }
            }
        } else {
            Log "Local deleted -> remove S3: $relativeKey"
            try { aws s3 rm $s3Uri --quiet } catch { Log "Failed to delete from S3: $_" }
        }
        return
    }

    # Full compare path
    Refresh-S3-FromFilevine -ProjectId $ProjectId

    $previous = Load-Manifest -ManifestPath $ManifestPath
    $local    = Get-LocalState -LocalPath $LocalPath
    $s3       = Get-S3State -S3Path $S3Path -S3Prefix $S3Prefix

    Log "Local: $($local.Keys.Count); S3: $($s3.Keys.Count); Prev: $($previous.Keys.Count)"
    Log "S3 sample under '$S3Prefix':"
    try { aws s3 ls "$S3Path/$S3Prefix/" --recursive | Select-Object -First 5 | ForEach-Object { Log $_ } }
    catch { Log "S3 preview failed: $_" }

    $allKeys = @()
    $allKeys += $local.Keys
    $allKeys += $s3.Keys
    $allKeys += ($previous.Keys | ForEach-Object { $_.ToLower() })
    $allKeys = $allKeys | Sort-Object -Unique

    # Process placeholders first (ensures folder creation), then shallower paths first
    $allKeys = $allKeys | Sort-Object `
        @{ Expression = { -not $_.ToLower().EndsWith('.placeholder') }; Ascending = $true }, `
        @{ Expression = { $_.Split('/').Length }; Ascending = $true }, `
        @{ Expression = { $_ }; Ascending = $true }

    Log "Total keys to process: $($allKeys.Count)"

    foreach ($rawKey in $allKeys) {
        if ([string]::IsNullOrWhiteSpace($rawKey)) { continue }
        if (_IsIgnored (Split-Path $rawKey -Leaf)) { continue }

        $keyLower = $rawKey.ToLower()

        # S3 presence (we need this first to access relOriginal)
        $inS3 = $s3.ContainsKey($keyLower)

        # Keep original-case relative key from S3 when present
        $relOriginal = if ($inS3) { $s3[$keyLower].relOriginal } else { $rawKey }

        # Collapse any higher-level prefixes defensively (usually a no-op here)
        $relForLocal = Normalize-RelForLocal -Rel $relOriginal -ProjectName $ProjectName

        # Local/S3/Prev lookups using the LOCAL-style relative key
        $inLocal = $local.ContainsKey($relForLocal)
        $inPrev  = $previous.ContainsKey($relForLocal) -or $previous.ContainsKey($keyLower) -or $previous.ContainsKey($rawKey)

        # Paths/URIs
        $localFile   = Join-Path $LocalPath ($relForLocal.Replace('/', '\'))
        $localFileLP = Use-LongPath $localFile
        $realS3Key   = if ($inS3) { $s3[$keyLower].realKey } else { "$S3Prefix/$relForLocal" }
        $s3Uri       = "$S3Path/$realS3Key".Replace('\','/')

        # 1) placeholders create folders locally
        if ($relOriginal.ToLower().EndsWith('.placeholder')) {
            $folderRel = $relForLocal.Substring(0, $relForLocal.Length - '.placeholder'.Length)
            $folderLoc = Join-Path $LocalPath ($folderRel.Replace('/', '\'))
            if (-not (Test-Path $folderLoc)) {
                Log "Create folder from placeholder: $folderRel"
                Ensure-Directory -DirPath $folderLoc
            }
            continue
        }

        # Helper to read "source" from $previous regardless of which key form was saved
        $prevRec = $null
        if     ($previous.ContainsKey($relForLocal)) { $prevRec = $previous[$relForLocal] }
        elseif ($previous.ContainsKey($keyLower))    { $prevRec = $previous[$keyLower] }
        elseif ($previous.ContainsKey($rawKey))      { $prevRec = $previous[$rawKey] }

        # 2) deleted on S3 -> delete local
        if ($inPrev -and -not $inS3 -and $prevRec -and $prevRec.source -eq 's3') {
            if ($inLocal -and (Test-Path $localFileLP)) {
                Log "Deleted in S3 -> remove local: $relForLocal"
                try { Remove-Item -Path $localFileLP -Force -ErrorAction Stop } catch { Log "Failed local delete: $_" }
            }
            continue
        }

        # 3) deleted locally -> delete S3 (unless hydrating)
        if ($inPrev -and -not $inLocal -and $prevRec -and $prevRec.source -eq 'local') {
            if ($HydrateOnly) { Log "HydrateOnly: skip S3 delete for $relForLocal"; continue }
            Log "Deleted local -> remove S3: $relForLocal"
            try { aws s3 rm $s3Uri --quiet } catch { Log "Failed S3 delete: $_" }
            continue
        }

        # 4) both exist -> compare times
        # if ($inLocal -and $inS3) {
        #     try {
        #         $lt = [datetime]::Parse(($local[$relForLocal].lastModified))
        #         $st = [datetime]::Parse(($s3[$keyLower].lastModified))
        #         if ($lt -gt $st) {
        #             if ($HydrateOnly) { Log "HydrateOnly: skip upload (local newer): $relForLocal"; continue }

        #             $origin = Get-FileMeta $localFile 'origin'
        #             if ($origin -eq 'filevine') {
        #                 $stored = Get-FileMeta $localFile 'fingerprint'
        #                 $curr   = MakeFp $localFile
        #                 if ($stored -and $stored -eq $curr) { Log "Echo shield: skip upload (unchanged Filevine download): $relForLocal"; continue }
        #             }

        #             Log "Local newer -> upload: $relForLocal"
        #             $out = aws s3 cp $localFile $s3Uri 2>&1
        #             if ($LASTEXITCODE -ne 0) { Log "S3 upload FAILED (exit $LASTEXITCODE) for: $localFile"; Write-Host $out }
        #             else {
        #                 $fp = MakeFp $localFile
        #                 if ($fp) { Set-FileMeta -Path $localFile -KV @{ origin='local'; fingerprint=$fp; markedAt=(Get-Date).ToString('o') } }
        #                 if ($EnableFilevineUpload -and $ProjectId -gt 0) { Invoke-FilevineUpload -FullLocalPath $localFile -RelativeKey $relForLocal -ProjectId $ProjectId }
        #             }
        #         } elseif ($st -gt $lt) {
        #             Log "S3 newer -> download: $relForLocal"
        #             $parentDir = Split-Path $localFile -Parent
        #             Ensure-Directory -DirPath $parentDir
        #             $out = aws s3 cp $s3Uri $localFile 2>&1
        #             if ($LASTEXITCODE -ne 0) { Log "S3 download FAILED (exit $LASTEXITCODE) for: $s3Uri"; Write-Host $out; continue }
        #             if (Test-FileExists $localFile) {
        #                 $fp = MakeFp $localFile
        #                 if ($fp) { Set-FileMeta -Path $localFile -KV @{ origin='filevine'; fingerprint=$fp; markedAt=(Get-Date).ToString('o') } }
        #                 else { Log "Warning: fingerprint failed for: $localFile" }
        #             } else { Log "Download reported success but file not found (long-path?) : $localFile" }
        #         }
        #     } catch { Log "Compare/sync error: $_" }
        #     continue
        # }
        # 4) both exist -> compare times
        if ($inLocal -and $inS3) {
            try {
                $lt = [datetime]::Parse(($local[$relForLocal].lastModified))   # local timestamp
                $st = [datetime]::Parse(($s3[$keyLower].lastModified))         # s3 timestamp
                $skew = [timespan]::FromSeconds(2)

                # Content fingerprint (echo-shield) – works for both local-origin and filevine-origin files
                $stored = Get-FileMeta $localFile 'fingerprint'
                $curr   = MakeFp $localFile
                if ($stored -and $curr -and $stored -eq $curr) {
                    # Unchanged bytes; do nothing even if timestamps disagree
                    Log "Echo shield: unchanged content -> skip sync: $relForLocal"
                    continue
                }

                # If times are within small skew, do nothing
                if ([math]::Abs(($lt - $st).TotalSeconds) -lt $skew.TotalSeconds) {
                    Log "Skew guard: timestamps within $($skew.TotalSeconds)s -> skip: $relForLocal"
                    continue
                }

                if ($lt -gt $st) {
                    if ($HydrateOnly) { Log "HydrateOnly: skip upload (local newer): $relForLocal"; continue }

                    Log "Local newer -> upload: $relForLocal"
                    $out = aws s3 cp $localFile $s3Uri 2>&1
                    if ($LASTEXITCODE -ne 0) { Log "S3 upload FAILED (exit $LASTEXITCODE) for: $localFile"; Write-Host $out }
                    else {
                        $fp = $curr; if (-not $fp) { $fp = MakeFp $localFile }
                        if ($fp) { Set-FileMeta -Path $localFile -KV @{ origin='local'; fingerprint=$fp; markedAt=(Get-Date).ToString('o') } }
                        if ($EnableFilevineUpload -and $ProjectId -gt 0) { Invoke-FilevineUpload -FullLocalPath $localFile -RelativeKey $relForLocal -ProjectId $ProjectId }
                    }
                } elseif ($st -gt $lt) {
                    Log "S3 newer -> download: $relForLocal"
                    $parentDir = Split-Path $localFile -Parent
                    Ensure-Directory -DirPath $parentDir
                    $out = aws s3 cp $s3Uri $localFile 2>&1
                    if ($LASTEXITCODE -ne 0) { Log "S3 download FAILED (exit $LASTEXITCODE) for: $s3Uri"; Write-Host $out; continue }
                    if (Test-FileExists $localFile) {
                        $fp = MakeFp $localFile
                        if ($fp) { Set-FileMeta -Path $localFile -KV @{ origin='filevine'; fingerprint=$fp; markedAt=(Get-Date).ToString('o') } }
                        else { Log "Warning: fingerprint failed for: $localFile" }
                    } else { Log "Download reported success but file not found (long-path?) : $localFile" }
                }
            } catch { Log "Compare/sync error: $_" }
            continue
        }

        # 5) new local -> upload
        if ($inLocal -and -not $inS3) {
            if ($HydrateOnly) { Log "HydrateOnly: skip upload (new local): $relForLocal"; continue }

            $origin = Get-FileMeta $localFile 'origin'
            if ($origin -eq 'filevine') {
                $stored = Get-FileMeta $localFile 'fingerprint'
                $curr   = MakeFp $localFile
                if ($stored -and $stored -eq $curr) { Log "Echo shield: skip upload (unchanged Filevine download): $relForLocal"; continue }
            }

            Log "New local -> upload: $relForLocal"
            try {
                $out = aws s3 cp $localFile $s3Uri 2>&1
                if ($LASTEXITCODE -ne 0) { Log "S3 upload FAILED (exit $LASTEXITCODE) for: $localFile"; Write-Host $out }
                else {
                    $fp = MakeFp $localFile
                    if ($fp) { Set-FileMeta -Path $localFile -KV @{ origin='local'; fingerprint=$fp; markedAt=(Get-Date).ToString('o') } }
                    if ($EnableFilevineUpload -and $ProjectId -gt 0) { Invoke-FilevineUpload -FullLocalPath $localFile -RelativeKey $relForLocal -ProjectId $ProjectId }
                }
            } catch { Log "Upload failed: $_" }
            continue
        }

        # 6) new on S3 -> download
        if ($inS3 -and -not $inLocal) {
            Log "New S3 -> download: $relForLocal"
            $parentDir = Split-Path $localFile -Parent
            Ensure-Directory -DirPath $parentDir
            $out = aws s3 cp $s3Uri $localFile 2>&1
            if ($LASTEXITCODE -ne 0) { Log "S3 download FAILED (exit $LASTEXITCODE) for: $s3Uri"; Write-Host $out; continue }
            if (Test-FileExists $localFile) {
                $fp = MakeFp $localFile
                if ($fp) { Set-FileMeta -Path $localFile -KV @{ origin='filevine'; fingerprint=$fp; markedAt=(Get-Date).ToString('o') } }
                else { Log "Warning: fingerprint failed for: $localFile" }
            } else { Log "Download reported success but file not found (long-path?) : $localFile" }
        }
    }

    # Save manifest
    $newState = @{}
    foreach ($k in $allKeys) {
        if     ($local.ContainsKey($k))    { $newState[$k] = $local[$k] }
        elseif ($s3.ContainsKey($k))       { $newState[$k] = $s3[$k] }
        elseif ($previous.ContainsKey($k)) { $newState[$k] = $previous[$k] }
    }
    Save-Manifest -state $newState -ManifestPath $ManifestPath
    Log 'Sync complete. Manifest updated.'
}

# -------- Boot: ensure S3 projects exist locally, hydrate serially, then watch & reconcile --------
# 1) Ensure any S3 projects exist locally
$s3Projects = Get-S3Projects -S3Path $S3Path -RootPrefix $S3RootPrefix
foreach ($p in $s3Projects) { [void](Ensure-LocalProjectFolder -ZRoot $ZDriveRoot -ProjectName $p) }

# 2A) Initial hydration (serial, per project; no uploads/deletes to S3)
$projects = Get-ChildItem $ZDriveRoot -Directory
foreach ($proj in $projects) {
    $ctx = Map-ProjectContext -ProjDir $proj
    try {
        TwoWaySync -LocalPath $ctx.LocalPath `
                   -S3Prefix $ctx.S3Prefix `
                   -ManifestPath $ctx.ManifestPath `
                   -S3Path $S3Path `
                   -ProjectId $ctx.ProjectId `
                   -ProjectName $ctx.ProjectName `
                   -HydrateOnly
    } catch { Log "Initial hydration failed for $($ctx.ProjectName): $_" }
}

# 2B) Start watchers after hydrate
$WatchedProjects = @{}
foreach ($proj in (Get-ChildItem $ZDriveRoot -Directory)) {
    $ctx = Map-ProjectContext -ProjDir $proj
    Start-FileWatcher -LocalPath $ctx.LocalPath -S3Prefix $ctx.S3Prefix -ManifestPath $ctx.ManifestPath -ProjectId $ctx.ProjectId -S3PathParam $S3Path -ProjectName $ctx.ProjectName
    $WatchedProjects[$ctx.ProjectName] = $true
}

# 2C) One normal reconciliation pass
foreach ($proj in (Get-ChildItem $ZDriveRoot -Directory)) {
    $ctx = Map-ProjectContext -ProjDir $proj
    try {
        TwoWaySync -LocalPath $ctx.LocalPath -S3Prefix $ctx.S3Prefix -ManifestPath $ctx.ManifestPath -S3Path $S3Path -ProjectId $ctx.ProjectId -ProjectName $ctx.ProjectName
    } catch { Log "Post-watch sync failed for $($ctx.ProjectName): $_" }
}

# 3) Polling loop — also handle projects added later (hydrate→watch→reconcile)
while ($true) {
    $s3Projects = Get-S3Projects -S3Path $S3Path -RootPrefix $S3RootPrefix
    foreach ($p in $s3Projects) { [void](Ensure-LocalProjectFolder -ZRoot $ZDriveRoot -ProjectName $p) }

    $projects = Get-ChildItem $ZDriveRoot -Directory
    foreach ($proj in $projects) {
        $ctx = Map-ProjectContext -ProjDir $proj
        try {
            if (-not $WatchedProjects.ContainsKey($ctx.ProjectName)) {
                # brand-new locally: hydrate first, then watch, then normal pass
                TwoWaySync -LocalPath $ctx.LocalPath -S3Prefix $ctx.S3Prefix -ManifestPath $ctx.ManifestPath -S3Path $S3Path -ProjectId $ctx.ProjectId -ProjectName $ctx.ProjectName -HydrateOnly
                Start-FileWatcher -LocalPath $ctx.LocalPath -S3Prefix $ctx.S3Prefix -ManifestPath $ctx.ManifestPath -ProjectId $ctx.ProjectId -S3PathParam $S3Path -ProjectName $ctx.ProjectName
                $WatchedProjects[$ctx.ProjectName] = $true
                TwoWaySync -LocalPath $ctx.LocalPath -S3Prefix $ctx.S3Prefix -ManifestPath $ctx.ManifestPath -S3Path $S3Path -ProjectId $ctx.ProjectId -ProjectName $ctx.ProjectName
            } else {
                TwoWaySync -LocalPath $ctx.LocalPath -S3Prefix $ctx.S3Prefix -ManifestPath $ctx.ManifestPath -S3Path $S3Path -ProjectId $ctx.ProjectId -ProjectName $ctx.ProjectName
            }
        } catch { Log "Polling sync failed for $($ctx.ProjectName): $_" }
    }

    Log 'Polling cycle completed. Sleeping 300s...'
    Start-Sleep -Seconds 300
}

# --- Optional (machine once): Enable OS long paths ---
# New-ItemProperty -Path HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem `
#   -Name LongPathsEnabled -PropertyType DWord -Value 1 -Force | Out-Null
