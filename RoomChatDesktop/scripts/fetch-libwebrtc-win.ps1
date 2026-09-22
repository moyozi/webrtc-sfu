#Requires -Version 5.1
<#
.SYNOPSIS
  手动获取 reactor-webrtc 的 Windows x64 预编译 libwebrtc。

.DESCRIPTION
  reactor-webrtc-sys 的构建脚本会自动从 GitHub Releases 下载预编译库。若网络
  无法直连 GitHub (例如国内), 构建脚本会静默退化为 "只检查不链接" 模式, 最终在
  link.exe 阶段报大量:

      unresolved external symbol reactor_webrtc_*

  本脚本负责手动 下载 + SHA256 校验 + 解压, 并打印需要设置的环境变量。

.PARAMETER Tag
  预编译 release tag。默认值对应 reactor-webrtc-sys 0.18.0 内置的 fallback tag。

.PARAMETER Mirror
  GitHub 镜像前缀, 例如 https://gh-proxy.com/ 或 https://ghfast.top/ 。
  留空则直连 GitHub。

.PARAMETER Dest
  解压目标目录 (需含 lib\ 与 include\)。

.PARAMETER Persist
  用 setx 写入用户级环境变量 REACTOR_WEBRTC_LIB_DIR (新终端生效)。

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts\fetch-libwebrtc-win.ps1 `
      -Mirror https://gh-proxy.com/ -Persist
#>
[CmdletBinding()]
param(
    [string]$Tag    = "webrtc-7907-a5ddff60-p9",
    [string]$Asset  = "reactor-webrtc-win-x64-release.tar.zst",
    [string]$Mirror = "",
    [string]$Dest   = (Join-Path $PSScriptRoot "..\third_party\libwebrtc-win-x64"),
    [switch]$Persist
)

$ErrorActionPreference = "Stop"

# GitHub 现在要求 TLS 1.2+; Windows PowerShell 5.1 默认可能没开。
[Net.ServicePointManager]::SecurityProtocol = `
    [Net.SecurityProtocolType]::Tls12 -bor [Net.SecurityProtocolType]::Tls11

$github = "https://github.com/reactor-team/reactor-webrtc/releases/download/$Tag/$Asset"
$url    = if ($Mirror) { "$Mirror$github" } else { $github }

$Dest = [System.IO.Path]::GetFullPath($Dest)
$tmp  = Join-Path $env:TEMP $Asset

Write-Host "==> 下载 $url" -ForegroundColor Cyan
Invoke-WebRequest -UseBasicParsing $url -OutFile $tmp

$expected = $null
try {
    $shaText = (Invoke-WebRequest -UseBasicParsing "$url.sha256").Content
    if ($shaText -is [byte[]]) { $shaText = [Text.Encoding]::UTF8.GetString($shaText) }
    $expected = ($shaText -split '\s+')[0].Trim().ToLower()
} catch {
    Write-Warning "无法获取校验和 ($url.sha256), 跳过 SHA256 校验"
}

if ($expected) {
    $actual = (Get-FileHash $tmp -Algorithm SHA256).Hash.ToLower()
    if ($actual -ne $expected) {
        throw "SHA256 不匹配: 期望 $expected, 实际 $actual"
    }
    Write-Host "==> SHA256 校验通过" -ForegroundColor Green
}

Write-Host "==> 解压到 $Dest"
Remove-Item -Recurse -Force $Dest -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path $Dest | Out-Null

$lib = Join-Path $Dest "lib\libwebrtc.lib"
$inc = Join-Path $Dest "include"

# Windows 自带的 bsdtar 通常支持 zstd; 不行再退回 7-Zip。
tar -xf $tmp -C $Dest
if (-not (Test-Path $lib)) {
    Write-Warning "tar 解压后未找到 $lib, 改用 7-Zip 重试…"
    $sevenZip = Get-Command 7z -ErrorAction SilentlyContinue
    if ($sevenZip) {
        Remove-Item -Recurse -Force $Dest -ErrorAction SilentlyContinue
        New-Item -ItemType Directory -Force $Dest | Out-Null
        & $sevenZip.Source x $tmp "-o$Dest" -y
    }
}

# 有些 tar 会多套一层目录, 自动纠正。
if (-not (Test-Path $lib)) {
    $hit = Get-ChildItem -Path $Dest -Recurse -Filter libwebrtc.lib -ErrorAction SilentlyContinue |
           Select-Object -First 1
    if ($hit) {
        $Dest = $hit.Directory.Parent.FullName
        $lib  = Join-Path $Dest "lib\libwebrtc.lib"
        $inc  = Join-Path $Dest "include"
        Write-Host "==> 库实际位于 $Dest"
    }
}

if (-not (Test-Path $lib)) {
    throw "解压失败: 未找到 libwebrtc.lib。请安装支持 .zst 的工具 (7-Zip 24.09+/WinRAR 6+)" +
          "手动解压 $tmp, 然后设置 REACTOR_WEBRTC_LIB_DIR。"
}
if (-not (Test-Path $inc)) {
    Write-Warning "未找到 $inc —— include 目录缺失, 编译 glue 会失败。"
}

Write-Host ""
Write-Host "完成。在当前终端执行:" -ForegroundColor Green
Write-Host "  `$env:REACTOR_WEBRTC_LIB_DIR = `"$Dest`""
Write-Host "  cargo clean -p reactor-webrtc-sys"
Write-Host "  cargo run --release"

if ($Persist) {
    setx REACTOR_WEBRTC_LIB_DIR "$Dest" | Out-Null
    Write-Host ""
    Write-Host "已写入用户环境变量 REACTOR_WEBRTC_LIB_DIR (新开的终端生效)。" -ForegroundColor Green
}
