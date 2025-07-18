﻿using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace LanSync
{
    public class LanSyncApp
    {
        private readonly string _folder;
        private readonly int _port;
        private readonly UdpClient _udp;
        private readonly TcpListener _tcp;
        private readonly ConcurrentDictionary<string, IPEndPoint> _peers = new();
        private readonly string _peerFile;
        private readonly string _stateFile;
        private readonly string _tmpFolder;
        private FileSystemWatcher _watcher;

        // Enhanced suppression for loop prevention with better tracking
        private readonly ConcurrentDictionary<string, DateTime> _recentlyReceived = new();
        private readonly ConcurrentDictionary<string, DateTime> _recentlySynced = new();
        private readonly ConcurrentDictionary<string, DateTime> _pendingOperations = new();

        // File state tracking with conflict resolution
        private readonly ConcurrentDictionary<string, FileMeta> _files = new();

        // Enhanced batching system - replaces simple debouncing
        private readonly ConcurrentDictionary<string, PendingFileOperation> _pendingFileOperations = new();
        private readonly Timer _batchProcessingTimer;
        private DateTime _lastFileActivity = DateTime.UtcNow;
        private readonly SemaphoreSlim _batchProcessingSemaphore = new(1, 1);

        // Connection pooling
        private readonly ConcurrentDictionary<IPEndPoint, SemaphoreSlim> _connectionSemaphores = new();

        // Full sync support
        private readonly SemaphoreSlim _fullSyncSemaphore = new(1, 1);
        private volatile bool _initialSyncCompleted = false;

        // Progress tracking
        private readonly SyncProgress _syncProgress = new();

        // State file access synchronization
        private readonly SemaphoreSlim _stateFileSemaphore = new(1, 1);

        // Protocol commands
        private const string FILE_LIST_REQUEST = "FILE_LIST_REQUEST";
        private const string FILE_LIST_RESPONSE = "FILE_LIST_RESPONSE";
        private const string REQUEST_FILE = "REQUEST";

        // Configuration constants
        private const int MaxConcurrentConnections = 3;
        private const int CleanupSeconds = 30;
        private const int SuppressionSeconds = 5;
        private const int FileEventDebounceMs = 200; // Reduced for individual file debouncing
        private const int BatchQuietPeriodMs = 3000; // Configurable quiet period (3 seconds)
        private const int BatchProcessingIntervalMs = 500; // Check for batches every 500ms
        private const int FileStabilitySleepMs = 500;
        private const int MaxRetryAttempts = 3;
        private const int RetryDelayMs = 1000;

        // Shutdown coordination
        private readonly CancellationTokenSource _shutdownToken = new();

        // Enhanced pending operation tracking
        private class PendingFileOperation
        {
            public string FilePath { get; set; }
            public string OperationType { get; set; } // "Changed", "Created", "Deleted", "Renamed"
            public DateTime LastActivity { get; set; }
            public string OldPath { get; set; } // For rename operations
            public bool IsProcessed { get; set; }
        }

        public LanSyncApp(string folder, int port)
        {
            _folder = folder;
            _port = port;

            _udp = new UdpClient(port);
            _udp.EnableBroadcast = true;
            _tcp = new TcpListener(IPAddress.Any, port);

            var appDataFolder = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                "LanSync");
            Directory.CreateDirectory(appDataFolder);
            _peerFile = Path.Combine(appDataFolder, "peers.json");
            _stateFile = Path.Combine(appDataFolder, "file_state.json");

            _tmpFolder = Path.Combine(_folder, ".tmp");
            Directory.CreateDirectory(_tmpFolder);

            // Initialize batch processing timer
            _batchProcessingTimer = new Timer(async (obj) => await ProcessPendingBatch(obj), null,
                TimeSpan.FromMilliseconds(BatchProcessingIntervalMs),
                TimeSpan.FromMilliseconds(BatchProcessingIntervalMs));
        }

        public async Task RunAsync()
        {
            try
            {
                LoadPeers();
                LoadFileStates();

                // Initialize connection semaphores for existing peers
                foreach (var peer in _peers.Values)
                {
                    _connectionSemaphores[peer] = new SemaphoreSlim(MaxConcurrentConnections);
                }

                // Start background tasks
                var tasks = new[]
                {
                    Task.Run(() => SuppressionCleanupTask(_shutdownToken.Token)),
                    Task.Run(() => ReceiveBroadcasts(_shutdownToken.Token)),
                    Task.Run(() => BroadcastPresence(_shutdownToken.Token)),
                    Task.Run(() => AcceptIncoming(_shutdownToken.Token)),
                    Task.Run(() => PeriodicStateSync(_shutdownToken.Token)),
                    Task.Run(() => PeriodicFullSync(_shutdownToken.Token))
                };

                // Start file watcher
                StartFileWatcher();

                Console.WriteLine($"[INFO] Watching folder (recursive): {_folder}");
                Console.WriteLine($"[INFO] Batch processing: {BatchQuietPeriodMs}ms quiet period, {BatchProcessingIntervalMs}ms check interval");
                Console.WriteLine("[INFO] LAN Sync running. Press Ctrl+C to exit.");

                // Perform initial full sync after a short delay
                _ = Task.Run(async () =>
                {
                    await Task.Delay(3000); // Wait for peer discovery
                    await PerformFullSyncWithPeers();
                });

                // Wait for shutdown signal
                await Task.Delay(-1, _shutdownToken.Token);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("[INFO] Shutdown requested.");
            }
            finally
            {
                await Shutdown();
            }
        }

        // Enhanced batch processing system
        private async Task ProcessPendingBatch(object state)
        {
            if (_shutdownToken.Token.IsCancellationRequested) return;

            try
            {
                var now = DateTime.UtcNow;
                var quietPeriodElapsed = (now - _lastFileActivity).TotalMilliseconds >= BatchQuietPeriodMs;

                if (!quietPeriodElapsed || _pendingFileOperations.IsEmpty)
                    return;

                // Check if we can acquire the semaphore without blocking
                if (!await _batchProcessingSemaphore.WaitAsync(10))
                    return;

                try
                {
                    var pendingOperations = _pendingFileOperations.Values
                        .Where(op => !op.IsProcessed && (now - op.LastActivity).TotalMilliseconds >= BatchQuietPeriodMs)
                        .OrderBy(op => op.LastActivity)
                        .ToList();

                    if (pendingOperations.Count == 0)
                        return;

                    Console.WriteLine($"[BATCH] Processing {pendingOperations.Count} pending file operations after quiet period");

                    // Mark operations as being processed
                    foreach (var op in pendingOperations)
                        op.IsProcessed = true;

                    // Group operations by type for efficient processing
                    var changes = pendingOperations.Where(op => op.OperationType == "Changed" || op.OperationType == "Created").ToList();
                    var deletions = pendingOperations.Where(op => op.OperationType == "Deleted").ToList();
                    var renames = pendingOperations.Where(op => op.OperationType == "Renamed").ToList();

                    // Process different operation types
                    await ProcessBatchedChanges(changes);
                    await ProcessBatchedDeletions(deletions);
                    await ProcessBatchedRenames(renames);

                    // Clean up processed operations
                    foreach (var op in pendingOperations)
                    {
                        _pendingFileOperations.TryRemove(op.FilePath, out _);
                    }

                    // Save state once after processing the entire batch
                    await SaveFileStatesAsync();

                    Console.WriteLine($"[BATCH] Completed processing {pendingOperations.Count} operations");
                }
                finally
                {
                    _batchProcessingSemaphore.Release();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] ProcessPendingBatch: {ex.Message}");
            }
        }

        private async Task ProcessBatchedChanges(List<PendingFileOperation> changes)
        {
            if (changes.Count == 0) return;

            Console.WriteLine($"[BATCH] Processing {changes.Count} file changes/creations");

            var validChanges = new List<(string relPath, string fullPath, FileMeta meta)>();

            // First pass: validate files and compute metadata
            foreach (var change in changes)
            {
                try
                {
                    var relPath = GetRelativePath(change.FilePath);
                    if (relPath == null || IsInTmpFolder(change.FilePath) || !File.Exists(change.FilePath))
                        continue;

                    if (IsRecentlyProcessed(relPath))
                    {
                        Console.WriteLine($"[SKIP] Not syncing {relPath} (recently processed).");
                        continue;
                    }

                    if (!await WaitForFileStability(change.FilePath))
                    {
                        Console.WriteLine($"[SKIP] File {relPath} is locked or unstable.");
                        continue;
                    }

                    // Mark as pending to prevent concurrent processing
                    _pendingOperations[relPath] = DateTime.UtcNow;

                    // Compute file metadata
                    var hash = ComputeFileHash(change.FilePath);
                    var lastWrite = File.GetLastWriteTimeUtc(change.FilePath);
                    var fileSize = new FileInfo(change.FilePath).Length;

                    // Check for conflicts with existing file state
                    if (_files.TryGetValue(relPath, out var existingMeta))
                    {
                        if (existingMeta.Hash == hash)
                        {
                            Console.WriteLine($"[SKIP] File {relPath} unchanged (same hash).");
                            _pendingOperations.TryRemove(relPath, out _);
                            continue;
                        }

                        // Conflict resolution: use last write time
                        if (lastWrite <= existingMeta.LastWriteUtc.AddSeconds(1)) // 1 second tolerance
                        {
                            Console.WriteLine($"[SKIP] File {relPath} is older or same age as existing version.");
                            _pendingOperations.TryRemove(relPath, out _);
                            continue;
                        }
                    }

                    var meta = new FileMeta
                    {
                        Hash = hash,
                        LastWriteUtc = lastWrite,
                        Size = fileSize,
                        Version = Guid.NewGuid().ToString(),
                        LastModifiedBy = GetLocalIPAddress()
                    };

                    _files[relPath] = meta;
                    validChanges.Add((relPath, change.FilePath, meta));

                    Console.WriteLine($"[BATCH] Prepared {relPath} for sync (Size: {fileSize}, Hash: {hash[..8]}...)");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERR ] ProcessBatchedChanges prep {change.FilePath}: {ex.Message}");
                }
            }

            if (validChanges.Count == 0)
                return;

            // Second pass: sync files to peers with parallel processing
            Console.WriteLine($"[BATCH] Syncing {validChanges.Count} files to peers");

            var syncTasks = validChanges.Select(async change =>
            {
                try
                {
                    await SyncFileToPeers(change.relPath, change.fullPath);
                    _recentlySynced[change.relPath] = DateTime.UtcNow;
                    Console.WriteLine($"[BATCH] Synced {change.relPath}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERR ] Batch sync {change.relPath}: {ex.Message}");
                }
                finally
                {
                    _pendingOperations.TryRemove(change.relPath, out _);
                }
            });

            await Task.WhenAll(syncTasks);
        }

        private async Task ProcessBatchedDeletions(List<PendingFileOperation> deletions)
        {
            if (deletions.Count == 0) return;

            Console.WriteLine($"[BATCH] Processing {deletions.Count} file deletions");

            var deletionTasks = deletions.Select(async deletion =>
            {
                try
                {
                    var relPath = GetRelativePath(deletion.FilePath);
                    if (relPath == null || IsInTmpFolder(deletion.FilePath))
                        return;

                    _pendingOperations[relPath] = DateTime.UtcNow;

                    _files.TryRemove(relPath, out _);
                    Console.WriteLine($"[BATCH] Deleted {relPath}");

                    await SyncDeleteToPeers(relPath);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERR ] ProcessBatchedDeletions {deletion.FilePath}: {ex.Message}");
                }
                finally
                {
                    var relPath = GetRelativePath(deletion.FilePath);
                    if (relPath != null)
                        _pendingOperations.TryRemove(relPath, out _);
                }
            });

            await Task.WhenAll(deletionTasks);
        }

        private async Task ProcessBatchedRenames(List<PendingFileOperation> renames)
        {
            if (renames.Count == 0) return;

            Console.WriteLine($"[BATCH] Processing {renames.Count} file renames");

            var renameTasks = renames.Select(async rename =>
            {
                try
                {
                    if (IsInTmpFolder(rename.FilePath)) return;

                    var oldRel = GetRelativePath(rename.OldPath);
                    var newRel = GetRelativePath(rename.FilePath);
                    if (oldRel == null || newRel == null) return;

                    // Update file tracking
                    if (_files.TryRemove(oldRel, out var meta) && File.Exists(rename.FilePath))
                    {
                        // Update metadata for new path
                        var hash = ComputeFileHash(rename.FilePath);
                        var lastWrite = File.GetLastWriteTimeUtc(rename.FilePath);
                        var fileSize = new FileInfo(rename.FilePath).Length;

                        _files[newRel] = new FileMeta
                        {
                            Hash = hash,
                            LastWriteUtc = lastWrite,
                            Size = fileSize,
                            Version = Guid.NewGuid().ToString(),
                            LastModifiedBy = GetLocalIPAddress()
                        };
                    }

                    Console.WriteLine($"[BATCH] Renamed {oldRel} -> {newRel}");

                    await SyncRenameToPeers(oldRel, newRel);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERR ] ProcessBatchedRenames {rename.FilePath}: {ex.Message}");
                }
            });

            await Task.WhenAll(renameTasks);
        }

        // Enhanced file watcher with batching
        private void StartFileWatcher()
        {
            _watcher = new FileSystemWatcher(_folder)
            {
                NotifyFilter = NotifyFilters.FileName | NotifyFilters.Size | NotifyFilters.LastWrite | NotifyFilters.CreationTime,
                IncludeSubdirectories = true,
                EnableRaisingEvents = true
            };

            _watcher.Created += (s, e) => ScheduleFileOperation(e.FullPath, "Created");
            _watcher.Changed += (s, e) => ScheduleFileOperation(e.FullPath, "Changed");
            _watcher.Deleted += (s, e) => ScheduleFileOperation(e.FullPath, "Deleted");
            _watcher.Renamed += (s, e) => ScheduleRenameOperation(e.OldFullPath, e.FullPath);
        }

        private void ScheduleFileOperation(string path, string operationType)
        {
            if (IsInTmpFolder(path)) return;

            var relPath = GetRelativePath(path);
            if (relPath == null) return;

            _lastFileActivity = DateTime.UtcNow;

            var operation = new PendingFileOperation
            {
                FilePath = path,
                OperationType = operationType,
                LastActivity = DateTime.UtcNow,
                IsProcessed = false
            };

            // Update or add the pending operation
            _pendingFileOperations.AddOrUpdate(relPath, operation, (key, existing) =>
            {
                // If there's already a pending operation, update it with the latest activity
                existing.LastActivity = DateTime.UtcNow;
                existing.OperationType = operationType; // Latest operation takes precedence
                existing.IsProcessed = false;
                return existing;
            });

            Console.WriteLine($"[QUEUE] {operationType}: {relPath} (queued for batch processing)");
        }

        private void ScheduleRenameOperation(string oldPath, string newPath)
        {
            if (IsInTmpFolder(newPath)) return;

            var oldRel = GetRelativePath(oldPath);
            var newRel = GetRelativePath(newPath);
            if (oldRel == null || newRel == null) return;

            _lastFileActivity = DateTime.UtcNow;

            // Remove any existing operations for both paths
            _pendingFileOperations.TryRemove(oldRel, out _);
            _pendingFileOperations.TryRemove(newRel, out _);

            var operation = new PendingFileOperation
            {
                FilePath = newPath,
                OldPath = oldPath,
                OperationType = "Renamed",
                LastActivity = DateTime.UtcNow,
                IsProcessed = false
            };

            _pendingFileOperations[newRel] = operation;

            Console.WriteLine($"[QUEUE] Renamed: {oldRel} -> {newRel} (queued for batch processing)");
        }

        // Thread-safe state file operations
        private async Task SaveFileStatesAsync()
        {
            await _stateFileSemaphore.WaitAsync();
            try
            {
                var dic = _files.ToDictionary(x => x.Key, x => x.Value);
                var json = JsonSerializer.Serialize(dic, new JsonSerializerOptions { WriteIndented = true });

                // Use a temporary file and atomic move to prevent corruption
                var tempFile = _stateFile + ".tmp";
                await File.WriteAllTextAsync(tempFile, json);

                if (File.Exists(_stateFile))
                    File.Delete(_stateFile);

                File.Move(tempFile, _stateFile);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] SaveFileStatesAsync: {ex.Message}");
            }
            finally
            {
                _stateFileSemaphore.Release();
            }
        }

        // Progress tracking class
        private class SyncProgress
        {
            private int _totalFiles = 0;
            private int _processedFiles = 0;
            private int _successfulFiles = 0;
            private int _failedFiles = 0;
            private readonly object _lock = new object();

            public void Reset(int totalFiles)
            {
                lock (_lock)
                {
                    _totalFiles = totalFiles;
                    _processedFiles = 0;
                    _successfulFiles = 0;
                    _failedFiles = 0;
                }
            }

            public void RecordSuccess()
            {
                lock (_lock)
                {
                    _processedFiles++;
                    _successfulFiles++;
                }
            }

            public void RecordFailure()
            {
                lock (_lock)
                {
                    _processedFiles++;
                    _failedFiles++;
                }
            }

            public (int total, int processed, int successful, int failed, double percentage) GetStatus()
            {
                lock (_lock)
                {
                    var percentage = _totalFiles > 0 ? (double)_processedFiles / _totalFiles * 100 : 0;
                    return (_totalFiles, _processedFiles, _successfulFiles, _failedFiles, percentage);
                }
            }

            public bool IsCompleted()
            {
                lock (_lock)
                {
                    return _processedFiles >= _totalFiles && _totalFiles > 0;
                }
            }
        }

        // Enhanced FileMeta class
        private class FileMeta
        {
            public string Hash { get; set; }
            public DateTime LastWriteUtc { get; set; }
            public long Size { get; set; }
            public string Version { get; set; } = Guid.NewGuid().ToString();
            public string LastModifiedBy { get; set; }
        }

        // Full sync implementation
        public async Task PerformFullSyncWithPeers()
        {
            if (!await _fullSyncSemaphore.WaitAsync(1000))
            {
                Console.WriteLine("[SYNC] Full sync already in progress, skipping...");
                return;
            }

            try
            {
                Console.WriteLine("[SYNC] ═══════════════════════════════════════");
                Console.WriteLine("[SYNC] Starting full synchronization...");
                Console.WriteLine("[SYNC] ═══════════════════════════════════════");

                // Phase 1: Scan local directory
                Console.WriteLine("[SYNC] Phase 1: Scanning local directory...");
                await ScanLocalDirectory();

                if (_peers.Count == 0)
                {
                    Console.WriteLine("[SYNC] No peers available for synchronization.");
                    return;
                }

                // Phase 2: Request file lists from all peers
                Console.WriteLine($"[SYNC] Phase 2: Requesting file lists from {_peers.Count} peer(s)...");
                var peerFileLists = new ConcurrentDictionary<IPEndPoint, Dictionary<string, FileMeta>>();

                var peerTasks = _peers.Values.Select(async peer =>
                {
                    try
                    {
                        var fileList = await RequestPeerFileList(peer);
                        if (fileList != null)
                        {
                            peerFileLists[peer] = fileList;
                            Console.WriteLine($"[SYNC] Received {fileList.Count} file entries from {peer}");
                        }
                        else
                        {
                            Console.WriteLine($"[SYNC] Failed to get file list from {peer}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[ERR ] Failed to get file list from {peer}: {ex.Message}");
                    }
                });

                await Task.WhenAll(peerTasks);

                // Phase 3: Perform three-way merge
                Console.WriteLine("[SYNC] Phase 3: Analyzing file differences...");
                await PerformThreeWaySync(peerFileLists);

                _initialSyncCompleted = true;
                Console.WriteLine("[SYNC] ═══════════════════════════════════════");
                Console.WriteLine("[SYNC] Full synchronization completed!");
                Console.WriteLine("[SYNC] ═══════════════════════════════════════");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] Full sync failed: {ex.Message}");
            }
            finally
            {
                _fullSyncSemaphore.Release();
            }
        }

        private async Task ScanLocalDirectory()
        {
            var localFiles = new HashSet<string>();
            await ScanDirectoryRecursive(_folder, localFiles);

            // Remove deleted files from tracking
            var trackedFiles = _files.Keys.ToList();
            var removedCount = 0;

            foreach (var trackedFile in trackedFiles)
            {
                if (!localFiles.Contains(trackedFile))
                {
                    _files.TryRemove(trackedFile, out _);
                    removedCount++;
                }
            }

            Console.WriteLine($"[SCAN] Found {localFiles.Count} files, removed {removedCount} deleted files from tracking");
            await SaveFileStatesAsync();
        }

        private async Task ScanDirectoryRecursive(string directory, HashSet<string> foundFiles)
        {
            try
            {
                // Process files
                foreach (var file in Directory.GetFiles(directory))
                {
                    if (IsInTmpFolder(file)) continue;

                    var relPath = GetRelativePath(file);
                    if (relPath == null) continue;

                    foundFiles.Add(relPath);

                    // Check if file changed since last scan
                    var hash = ComputeFileHash(file);
                    var lastWrite = File.GetLastWriteTimeUtc(file);
                    var size = new FileInfo(file).Length;

                    if (!_files.TryGetValue(relPath, out var existing) ||
                        existing.Hash != hash ||
                        existing.LastWriteUtc != lastWrite ||
                        existing.Size != size)
                    {
                        _files[relPath] = new FileMeta
                        {
                            Hash = hash,
                            LastWriteUtc = lastWrite,
                            Size = size,
                            Version = Guid.NewGuid().ToString(),
                            LastModifiedBy = GetLocalIPAddress()
                        };
                    }
                }

                // Process subdirectories
                foreach (var subDir in Directory.GetDirectories(directory))
                {
                    if (Path.GetFileName(subDir) != ".tmp")
                        await ScanDirectoryRecursive(subDir, foundFiles);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] ScanDirectoryRecursive: {ex.Message}");
            }
        }

        private async Task<Dictionary<string, FileMeta>> RequestPeerFileList(IPEndPoint peer)
        {
            var semaphore = GetOrCreateSemaphore(peer);
            await semaphore.WaitAsync();

            try
            {
                using var client = new TcpClient();
                client.ReceiveTimeout = 30000;
                client.SendTimeout = 30000;

                await client.ConnectAsync(peer.Address, _port);
                using var stream = client.GetStream();

                // Send file list request
                var msg = Encoding.UTF8.GetBytes($"{FILE_LIST_REQUEST}\n");
                await stream.WriteAsync(msg, 0, msg.Length);

                // Read response
                var response = await ReadLineAsync(stream);
                if (response?.StartsWith(FILE_LIST_RESPONSE) == true)
                {
                    var parts = response.Split(':');
                    if (parts.Length >= 2 && int.TryParse(parts[1], out var jsonLength))
                    {
                        var jsonBytes = new byte[jsonLength];

                        int totalRead = 0;
                        while (totalRead < jsonLength)
                        {
                            int bytesRead = await stream.ReadAsync(jsonBytes, totalRead, jsonLength - totalRead);
                            if (bytesRead == 0) break;
                            totalRead += bytesRead;
                        }

                        var json = Encoding.UTF8.GetString(jsonBytes);
                        return JsonSerializer.Deserialize<Dictionary<string, FileMeta>>(json);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] RequestPeerFileList from {peer}: {ex.Message}");
            }
            finally
            {
                semaphore.Release();
            }

            return null;
        }

        private async Task PerformThreeWaySync(ConcurrentDictionary<IPEndPoint, Dictionary<string, FileMeta>> peerFileLists)
        {
            var allFiles = new HashSet<string>();

            // Collect all unique file paths
            allFiles.UnionWith(_files.Keys);
            foreach (var peerFiles in peerFileLists.Values)
                allFiles.UnionWith(peerFiles.Keys);

            if (allFiles.Count == 0)
            {
                Console.WriteLine("[SYNC] No files to synchronize.");
                return;
            }

            // Initialize progress tracking
            _syncProgress.Reset(allFiles.Count);

            Console.WriteLine($"[SYNC] Found {allFiles.Count} unique files across all peers");
            Console.WriteLine("[SYNC] Starting file synchronization...");

            // Progress reporting task
            var progressTask = Task.Run(async () =>
            {
                while (!_syncProgress.IsCompleted() && !_shutdownToken.Token.IsCancellationRequested)
                {
                    var (total, processed, successful, failed, percentage) = _syncProgress.GetStatus();
                    if (total > 0)
                    {
                        Console.WriteLine($"[PROGRESS] {processed}/{total} files processed ({percentage:F1}%) - Success: {successful}, Failed: {failed}");
                    }
                    await Task.Delay(2000, _shutdownToken.Token);
                }
            });

            // Process files with controlled parallelism
            var semaphore = new SemaphoreSlim(Environment.ProcessorCount);
            var syncTasks = allFiles.Select(async filePath =>
            {
                await semaphore.WaitAsync();
                try
                {
                    await SyncSingleFile(filePath, peerFileLists);
                }
                finally
                {
                    semaphore.Release();
                }
            });

            await Task.WhenAll(syncTasks);

            // Final progress report
            var (finalTotal, finalProcessed, finalSuccessful, finalFailed, finalPercentage) = _syncProgress.GetStatus();
            Console.WriteLine($"[SYNC] Synchronization complete: {finalProcessed}/{finalTotal} files processed");
            Console.WriteLine($"[SYNC] Results: {finalSuccessful} successful, {finalFailed} failed");
        }

        private async Task SyncSingleFile(string filePath, ConcurrentDictionary<IPEndPoint, Dictionary<string, FileMeta>> peerFileLists)
        {
            try
            {
                var localFile = _files.TryGetValue(filePath, out var local) ? local : null;
                var localPath = Path.Combine(_folder, filePath);
                var localExists = File.Exists(localPath);

                // Find the most recent version across all peers
                FileMeta mostRecentVersion = localFile;
                IPEndPoint sourcePeer = null;

                foreach (var (peer, files) in peerFileLists)
                {
                    if (files.TryGetValue(filePath, out var peerFile))
                    {
                        if (mostRecentVersion == null ||
                            peerFile.LastWriteUtc > mostRecentVersion.LastWriteUtc ||
                            (peerFile.LastWriteUtc == mostRecentVersion.LastWriteUtc &&
                             string.Compare(peerFile.Version, mostRecentVersion.Version, StringComparison.Ordinal) > 0))
                        {
                            mostRecentVersion = peerFile;
                            sourcePeer = peer;
                        }
                    }
                }

                // Determine action needed
                if (mostRecentVersion == null)
                {
                    // File doesn't exist anywhere
                    _syncProgress.RecordSuccess();
                    return;
                }

                if (localFile == null && !localExists)
                {
                    // File doesn't exist locally, download from peer
                    if (sourcePeer != null)
                    {
                        await RequestFileFromPeer(sourcePeer, filePath);
                        Console.WriteLine($"[SYNC] Downloaded missing file: {filePath}");
                        _syncProgress.RecordSuccess();
                    }
                    else
                    {
                        _syncProgress.RecordSuccess();
                    }
                }
                else if (localFile != null && localExists)
                {
                    // File exists locally, check if we need to update
                    if (mostRecentVersion.Hash != localFile.Hash)
                    {
                        if (sourcePeer != null && mostRecentVersion.LastWriteUtc > localFile.LastWriteUtc)
                        {
                            await RequestFileFromPeer(sourcePeer, filePath);
                            Console.WriteLine($"[SYNC] Updated outdated file: {filePath}");
                            _syncProgress.RecordSuccess();
                        }
                        else if (sourcePeer == null)
                        {
                            // We have the most recent version, send to peers
                            await SyncFileToPeers(filePath, localPath);
                            Console.WriteLine($"[SYNC] Sent newer file to peers: {filePath}");
                            _syncProgress.RecordSuccess();
                        }
                        else
                        {
                            _syncProgress.RecordSuccess();
                        }
                    }
                    else
                    {
                        _syncProgress.RecordSuccess();
                    }
                }
                else if (localFile != null && !localExists)
                {
                    // File tracked but missing locally
                    if (sourcePeer != null)
                    {
                        await RequestFileFromPeer(sourcePeer, filePath);
                        Console.WriteLine($"[SYNC] Restored missing file: {filePath}");
                        _syncProgress.RecordSuccess();
                    }
                    else
                    {
                        // File doesn't exist anywhere, remove from tracking
                        _files.TryRemove(filePath, out _);
                        _syncProgress.RecordSuccess();
                    }
                }
                else
                {
                    _syncProgress.RecordSuccess();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] SyncSingleFile {filePath}: {ex.Message}");
                _syncProgress.RecordFailure();
            }
        }

        private async Task RequestFileFromPeer(IPEndPoint peer, string filePath)
        {
            var semaphore = GetOrCreateSemaphore(peer);
            await semaphore.WaitAsync();

            try
            {
                using var client = new TcpClient();
                client.ReceiveTimeout = 60000;
                client.SendTimeout = 30000;

                await client.ConnectAsync(peer.Address, _port);
                using var stream = client.GetStream();

                var msg = Encoding.UTF8.GetBytes($"{REQUEST_FILE}:{filePath}\n");
                await stream.WriteAsync(msg, 0, msg.Length);

                // Handle the file reception
                var absPath = Path.Combine(_folder, filePath);
                await ReceiveFileAsync(stream, absPath, filePath);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] RequestFileFromPeer {filePath} from {peer}: {ex.Message}");
                throw;
            }
            finally
            {
                semaphore.Release();
            }
        }

        // Periodic full sync task
        private async Task PeriodicFullSync(CancellationToken cancellationToken)
        {
            // Wait for initial sync to complete
            while (!_initialSyncCompleted && !cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(5000, cancellationToken);
            }

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromHours(1), cancellationToken); // Full sync every hour
                    Console.WriteLine("[SYNC] Starting scheduled full synchronization...");
                    await PerformFullSyncWithPeers();
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERR ] PeriodicFullSync: {ex.Message}");
                }
            }
        }

        // Enhanced suppression cleanup with better logic
        private async Task SuppressionCleanupTask(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var now = DateTime.UtcNow;
                    CleanOld(_recentlyReceived, now, SuppressionSeconds);
                    CleanOld(_recentlySynced, now, SuppressionSeconds);
                    CleanOld(_pendingOperations, now, CleanupSeconds);
                    await Task.Delay(CleanupSeconds * 1000, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        private void CleanOld(ConcurrentDictionary<string, DateTime> dict, DateTime now, int maxAgeSeconds)
        {
            var keysToRemove = new List<string>();
            foreach (var kv in dict)
            {
                if ((now - kv.Value).TotalSeconds > maxAgeSeconds)
                    keysToRemove.Add(kv.Key);
            }
            foreach (var key in keysToRemove)
                dict.TryRemove(key, out _);
        }

        // Enhanced peer synchronization with retry logic
        private async Task SyncFileToPeers(string relPath, string path)
        {
            if (_peers.Count == 0) return;

            var tasks = _peers.Values.Select(peer => SyncFileToPeer(peer, relPath, path)).ToArray();
            await Task.WhenAll(tasks);
        }

        private async Task SyncFileToPeer(IPEndPoint peer, string relPath, string path)
        {
            var semaphore = GetOrCreateSemaphore(peer);
            await semaphore.WaitAsync();

            try
            {
                for (int attempt = 1; attempt <= MaxRetryAttempts; attempt++)
                {
                    try
                    {
                        using var client = new TcpClient();
                        client.ReceiveTimeout = 30000;
                        client.SendTimeout = 30000;

                        await client.ConnectAsync(peer.Address, _port);
                        using var stream = client.GetStream();

                        var msg = Encoding.UTF8.GetBytes("SEND:" + relPath + "\n");
                        await stream.WriteAsync(msg, 0, msg.Length);
                        await SendFileAsync(stream, path);

                        Console.WriteLine($"[SYNC] Sent {relPath} to {peer} (attempt {attempt})");
                        return; // Success
                    }
                    catch (Exception ex) when (attempt < MaxRetryAttempts)
                    {
                        Console.WriteLine($"[WARN] Failed to send {relPath} to {peer} (attempt {attempt}): {ex.Message}");
                        await Task.Delay(RetryDelayMs * attempt);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[ERR ] Failed to send {relPath} to {peer} after {MaxRetryAttempts} attempts: {ex.Message}");
                    }
                }
            }
            finally
            {
                semaphore.Release();
            }
        }

        private async Task SyncDeleteToPeers(string relPath)
        {
            if (_peers.Count == 0) return;

            var tasks = _peers.Values.Select(peer => SyncDeleteToPeer(peer, relPath)).ToArray();
            await Task.WhenAll(tasks);
        }

        private async Task SyncDeleteToPeer(IPEndPoint peer, string relPath)
        {
            var semaphore = GetOrCreateSemaphore(peer);
            await semaphore.WaitAsync();

            try
            {
                using var client = new TcpClient();
                client.ReceiveTimeout = 10000;
                client.SendTimeout = 10000;

                await client.ConnectAsync(peer.Address, _port);
                using var stream = client.GetStream();

                var msg = Encoding.UTF8.GetBytes("DELETE:" + relPath + "\n");
                await stream.WriteAsync(msg, 0, msg.Length);

                Console.WriteLine($"[SYNC] Delete {relPath} sent to {peer}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] Failed to send delete to {peer}: {ex.Message}");
            }
            finally
            {
                semaphore.Release();
            }
        }

        private async Task SyncRenameToPeers(string oldRel, string newRel)
        {
            if (_peers.Count == 0) return;

            var tasks = _peers.Values.Select(peer => SyncRenameToPeer(peer, oldRel, newRel)).ToArray();
            await Task.WhenAll(tasks);
        }

        private async Task SyncRenameToPeer(IPEndPoint peer, string oldRel, string newRel)
        {
            var semaphore = GetOrCreateSemaphore(peer);
            await semaphore.WaitAsync();

            try
            {
                using var client = new TcpClient();
                client.ReceiveTimeout = 10000;
                client.SendTimeout = 10000;

                await client.ConnectAsync(peer.Address, _port);
                using var stream = client.GetStream();

                var msg = Encoding.UTF8.GetBytes($"RENAME:{oldRel}|{newRel}\n");
                await stream.WriteAsync(msg, 0, msg.Length);

                Console.WriteLine($"[SYNC] Rename {oldRel} -> {newRel} sent to {peer}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] Failed to send rename to {peer}: {ex.Message}");
            }
            finally
            {
                semaphore.Release();
            }
        }

        // Enhanced networking with better error handling
        private async Task BroadcastPresence(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var localIp = GetLocalIPAddress();
                    var msg = Encoding.UTF8.GetBytes($"LAN_SYNC:{localIp}:{_port}");
                    await _udp.SendAsync(msg, msg.Length, new IPEndPoint(IPAddress.Broadcast, _port));
                    await Task.Delay(5000, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERR ] BroadcastPresence: {ex.Message}");
                    await Task.Delay(1000, cancellationToken);
                }
            }
        }

        private async Task ReceiveBroadcasts(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var result = await _udp.ReceiveAsync();
                    var msg = Encoding.UTF8.GetString(result.Buffer);

                    if (msg.StartsWith("LAN_SYNC:"))
                    {
                        var parts = msg.Split(":");
                        if (parts.Length == 3 && int.TryParse(parts[2], out var port))
                        {
                            var ip = parts[1];
                            var localIp = GetLocalIPAddress();

                            if (ip != localIp)
                            {
                                var endpoint = new IPEndPoint(IPAddress.Parse(ip), port);
                                bool isNewPeer = !_peers.ContainsKey(ip);

                                _peers[ip] = endpoint;

                                if (isNewPeer)
                                {
                                    _connectionSemaphores[endpoint] = new SemaphoreSlim(MaxConcurrentConnections);
                                    await SavePeersAsync();
                                    Console.WriteLine($"[DISC] Discovered new peer: {ip}:{port}");

                                    // Trigger full sync when new peer is discovered
                                    _ = Task.Run(async () =>
                                    {
                                        await Task.Delay(2000); // Small delay to let peer stabilize
                                        await PerformFullSyncWithPeers();
                                    });
                                }
                            }
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERR ] ReceiveBroadcasts: {ex.Message}");
                    await Task.Delay(1000, cancellationToken);
                }
            }
        }

        private async Task AcceptIncoming(CancellationToken cancellationToken)
        {
            _tcp.Start();
            Console.WriteLine($"[INFO] TCP server listening on port {_port} for incoming syncs.");

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var client = await _tcp.AcceptTcpClientAsync();
                    _ = Task.Run(() => HandleClient(client, cancellationToken));
                }
                catch (ObjectDisposedException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERR ] AcceptIncoming: {ex.Message}");
                    await Task.Delay(1000, cancellationToken);
                }
            }
        }

        private async Task HandleClient(TcpClient client, CancellationToken cancellationToken)
        {
            try
            {
                client.ReceiveTimeout = 30000;
                client.SendTimeout = 30000;

                using var stream = client.GetStream();
                string line = await ReadLineAsync(stream);
                if (line == null || cancellationToken.IsCancellationRequested) return;

                if (line.StartsWith("SEND:"))
                {
                    var relPath = line.Substring(5);
                    var absPath = Path.Combine(_folder, relPath);
                    if (!IsInTmpFolder(absPath))
                    {
                        await ReceiveFileAsync(stream, absPath, relPath);
                    }
                }
                else if (line.StartsWith("DELETE:"))
                {
                    var relPath = line.Substring(7);
                    await HandleRemoteDelete(relPath);
                }
                else if (line.StartsWith("RENAME:"))
                {
                    var parts = line.Substring(7).Split('|');
                    if (parts.Length == 2)
                    {
                        await HandleRemoteRename(parts[0], parts[1]);
                    }
                }
                else if (line.StartsWith(FILE_LIST_REQUEST))
                {
                    await HandleFileListRequest(stream);
                }
                else if (line.StartsWith($"{REQUEST_FILE}:"))
                {
                    var relPath = line.Substring(REQUEST_FILE.Length + 1);
                    await HandleFileRequest(stream, relPath);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] HandleClient: {ex.Message}");
            }
            finally
            {
                try { client.Close(); } catch { }
            }
        }

        private async Task HandleFileListRequest(NetworkStream stream)
        {
            try
            {
                var fileList = _files.ToDictionary(x => x.Key, x => x.Value);
                var json = JsonSerializer.Serialize(fileList);
                var jsonBytes = Encoding.UTF8.GetBytes(json);

                var response = Encoding.UTF8.GetBytes($"{FILE_LIST_RESPONSE}:{jsonBytes.Length}\n");
                await stream.WriteAsync(response, 0, response.Length);
                await stream.WriteAsync(jsonBytes, 0, jsonBytes.Length);

                Console.WriteLine($"[RESP] Sent file list with {fileList.Count} entries");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] HandleFileListRequest: {ex.Message}");
            }
        }

        private async Task HandleFileRequest(NetworkStream stream, string relPath)
        {
            try
            {
                var absPath = Path.Combine(_folder, relPath);
                if (File.Exists(absPath) && !IsInTmpFolder(absPath))
                {
                    await SendFileAsync(stream, absPath);
                    Console.WriteLine($"[SEND] Sent requested file: {relPath}");
                }
                else
                {
                    Console.WriteLine($"[WARN] Requested file not found: {relPath}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] HandleFileRequest: {ex.Message}");
            }
        }

        private async Task HandleRemoteDelete(string relPath)
        {
            try
            {
                var absPath = Path.Combine(_folder, relPath);
                if (File.Exists(absPath))
                {
                    // Mark as received to prevent sync loops
                    _recentlyReceived[relPath] = DateTime.UtcNow;

                    File.Delete(absPath);
                    Console.WriteLine($"[RECV] Deleted by peer: {relPath}");
                }

                _files.TryRemove(relPath, out _);
                await SaveFileStatesAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] HandleRemoteDelete: {ex.Message}");
            }
        }

        private async Task HandleRemoteRename(string oldRel, string newRel)
        {
            try
            {
                var oldAbs = Path.Combine(_folder, oldRel);
                var newAbs = Path.Combine(_folder, newRel);

                if (File.Exists(oldAbs))
                {
                    // Mark both paths as received to prevent sync loops
                    _recentlyReceived[oldRel] = DateTime.UtcNow;
                    _recentlyReceived[newRel] = DateTime.UtcNow;

                    Directory.CreateDirectory(Path.GetDirectoryName(newAbs));
                    File.Move(oldAbs, newAbs, true);
                    Console.WriteLine($"[RECV] Renamed by peer: {oldRel} -> {newRel}");

                    // Update file tracking
                    if (_files.TryRemove(oldRel, out var meta))
                    {
                        var hash = ComputeFileHash(newAbs);
                        var lastWrite = File.GetLastWriteTimeUtc(newAbs);
                        var fileSize = new FileInfo(newAbs).Length;

                        _files[newRel] = new FileMeta
                        {
                            Hash = hash,
                            LastWriteUtc = lastWrite,
                            Size = fileSize,
                            Version = Guid.NewGuid().ToString(),
                            LastModifiedBy = GetLocalIPAddress()
                        };
                    }
                }
                else
                {
                    _files.TryRemove(oldRel, out _);
                }

                await SaveFileStatesAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] HandleRemoteRename: {ex.Message}");
            }
        }

        // Enhanced file operations with better error handling and progress
        private async Task SendFileAsync(NetworkStream stream, string path)
        {
            try
            {
                var info = new FileInfo(path);
                var hash = ComputeFileHash(path);
                var header = Encoding.UTF8.GetBytes($"{info.Name}|{info.Length}|{hash}\n");
                await stream.WriteAsync(header, 0, header.Length);

                using var fileStream = File.OpenRead(path);
                var buffer = new byte[64 * 1024]; // 64KB chunks
                long totalSent = 0;
                int bytesRead;

                while ((bytesRead = await fileStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
                {
                    await stream.WriteAsync(buffer, 0, bytesRead);
                    totalSent += bytesRead;

                    // Progress reporting for large files
                    if (info.Length > 1024 * 1024 && totalSent % (256 * 1024) == 0) // Report every 256KB for files > 1MB
                    {
                        var percentage = (double)totalSent / info.Length * 100;
                        Console.WriteLine($"[SEND] {Path.GetFileName(path)}: {percentage:F1}% ({totalSent}/{info.Length} bytes)");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] SendFileAsync: {ex.Message}");
                throw;
            }
        }

        private async Task ReceiveFileAsync(NetworkStream stream, string absPath, string relPath)
        {
            try
            {
                string header = await ReadLineAsync(stream);
                if (header == null) return;

                var parts = header.Split('|');
                if (parts.Length != 3) return;

                var fileName = parts[0];
                var fileSize = long.Parse(parts[1]);
                var expectedHash = parts[2];

                // Check if we should accept this file based on conflict resolution
                if (_files.TryGetValue(relPath, out var existingMeta))
                {
                    if (existingMeta.Hash == expectedHash)
                    {
                        Console.WriteLine($"[SKIP] File {relPath} already up to date.");
                        return;
                    }
                }

                var tmpPath = Path.Combine(_tmpFolder, $"{Guid.NewGuid()}.tmp");

                using (var fileStream = File.Create(tmpPath))
                {
                    var buffer = new byte[64 * 1024]; // 64KB chunks
                    long totalRead = 0;

                    while (totalRead < fileSize)
                    {
                        int bytesToRead = (int)Math.Min(buffer.Length, fileSize - totalRead);
                        int bytesRead = await stream.ReadAsync(buffer, 0, bytesToRead);
                        if (bytesRead == 0) break;

                        await fileStream.WriteAsync(buffer, 0, bytesRead);
                        totalRead += bytesRead;

                        // Progress reporting for large files
                        if (fileSize > 1024 * 1024 && totalRead % (256 * 1024) == 0) // Report every 256KB for files > 1MB
                        {
                            var percentage = (double)totalRead / fileSize * 100;
                            Console.WriteLine($"[RECV] {fileName}: {percentage:F1}% ({totalRead}/{fileSize} bytes)");
                        }
                    }
                }

                var hash = ComputeFileHash(tmpPath);
                if (hash == expectedHash)
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(absPath));
                    File.Move(tmpPath, absPath, true);

                    // Mark as received and update file state
                    _recentlyReceived[relPath] = DateTime.UtcNow;
                    _files[relPath] = new FileMeta
                    {
                        Hash = hash,
                        LastWriteUtc = File.GetLastWriteTimeUtc(absPath),
                        Size = fileSize,
                        Version = Guid.NewGuid().ToString(),
                        LastModifiedBy = "peer" // Could be enhanced to track specific peer
                    };
                    await SaveFileStatesAsync();

                    Console.WriteLine($"[RECV] Received file {relPath} ({fileSize} bytes), integrity OK.");
                }
                else
                {
                    File.Delete(tmpPath);
                    Console.WriteLine($"[ERR ] Hash mismatch for file {relPath} ({fileSize} bytes), file discarded.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] ReceiveFileAsync: {ex.Message}");
            }
        }

        // Enhanced utility methods
        private bool IsRecentlyProcessed(string relPath)
        {
            var now = DateTime.UtcNow;

            if (_recentlyReceived.TryGetValue(relPath, out var receivedAt) &&
                (now - receivedAt).TotalSeconds < SuppressionSeconds)
                return true;

            if (_recentlySynced.TryGetValue(relPath, out var sentAt) &&
                (now - sentAt).TotalSeconds < SuppressionSeconds)
                return true;

            if (_pendingOperations.ContainsKey(relPath))
                return true;

            return false;
        }

        private async Task<bool> WaitForFileStability(string path)
        {
            try
            {
                // Wait for file to stabilize
                await Task.Delay(FileStabilitySleepMs);

                // Check if file is locked
                for (int i = 0; i < 3; i++)
                {
                    try
                    {
                        using var stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read);
                        return true; // File is accessible
                    }
                    catch (IOException)
                    {
                        if (i < 2) await Task.Delay(200);
                    }
                }
                return false;
            }
            catch
            {
                return false;
            }
        }

        private SemaphoreSlim GetOrCreateSemaphore(IPEndPoint peer)
        {
            return _connectionSemaphores.GetOrAdd(peer, _ => new SemaphoreSlim(MaxConcurrentConnections));
        }

        private async Task PeriodicStateSync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromMinutes(5), cancellationToken);
                    await SaveFileStatesAsync();
                    await SavePeersAsync();
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERR ] PeriodicStateSync: {ex.Message}");
                }
            }
        }

        // Async versions of save methods
        private async Task SavePeersAsync()
        {
            try
            {
                var dict = new Dictionary<string, string>();
                foreach (var kv in _peers)
                    dict[kv.Key] = kv.Value.ToString();

                var json = JsonSerializer.Serialize(dict, new JsonSerializerOptions { WriteIndented = true });
                await File.WriteAllTextAsync(_peerFile, json);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] SavePeersAsync: {ex.Message}");
            }
        }

        private async Task Shutdown()
        {
            try
            {
                Console.WriteLine("[INFO] Shutting down...");

                // Cancel all pending operations
                _shutdownToken.Cancel();

                // Stop the timer
                _batchProcessingTimer?.Dispose();

                // Process any remaining pending operations
                await ProcessPendingBatch(null);

                // Save final state
                await SaveFileStatesAsync();
                await SavePeersAsync();

                // Dispose resources
                _watcher?.Dispose();
                _tcp?.Stop();
                _udp?.Close();

                // Dispose semaphores
                foreach (var semaphore in _connectionSemaphores.Values)
                    semaphore.Dispose();

                _fullSyncSemaphore?.Dispose();
                _stateFileSemaphore?.Dispose();
                _batchProcessingSemaphore?.Dispose();
                _shutdownToken.Dispose();

                Console.WriteLine("[INFO] Shutdown complete.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] Shutdown error: {ex.Message}");
            }
        }

        public void RequestShutdown()
        {
            _shutdownToken.Cancel();
        }

        // Utility methods
        private bool IsInTmpFolder(string path)
        {
            var fullTmp = Path.GetFullPath(_tmpFolder) + Path.DirectorySeparatorChar;
            var fullPath = Path.GetFullPath(path);
            return fullPath.StartsWith(fullTmp, StringComparison.OrdinalIgnoreCase);
        }

        private string GetRelativePath(string fullPath)
        {
            var root = Path.GetFullPath(_folder);
            var full = Path.GetFullPath(fullPath);
            if (!full.StartsWith(root, StringComparison.OrdinalIgnoreCase)) return null;
            var rel = full.Substring(root.Length).TrimStart(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (string.IsNullOrWhiteSpace(rel)) return null;
            return rel.Replace(Path.AltDirectorySeparatorChar, Path.DirectorySeparatorChar);
        }

        private void LoadPeers()
        {
            try
            {
                if (File.Exists(_peerFile))
                {
                    var data = File.ReadAllText(_peerFile);
                    var peers = JsonSerializer.Deserialize<Dictionary<string, string>>(data);
                    if (peers != null)
                    {
                        foreach (var p in peers)
                            _peers[p.Key] = IPEndPoint.Parse(p.Value);
                        Console.WriteLine($"[INFO] Loaded {peers.Count} peers from disk:");
                        foreach (var p in _peers)
                            Console.WriteLine($"[INFO]   - {p.Key} => {p.Value}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] Failed to load peers: {ex.Message}");
            }
        }

        private void SavePeers()
        {
            try
            {
                var dict = new Dictionary<string, string>();
                foreach (var kv in _peers)
                    dict[kv.Key] = kv.Value.ToString();
                File.WriteAllText(_peerFile, JsonSerializer.Serialize(dict));
            }
            catch { }
        }

        private void LoadFileStates()
        {
            try
            {
                if (File.Exists(_stateFile))
                {
                    var data = File.ReadAllText(_stateFile);
                    var dic = JsonSerializer.Deserialize<Dictionary<string, FileMeta>>(data);
                    if (dic != null)
                    {
                        foreach (var kv in dic)
                            _files[kv.Key] = kv.Value;
                        Console.WriteLine($"[INFO] Loaded {dic.Count} file states from disk");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] Failed to load file state: {ex.Message}");
            }
        }

        private void SaveFileStates()
        {
            try
            {
                var dic = _files.ToDictionary(x => x.Key, x => x.Value);
                File.WriteAllText(_stateFile, JsonSerializer.Serialize(dic, new JsonSerializerOptions { WriteIndented = true }));
            }
            catch { }
        }

        private string ComputeFileHash(string file)
        {
            using var sha = SHA256.Create();
            using var fs = File.OpenRead(file);
            var hash = sha.ComputeHash(fs);
            return Convert.ToHexString(hash);
        }

        private async Task<string> ReadLineAsync(NetworkStream stream)
        {
            var buffer = new List<byte>();
            var singleByte = new byte[1];
            while (true)
            {
                int n = await stream.ReadAsync(singleByte, 0, 1);
                if (n == 0) break; // end of stream
                if (singleByte[0] == '\n') break;
                buffer.Add(singleByte[0]);
            }
            // Remove trailing '\r' if present
            if (buffer.Count > 0 && buffer[buffer.Count - 1] == '\r')
                buffer.RemoveAt(buffer.Count - 1);
            return Encoding.UTF8.GetString(buffer.ToArray());
        }

        private string GetLocalIPAddress()
        {
            var host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (var ip in host.AddressList)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                    return ip.ToString();
            }
            throw new Exception("No network adapters with an IPv4 address in the system!");
        }
    }
}