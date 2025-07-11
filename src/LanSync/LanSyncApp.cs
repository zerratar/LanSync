using System.Collections.Concurrent;
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

        // Debouncing for file events
        private readonly ConcurrentDictionary<string, CancellationTokenSource> _pendingFileEvents = new();

        // Connection pooling
        private readonly ConcurrentDictionary<IPEndPoint, SemaphoreSlim> _connectionSemaphores = new();
        private const int MaxConcurrentConnections = 3;

        // Configuration constants
        private const int CleanupSeconds = 30;
        private const int SuppressionSeconds = 5;
        private const int FileEventDebounceMs = 1000;
        private const int FileStabilitySleepMs = 500;
        private const int MaxRetryAttempts = 3;
        private const int RetryDelayMs = 1000;

        // Shutdown coordination
        private readonly CancellationTokenSource _shutdownToken = new();

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
                    Task.Run(() => PeriodicStateSync(_shutdownToken.Token))
                };

                // Start file watcher
                StartFileWatcher();

                Console.WriteLine($"[INFO] Watching folder (recursive): {_folder}");
                Console.WriteLine("[INFO] LAN Sync running. Press Ctrl+C to exit.");

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

        private void StartFileWatcher()
        {
            _watcher = new FileSystemWatcher(_folder)
            {
                NotifyFilter = NotifyFilters.FileName | NotifyFilters.Size | NotifyFilters.LastWrite | NotifyFilters.CreationTime,
                IncludeSubdirectories = true,
                EnableRaisingEvents = true
            };

            _watcher.Created += (s, e) => ScheduleFileEvent(e.FullPath, "Created");
            _watcher.Changed += (s, e) => ScheduleFileEvent(e.FullPath, "Changed");
            _watcher.Deleted += (s, e) => ScheduleFileEvent(e.FullPath, "Deleted");
            _watcher.Renamed += (s, e) => ScheduleRenameEvent(e.OldFullPath, e.FullPath);
        }

        private void ScheduleFileEvent(string path, string eventType)
        {
            if (IsInTmpFolder(path)) return;
            var relPath = GetRelativePath(path);
            if (relPath == null) return;

            // Cancel existing pending operation for this file
            if (_pendingFileEvents.TryRemove(relPath, out var existingCts))
            {
                existingCts.Cancel();
                existingCts.Dispose();
            }

            // Schedule new operation with debouncing
            var cts = new CancellationTokenSource();
            _pendingFileEvents[relPath] = cts;

            _ = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(FileEventDebounceMs, cts.Token);

                    // Remove from pending operations
                    _pendingFileEvents.TryRemove(relPath, out _);

                    if (eventType == "Deleted")
                        await OnFileDeleted(path);
                    else
                        await OnFileChanged(path, eventType);
                }
                catch (OperationCanceledException)
                {
                    // Event was debounced/cancelled
                }
                finally
                {
                    cts.Dispose();
                }
            });
        }

        private void ScheduleRenameEvent(string oldPath, string newPath)
        {
            if (IsInTmpFolder(newPath)) return;
            var oldRel = GetRelativePath(oldPath);
            var newRel = GetRelativePath(newPath);
            if (oldRel == null || newRel == null) return;

            // Cancel any pending operations for both paths
            if (_pendingFileEvents.TryRemove(oldRel, out var oldCts))
            {
                oldCts.Cancel();
                oldCts.Dispose();
            }
            if (_pendingFileEvents.TryRemove(newRel, out var newCts))
            {
                newCts.Cancel();
                newCts.Dispose();
            }

            _ = Task.Run(async () =>
            {
                await Task.Delay(FileEventDebounceMs / 2); // Shorter delay for renames
                await OnFileRenamed(oldPath, newPath);
            });
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

        // Enhanced file change handling with better conflict detection
        private async Task OnFileChanged(string path, string reason)
        {
            if (IsInTmpFolder(path)) return;
            var relPath = GetRelativePath(path);
            if (relPath == null) return;

            // Enhanced suppression check with atomic operations
            if (IsRecentlyProcessed(relPath))
            {
                Console.WriteLine($"[SKIP] Not syncing {relPath} (recently processed).");
                return;
            }

            if (!File.Exists(path)) return;

            // Wait for file to stabilize and check if it's locked
            if (!await WaitForFileStability(path))
            {
                Console.WriteLine($"[SKIP] File {relPath} is locked or unstable.");
                return;
            }

            try
            {
                // Mark as pending to prevent concurrent processing
                _pendingOperations[relPath] = DateTime.UtcNow;

                // Compute file metadata
                var hash = ComputeFileHash(path);
                var lastWrite = File.GetLastWriteTimeUtc(path);
                var fileSize = new FileInfo(path).Length;

                // Check for conflicts with existing file state
                if (_files.TryGetValue(relPath, out var existingMeta))
                {
                    if (existingMeta.Hash == hash)
                    {
                        Console.WriteLine($"[SKIP] File {relPath} unchanged (same hash).");
                        return;
                    }

                    // Conflict resolution: use last write time
                    if (lastWrite <= existingMeta.LastWriteUtc.AddSeconds(1)) // 1 second tolerance
                    {
                        Console.WriteLine($"[SKIP] File {relPath} is older or same age as existing version.");
                        return;
                    }
                }

                // Update file metadata
                _files[relPath] = new FileMeta
                {
                    Hash = hash,
                    LastWriteUtc = lastWrite,
                    Size = fileSize
                };
                await SaveFileStatesAsync();

                Console.WriteLine($"[EVENT] File {reason}: {relPath} (Size: {fileSize}, Hash: {hash[..8]}...)");

                // Sync to all peers with retry logic
                await SyncFileToPeers(relPath, path);

                _recentlySynced[relPath] = DateTime.UtcNow;
            }
            finally
            {
                _pendingOperations.TryRemove(relPath, out _);
            }
        }

        private async Task OnFileDeleted(string path)
        {
            if (IsInTmpFolder(path)) return;
            var relPath = GetRelativePath(path);
            if (relPath == null) return;

            // Mark as pending
            _pendingOperations[relPath] = DateTime.UtcNow;

            try
            {
                _files.TryRemove(relPath, out _);
                await SaveFileStatesAsync();

                Console.WriteLine($"[EVENT] File Deleted: {relPath}");

                await SyncDeleteToPeers(relPath);
            }
            finally
            {
                _pendingOperations.TryRemove(relPath, out _);
            }
        }

        private async Task OnFileRenamed(string oldPath, string newPath)
        {
            if (IsInTmpFolder(newPath)) return;
            var oldRel = GetRelativePath(oldPath);
            var newRel = GetRelativePath(newPath);
            if (oldRel == null || newRel == null) return;

            try
            {
                // Update file tracking
                if (_files.TryRemove(oldRel, out var meta) && File.Exists(newPath))
                {
                    // Update metadata for new path
                    var hash = ComputeFileHash(newPath);
                    var lastWrite = File.GetLastWriteTimeUtc(newPath);
                    var fileSize = new FileInfo(newPath).Length;

                    _files[newRel] = new FileMeta
                    {
                        Hash = hash,
                        LastWriteUtc = lastWrite,
                        Size = fileSize
                    };
                }

                await SaveFileStatesAsync();

                Console.WriteLine($"[EVENT] File Renamed: {oldRel} -> {newRel}");

                await SyncRenameToPeers(oldRel, newRel);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] OnFileRenamed: {ex.Message}");
            }
        }

        // Enhanced peer synchronization with retry logic
        private async Task SyncFileToPeers(string relPath, string path)
        {
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
                            Size = fileSize
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

        // Enhanced file operations with better error handling
        private async Task SendFileAsync(NetworkStream stream, string path)
        {
            try
            {
                var info = new FileInfo(path);
                var hash = ComputeFileHash(path);
                var header = Encoding.UTF8.GetBytes($"{info.Name}|{info.Length}|{hash}\n");
                await stream.WriteAsync(header, 0, header.Length);

                using var fileStream = File.OpenRead(path);
                await fileStream.CopyToAsync(stream);
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
                    var buffer = new byte[8192];
                    long totalRead = 0;

                    while (totalRead < fileSize)
                    {
                        int bytesToRead = (int)Math.Min(buffer.Length, fileSize - totalRead);
                        int bytesRead = await stream.ReadAsync(buffer, 0, bytesToRead);
                        if (bytesRead == 0) break;

                        await fileStream.WriteAsync(buffer, 0, bytesRead);
                        totalRead += bytesRead;
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
                        Size = fileSize
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

        private async Task SaveFileStatesAsync()
        {
            try
            {
                var dic = _files.ToDictionary(x => x.Key, x => x.Value);
                var json = JsonSerializer.Serialize(dic, new JsonSerializerOptions { WriteIndented = true });
                await File.WriteAllTextAsync(_stateFile, json);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] SaveFileStatesAsync: {ex.Message}");
            }
        }

        private async Task Shutdown()
        {
            try
            {
                Console.WriteLine("[INFO] Shutting down...");

                // Cancel all pending operations
                _shutdownToken.Cancel();

                // Cancel all pending file events
                foreach (var cts in _pendingFileEvents.Values)
                {
                    cts.Cancel();
                    cts.Dispose();
                }
                _pendingFileEvents.Clear();

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

        // Existing utility methods (unchanged)
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
                    foreach (var p in peers)
                        _peers[p.Key] = IPEndPoint.Parse(p.Value);
                    Console.WriteLine($"[INFO] Loaded {peers.Count} peers from disk:");
                    foreach (var p in _peers)
                        Console.WriteLine($"[INFO]   - {p.Key} => {p.Value}");
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
                        foreach (var kv in dic)
                            _files[kv.Key] = kv.Value;
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

        private class FileMeta
        {
            public string Hash { get; set; }
            public DateTime LastWriteUtc { get; set; }
            public long Size { get; set; }
        }
    }
}