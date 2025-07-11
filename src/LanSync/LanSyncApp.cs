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

        // Suppression for loop prevention
        private readonly ConcurrentDictionary<string, DateTime> _recentlyReceived = new();
        private readonly ConcurrentDictionary<string, DateTime> _recentlySynced = new();

        // File state tracking
        private readonly ConcurrentDictionary<string, FileMeta> _files = new();

        // Cleanup config (seconds)
        private const int CleanupSeconds = 10;

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
            LoadPeers();
            LoadFileStates();

            // Start periodic cleanup of suppression lists
            _ = Task.Run(SuppressionCleanupTask);

            // Start UDP discovery
            _ = Task.Run(ReceiveBroadcasts);
            _ = Task.Run(BroadcastPresence);

            // Start TCP server for incoming sync
            _ = Task.Run(AcceptIncoming);

            // Start watcher
            _watcher = new FileSystemWatcher(_folder)
            {
                NotifyFilter = NotifyFilters.FileName | NotifyFilters.Size | NotifyFilters.LastWrite,
                IncludeSubdirectories = true,
                EnableRaisingEvents = true
            };
            _watcher.Created += async (s, e) => await OnFileChanged(e.FullPath, "Created");
            _watcher.Changed += async (s, e) => await OnFileChanged(e.FullPath, "Changed");
            _watcher.Deleted += async (s, e) => await OnFileDeleted(e.FullPath);
            _watcher.Renamed += async (s, e) => await OnFileRenamed(e.OldFullPath, e.FullPath);

            Console.WriteLine($"[INFO] Watching folder (recursive): {_folder}");
            Console.WriteLine("[INFO] LAN Sync running. Press Ctrl+C to exit.");
            await Task.Delay(-1);
        }

        // --- Suppression cleanup ---
        private async Task SuppressionCleanupTask()
        {
            while (true)
            {
                var now = DateTime.UtcNow;
                CleanOld(_recentlyReceived, now);
                CleanOld(_recentlySynced, now);
                await Task.Delay(CleanupSeconds * 1000);
            }
        }
        private void CleanOld(ConcurrentDictionary<string, DateTime> dict, DateTime now)
        {
            foreach (var kv in dict)
                if ((now - kv.Value).TotalSeconds > CleanupSeconds)
                    dict.TryRemove(kv.Key, out _);
        }

        // --- OnFileChanged, OnFileDeleted, OnFileRenamed ---
        private async Task OnFileChanged(string path, string reason)
        {
            if (IsInTmpFolder(path)) return;
            var relPath = GetRelativePath(path);
            if (relPath == null) return;

            // Suppression: Received or Sent
            if (_recentlyReceived.TryGetValue(relPath, out var receivedAt))
            {
                _recentlyReceived.TryRemove(relPath, out _);
                if ((DateTime.UtcNow - receivedAt).TotalSeconds < 2)
                {
                    Console.WriteLine($"[SKIP] Not syncing {relPath} (just received from peer).");
                    return;
                }
            }
            if (_recentlySynced.TryGetValue(relPath, out var sentAt))
            {
                _recentlySynced.TryRemove(relPath, out _);
                if ((DateTime.UtcNow - sentAt).TotalSeconds < 2)
                {
                    Console.WriteLine($"[SKIP] Not syncing {relPath} (just sent to peer).");
                    return;
                }
            }
            if (!File.Exists(path)) return;

            // Update file meta
            var hash = ComputeFileHash(path);
            var lastWrite = File.GetLastWriteTimeUtc(path);
            _files[relPath] = new FileMeta { Hash = hash, LastWriteUtc = lastWrite };
            SaveFileStates();

            Console.WriteLine($"[EVENT] File {reason}: {relPath}");

            await Task.Delay(500); // Let file finish writing

            foreach (var peer in _peers.Values)
            {
                try
                {
                    using var client = new TcpClient();
                    await client.ConnectAsync(peer.Address, _port);
                    using var stream = client.GetStream();
                    var msg = Encoding.UTF8.GetBytes("SEND:" + relPath + "\n");
                    await stream.WriteAsync(msg, 0, msg.Length);
                    await SendFileAsync(stream, path);
                    _recentlySynced[relPath] = DateTime.UtcNow;
                    Console.WriteLine($"[SYNC] Sent {relPath} to {peer}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERR ] Failed to send file to {peer}: {ex.Message}");
                }
            }
        }

        private async Task OnFileDeleted(string path)
        {
            if (IsInTmpFolder(path)) return;
            var relPath = GetRelativePath(path);
            if (relPath == null) return;

            _files.TryRemove(relPath, out _);
            SaveFileStates();

            foreach (var peer in _peers.Values)
            {
                try
                {
                    using var client = new TcpClient();
                    await client.ConnectAsync(peer.Address, _port);
                    using var stream = client.GetStream();
                    var msg = Encoding.UTF8.GetBytes("DELETE:" + relPath + "\n");
                    await stream.WriteAsync(msg, 0, msg.Length);
                    Console.WriteLine($"[SYNC] Deleted {relPath} sent to {peer}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERR ] Failed to send delete to {peer}: {ex.Message}");
                }
            }
        }

        private async Task OnFileRenamed(string oldPath, string newPath)
        {
            if (IsInTmpFolder(newPath)) return;
            var oldRel = GetRelativePath(oldPath);
            var newRel = GetRelativePath(newPath);
            if (oldRel == null || newRel == null) return;

            _files.TryRemove(oldRel, out _);
            SaveFileStates();

            foreach (var peer in _peers.Values)
            {
                try
                {
                    using var client = new TcpClient();
                    await client.ConnectAsync(peer.Address, _port);
                    using var stream = client.GetStream();
                    var msg = Encoding.UTF8.GetBytes($"RENAME:{oldRel}|{newRel}\n");
                    await stream.WriteAsync(msg, 0, msg.Length);
                    Console.WriteLine($"[SYNC] Renamed {oldRel} to {newRel} sent to {peer}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERR ] Failed to send rename to {peer}: {ex.Message}");
                }
            }
        }

        // --- Networking and protocol ---
        private async Task BroadcastPresence()
        {
            while (true)
            {
                try
                {
                    var msg = Encoding.UTF8.GetBytes($"LAN_SYNC:{GetLocalIPAddress()}:{_port}");
                    await _udp.SendAsync(msg, msg.Length, new IPEndPoint(IPAddress.Broadcast, _port));
                    await Task.Delay(5000);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERR ] BroadcastPresence: {ex.Message}");
                }
            }
        }
        private async Task ReceiveBroadcasts()
        {
            while (true)
            {
                try
                {
                    var result = await _udp.ReceiveAsync();
                    var msg = Encoding.UTF8.GetString(result.Buffer);
                    if (msg.StartsWith("LAN_SYNC:"))
                    {
                        var parts = msg.Split(":");
                        if (parts.Length == 3)
                        {
                            var ip = parts[1];
                            var port = int.Parse(parts[2]);
                            var localIp = GetLocalIPAddress();
                            if (ip != localIp)
                            {
                                var endpoint = new IPEndPoint(IPAddress.Parse(ip), port);
                                bool isNewPeer = false;
                                if (!_peers.ContainsKey(ip))
                                {
                                    _peers[ip] = endpoint;
                                    isNewPeer = true;
                                    SavePeers();
                                    Console.WriteLine($"[DISC] Discovered new peer: {ip}:{port}");
                                }
                                if (!isNewPeer) _peers[ip] = endpoint;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERR ] ReceiveBroadcasts: {ex.Message}");
                }
            }
        }
        private async Task AcceptIncoming()
        {
            _tcp.Start();
            Console.WriteLine($"[INFO] TCP server listening on port {_port} for incoming syncs.");
            while (true)
            {
                try
                {
                    var client = await _tcp.AcceptTcpClientAsync();
                    _ = Task.Run(() => HandleClient(client));
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERR ] AcceptIncoming: {ex.Message}");
                }
            }
        }
        private async Task HandleClient(TcpClient client)
        {
            try
            {
                using var stream = client.GetStream();
                string line = await ReadLineAsync(stream);
                if (line == null) return;

                if (line.StartsWith("SEND:"))
                {
                    var relPath = line.Substring(5);
                    var absPath = Path.Combine(_folder, relPath);
                    if (IsInTmpFolder(absPath)) return;
                    await ReceiveFileAsync(stream, absPath, relPath);
                }
                else if (line.StartsWith("DELETE:"))
                {
                    var relPath = line.Substring(7);
                    var absPath = Path.Combine(_folder, relPath);
                    if (File.Exists(absPath))
                    {
                        File.Delete(absPath);
                        Console.WriteLine($"[RECV] Deleted by peer: {relPath}");
                    }
                    _files.TryRemove(relPath, out _);
                    SaveFileStates();
                }
                else if (line.StartsWith("RENAME:"))
                {
                    var parts = line.Substring(7).Split('|');
                    if (parts.Length == 2)
                    {
                        var oldRel = parts[0];
                        var newRel = parts[1];
                        var oldAbs = Path.Combine(_folder, oldRel);
                        var newAbs = Path.Combine(_folder, newRel);
                        if (File.Exists(oldAbs))
                        {
                            File.Move(oldAbs, newAbs, true);
                            Console.WriteLine($"[RECV] Renamed by peer: {oldRel} -> {newRel}");
                        }
                        _files.TryRemove(oldRel, out _);
                        SaveFileStates();
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

        // --- File sending and receiving ---
        private async Task SendFileAsync(NetworkStream stream, string path)
        {
            try
            {
                var info = new FileInfo(path);
                var hash = ComputeFileHash(path);
                var header = Encoding.UTF8.GetBytes($"{info.Name}|{info.Length}|{hash}\n");
                await stream.WriteAsync(header, 0, header.Length);

                using var fileStream = File.OpenRead(path);
                byte[] buffer = new byte[8192];
                long totalSent = 0;
                int bytesRead;
                while ((bytesRead = await fileStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
                {
                    await stream.WriteAsync(buffer, 0, bytesRead);
                    totalSent += bytesRead;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] SendFileAsync: {ex.Message}");
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

                var tmpPath = Path.Combine(_tmpFolder, $"{Guid.NewGuid()}.tmp");
                long totalRead = 0;
                using (var fileStream = File.Open(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None))
                {
                    byte[] buffer = new byte[8192];
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
                    _recentlyReceived[relPath] = DateTime.UtcNow;
                    _files[relPath] = new FileMeta { Hash = hash, LastWriteUtc = File.GetLastWriteTimeUtc(absPath) };
                    SaveFileStates();
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

        // --- Utilities ---
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
        }
    }
}
