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
        private readonly string _tmpFolder;
        private FileSystemWatcher _watcher;

        // For suppressing re-syncs (loop prevention)
        private readonly ConcurrentDictionary<string, DateTime> _recentlyReceived = new();

        public LanSyncApp(string folder, int port)
        {
            _folder = folder;
            _port = port;

            _udp = new UdpClient(port);
            _udp.EnableBroadcast = true;
            _tcp = new TcpListener(IPAddress.Any, port);

            // Store peers.json in AppData\LanSync
            var appDataFolder = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                "LanSync");
            Directory.CreateDirectory(appDataFolder);
            _peerFile = Path.Combine(appDataFolder, "peers.json");

            // Create tmp folder in sync root
            _tmpFolder = Path.Combine(_folder, ".tmp");
            Directory.CreateDirectory(_tmpFolder);
        }

        public async Task RunAsync()
        {
            LoadPeers();

            // Start UDP discovery
            _ = Task.Run(ReceiveBroadcasts);
            _ = Task.Run(BroadcastPresence);

            // Start TCP server for incoming sync
            _ = Task.Run(AcceptIncoming);

            // Start watcher
            _watcher = new FileSystemWatcher(_folder)
            {
                NotifyFilter = NotifyFilters.FileName | NotifyFilters.Size | NotifyFilters.LastWrite,
                IncludeSubdirectories = false,
                EnableRaisingEvents = true
            };

            _watcher.Created += async (s, e) => await OnFileChanged(e.FullPath, "Created");
            _watcher.Changed += async (s, e) => await OnFileChanged(e.FullPath, "Changed");
            _watcher.Renamed += async (s, e) => await OnFileChanged(e.FullPath, "Renamed");

            Console.WriteLine($"[INFO] Watching folder: {_folder}");
            Console.WriteLine("[INFO] LAN Sync running. Press Ctrl+C to exit.");
            await Task.Delay(-1);
        }

        private async Task OnFileChanged(string path, string reason)
        {
            // Ignore if in .tmp folder
            if (IsInTmpFolder(path))
            {
                Console.WriteLine($"[SKIP] Ignoring file in temp folder: {path}");
                return;
            }

            // Prevent infinite sync loop: Ignore if just received via sync
            if (_recentlyReceived.TryGetValue(path, out var receivedAt))
            {
                _recentlyReceived.TryRemove(path, out _);
                if ((DateTime.UtcNow - receivedAt).TotalSeconds < 2)
                {
                    Console.WriteLine($"[SKIP] Not syncing {Path.GetFileName(path)} (just received from peer).");
                    return;
                }
            }

            var fileName = Path.GetFileName(path);

            if (!File.Exists(path)) return; // Sometimes fires for deleted files

            Console.WriteLine($"[EVENT] File {reason}: {fileName}");

            // Wait a bit to ensure file is not locked/incomplete
            await Task.Delay(500);

            // Check again for existence (could have been deleted in the meantime)
            if (!File.Exists(path))
            {
                Console.WriteLine($"[SKIP] File disappeared after delay: {fileName}");
                return;
            }

            foreach (var peer in _peers.Values)
            {
                try
                {
                    Console.WriteLine($"[LOG] Connecting to peer {peer} to send file: {fileName}");
                    using var client = new TcpClient();
                    await client.ConnectAsync(peer.Address, _port);
                    using var stream = client.GetStream();

                    var msg = Encoding.UTF8.GetBytes("SEND:" + fileName + "\n");
                    await stream.WriteAsync(msg, 0, msg.Length);

                    Console.WriteLine($"[SYNC] Sending {fileName} to {peer}...");
                    await SendFileAsync(stream, path);
                    Console.WriteLine($"[SYNC] Done sending {fileName} to {peer}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERR ] Failed to send file to {peer}: {ex.Message}");
                }
            }
        }

        private bool IsInTmpFolder(string path)
        {
            var fullTmp = Path.GetFullPath(_tmpFolder) + Path.DirectorySeparatorChar;
            var fullPath = Path.GetFullPath(path);
            return fullPath.StartsWith(fullTmp, StringComparison.OrdinalIgnoreCase);
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
                    {
                        _peers[p.Key] = IPEndPoint.Parse(p.Value);
                    }
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
                Console.WriteLine($"[INFO] Saved peer list to {_peerFile}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] Failed to save peers: {ex.Message}");
            }
        }

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

                                if (!isNewPeer)
                                {
                                    // Update endpoint in case peer's port changed
                                    _peers[ip] = endpoint;
                                }
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
                    Console.WriteLine($"[CONN] Accepted incoming connection from {client.Client.RemoteEndPoint}");
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
                if (line == null)
                {
                    Console.WriteLine("[WARN] Received empty message.");
                    return;
                }
                if (line.StartsWith("SEND:"))
                {
                    var fileName = line.Substring(5);
                    var filePath = Path.Combine(_folder, fileName);
                    if (IsInTmpFolder(filePath))
                    {
                        Console.WriteLine($"[SKIP] Ignoring incoming file (in .tmp): {fileName}");
                        return;
                    }
                    Console.WriteLine($"[RECV] Peer wants to send: {fileName} (writing to {filePath})");
                    await ReceiveFileAsync(stream, filePath);
                }
                else
                {
                    Console.WriteLine($"[WARN] Unknown command received: {line}");
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

        private async Task SendFileAsync(NetworkStream stream, string filePath)
        {
            try
            {
                var info = new FileInfo(filePath);
                var hash = ComputeFileHash(filePath);

                var header = Encoding.UTF8.GetBytes($"{info.Name}|{info.Length}|{hash}\n");
                await stream.WriteAsync(header, 0, header.Length);

                using var fileStream = File.OpenRead(filePath);
                byte[] buffer = new byte[8192];
                int bytesRead;
                long totalSent = 0;

                while ((bytesRead = await fileStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
                {
                    await stream.WriteAsync(buffer, 0, bytesRead);
                    totalSent += bytesRead;
                }
                Console.WriteLine($"[SEND] Finished sending {info.Name} ({totalSent} bytes).");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] SendFileAsync: {ex.Message}");
            }
        }

        private async Task ReceiveFileAsync(NetworkStream stream, string filePath)
        {
            try
            {
                string header = await ReadLineAsync(stream);
                if (header == null)
                {
                    Console.WriteLine("[ERR ] No header received for file transfer.");
                    return;
                }
                var parts = header.Split('|');
                if (parts.Length != 3)
                {
                    Console.WriteLine($"[ERR ] Invalid file header received: {header}");
                    return;
                }
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
                        if (bytesRead == 0)
                        {
                            break;
                        }
                        await fileStream.WriteAsync(buffer, 0, bytesRead);
                        totalRead += bytesRead;
                    }
                }

                var hash = ComputeFileHash(tmpPath);
                if (hash == expectedHash)
                {
                    File.Move(tmpPath, filePath, true);
                    _recentlyReceived[filePath] = DateTime.UtcNow;  // Suppress resync
                    Console.WriteLine($"[RECV] Received file {fileName} ({fileSize} bytes), integrity OK.");
                }
                else
                {
                    File.Delete(tmpPath);
                    Console.WriteLine($"[ERR ] Hash mismatch for file {fileName} ({fileSize} bytes), file discarded.");
                    Console.WriteLine($"[ERR ] Expected hash: {expectedHash}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERR ] ReceiveFileAsync: {ex.Message}");
            }
        }

        private string ComputeFileHash(string file)
        {
            using var sha = SHA256.Create();
            using var fs = File.OpenRead(file);
            var hash = sha.ComputeHash(fs);
            return Convert.ToHexString(hash);
        }

        // Helper: Read a line from NetworkStream, ASCII/UTF8, without using StreamReader
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
