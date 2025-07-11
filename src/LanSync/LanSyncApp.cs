using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace LanSync
{
    public class LanSyncApp
    {
        private string _folder;
        private int _port;
        private UdpClient _udp;
        private TcpListener _tcp;
        private ConcurrentDictionary<string, IPEndPoint> _peers = new();
        private FileSystemWatcher _watcher;
        private string _peerFile => Path.Combine(_folder, ".peers.json");

        public LanSyncApp(string folder, int port)
        {
            _folder = folder;
            _port = port;
            _udp = new UdpClient(port);
            _udp.EnableBroadcast = true;
            _tcp = new TcpListener(IPAddress.Any, port);
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
                NotifyFilter = NotifyFilters.FileName | NotifyFilters.Size | NotifyFilters.LastWrite
            };
            _watcher.Created += (s, e) => OnFileChanged(e.FullPath);
            _watcher.Changed += (s, e) => OnFileChanged(e.FullPath);
            _watcher.Renamed += (s, e) => OnFileChanged(e.FullPath);
            _watcher.EnableRaisingEvents = true;

            Console.WriteLine("LAN Sync running. Press Ctrl+C to exit.");
            await Task.Delay(-1);
        }

        private void OnFileChanged(string path)
        {
            if (Path.GetFileName(path).StartsWith(".")) return; // Skip dotfiles
            Console.WriteLine($"Detected change: {path}");
            NotifyPeers(Path.GetFileName(path));
        }

        private void NotifyPeers(string fileName)
        {
            foreach (var peer in _peers.Values)
            {
                try
                {
                    using var client = new TcpClient();
                    client.Connect(peer.Address, _port);
                    using var stream = client.GetStream();

                    // Send a request to sync the file
                    var msg = Encoding.UTF8.GetBytes("SYNC:" + fileName + "\n");
                    stream.Write(msg, 0, msg.Length);
                }
                catch
                {
                    // Ignore
                }
            }
        }

        private void LoadPeers()
        {
            if (File.Exists(_peerFile))
            {
                var data = File.ReadAllText(_peerFile);
                var peers = JsonSerializer.Deserialize<Dictionary<string, string>>(data);
                foreach (var p in peers)
                    _peers[p.Key] = IPEndPoint.Parse(p.Value);
            }
        }
        private void SavePeers()
        {
            var dict = new Dictionary<string, string>();
            foreach (var kv in _peers)
                dict[kv.Key] = kv.Value.ToString();
            File.WriteAllText(_peerFile, JsonSerializer.Serialize(dict));
        }

        private async Task BroadcastPresence()
        {
            while (true)
            {
                var msg = Encoding.UTF8.GetBytes($"LAN_SYNC:{GetLocalIPAddress()}:{_port}");
                await _udp.SendAsync(msg, msg.Length, new IPEndPoint(IPAddress.Broadcast, _port));
                await Task.Delay(5000);
            }
        }

        private async Task ReceiveBroadcasts()
        {
            while (true)
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
                        if (ip != GetLocalIPAddress())
                        {
                            var endpoint = new IPEndPoint(IPAddress.Parse(ip), port);
                            _peers[ip] = endpoint;
                            SavePeers();
                        }
                    }
                }
            }
        }

        private async Task AcceptIncoming()
        {
            _tcp.Start();
            while (true)
            {
                var client = await _tcp.AcceptTcpClientAsync();
                _ = Task.Run(() => HandleClient(client));
            }
        }

        private async Task HandleClient(TcpClient client)
        {
            using var stream = client.GetStream();
            using var reader = new StreamReader(stream, Encoding.UTF8);
            string line = await reader.ReadLineAsync();
            if (line.StartsWith("SYNC:"))
            {
                var fileName = line.Substring(5);
                var filePath = Path.Combine(_folder, fileName);
                if (File.Exists(filePath))
                {
                    Console.WriteLine($"Peer requested file: {fileName}, sending...");
                    await SendFileAsync(stream, filePath);
                }
            }
            else if (line.StartsWith("REQUEST:"))
            {
                var fileName = line.Substring(8);
                var filePath = Path.Combine(_folder, fileName);
                if (File.Exists(filePath))
                    await SendFileAsync(stream, filePath);
            }
            else if (line.StartsWith("SEND:"))
            {
                var fileName = line.Substring(5);
                var filePath = Path.Combine(_folder, fileName);
                await ReceiveFileAsync(stream, filePath);
            }
        }

        private async Task SendFileAsync(NetworkStream stream, string filePath)
        {
            var info = new FileInfo(filePath);
            var hash = ComputeFileHash(filePath);

            var header = Encoding.UTF8.GetBytes($"{info.Name}|{info.Length}|{hash}\n");
            await stream.WriteAsync(header, 0, header.Length);

            // Send file in chunks
            using var fileStream = File.OpenRead(filePath);
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = await fileStream.ReadAsync(buffer, 0, buffer.Length)) > 0)
            {
                await stream.WriteAsync(buffer, 0, bytesRead);
            }
        }

        private async Task ReceiveFileAsync(NetworkStream stream, string filePath)
        {
            // Read header: filename|length|hash
            using var reader = new StreamReader(stream, Encoding.UTF8, false, 1024, true);
            string header = await reader.ReadLineAsync();
            var parts = header.Split('|');
            var fileName = parts[0];
            var fileSize = long.Parse(parts[1]);
            var expectedHash = parts[2];

            var tmpPath = filePath + ".tmp";

            using var fileStream = File.OpenWrite(tmpPath);
            byte[] buffer = new byte[8192];
            int bytesRead;
            long totalRead = 0;

            while (totalRead < fileSize &&
                (bytesRead = await stream.ReadAsync(buffer, 0, (int)Math.Min(buffer.Length, fileSize - totalRead))) > 0)
            {
                await fileStream.WriteAsync(buffer, 0, bytesRead);
                totalRead += bytesRead;
            }
            fileStream.Close();

            // Check hash
            var hash = ComputeFileHash(tmpPath);
            if (hash == expectedHash)
            {
                File.Move(tmpPath, filePath, true);
                Console.WriteLine($"Received file {fileName}, integrity OK.");
            }
            else
            {
                File.Delete(tmpPath);
                Console.WriteLine($"Received file {fileName}, but hash mismatch!");
            }
        }

        private string ComputeFileHash(string file)
        {
            using var sha = SHA256.Create();
            using var fs = File.OpenRead(file);
            var hash = sha.ComputeHash(fs);
            return Convert.ToHexString(hash);
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
