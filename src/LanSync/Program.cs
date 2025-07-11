namespace LanSync
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Simple LAN Sync - Enter the full path of the folder to sync:");
            string folder = Console.ReadLine();
            if (!Directory.Exists(folder))
            {
                Console.WriteLine("Folder does not exist.");
                return;
            }

            var app = new LanSyncApp(folder, 42042); // 42042 is our sync port
            await app.RunAsync();
        }
    }
}
