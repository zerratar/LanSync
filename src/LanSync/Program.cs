namespace LanSync
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Simple LAN Sync");
            var folder = string.Empty;
            if (args.Length > 0)
            {
                // If a folder is provided as a command line argument, use it
                if (Directory.Exists(args[0]))
                {
                    folder = args[0];
                    Console.WriteLine($"Using folder: {args[0]}");
                
                }
                else
                {
                    Console.WriteLine("Provided folder does not exist.");
                }
            }

            if (string.IsNullOrEmpty(folder))
            {
                // Otherwise, prompt for the folder
                Console.WriteLine("Enter the full path of the folder to sync: ");
                Console.Write("> ");
                folder = Console.ReadLine();
                if (!Directory.Exists(folder))
                {
                    Console.WriteLine("Folder does not exist, do you want to create it? [Y/N]");
                    Console.Write("> ");

                    var response = Console.ReadLine()?.Trim().ToUpper();
                    if (response == "Y" || response == "YES")
                    {
                        try
                        {
                            Directory.CreateDirectory(folder);
                            Console.WriteLine($"Created folder: {folder}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Failed to create folder: {ex.Message}");
                            return;
                        }
                    }
                    else
                    {
                        Console.WriteLine("Exiting...");
                        return;
                    }
                }
            }

            var app = new LanSyncApp(folder, 42042); // 42042 is our sync port
            await app.RunAsync();
        }
    }
}
