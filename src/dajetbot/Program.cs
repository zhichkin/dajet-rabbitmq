using System;

namespace DaJet.Telegram.Bot
{
    internal class Program
    {
        static void Main(string[] args)
        {
            string message = "Hello from DaJet Bot !!!";

            if (args != null && args.Length > 0)
            {
                message = args[0];
            }

            using (IDaJetTelegramBot bot = new DaJetTelegramBot())
            {
                try
                {
                    bot.SendMessage(null, message);

                    Console.WriteLine($"Message: \"{message}\" is sent successfully.");
                }
                catch (Exception error)
                {
                    Console.WriteLine(error.Message);
                }
            }
        }
    }
}