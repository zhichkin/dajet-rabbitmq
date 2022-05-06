using DaJet.Telegram.Bot;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Threading.Tasks;

namespace DaJet.Bot.Test
{
    [TestClass] public class DaJetBot
    {
        [TestMethod] public async Task SendMessageAsync()
        {
            using (IDaJetTelegramBot bot = new DaJetTelegramBot())
            {
                try
                {
                    await bot.SendMessageAsync(null, "Hello from bot async!");
                }
                catch (Exception error)
                {
                    Console.WriteLine(error.Message);
                }
            }
        }
        [TestMethod] public void SendMessage()
        {
            using (IDaJetTelegramBot bot = new DaJetTelegramBot())
            {
                try
                {
                    bot.SendMessage(null, "Hello from bot sync!");
                }
                catch (Exception error)
                {
                    Console.WriteLine(error.Message);
                }
            }
        }
    }
}