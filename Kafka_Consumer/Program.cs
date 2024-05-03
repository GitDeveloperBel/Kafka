// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;

IConsumer<string, int> consumer = null!;
AppDomain.CurrentDomain.ProcessExit += CurrentDomain_ProcessExit;

Console.WriteLine("Hello, World!");
Thread.Sleep(100);
var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "test",
    AutoOffsetReset = AutoOffsetReset.Earliest
};


using (consumer = new ConsumerBuilder<string, int>(config).Build())
{
    consumer.Subscribe("testTopic");

    while (true)
    {
        var consumerResult = consumer.Consume();
        var value = consumerResult.Message.Value;
        Console.WriteLine($"{consumerResult.Message.Key},{value}");
    }
    consumer.Close();
}



void CurrentDomain_ProcessExit(object? sender, EventArgs e)
{
    consumer?.Close();
}
//seems like if a comsumer is not closed, the Kafka broker will attempt to contact it? for a minute, before discarding it. If starting a new consumer before then, Kafka will not transmit message until to then.