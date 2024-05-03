using Confluent.Kafka;

try
{
    // See https://aka.ms/new-console-template for more information

    Console.WriteLine("Hello, World!");

    var config = new ProducerConfig
    {
        BootstrapServers = "localhost:9092",
    };
    Console.WriteLine("Hello, World!");

    using (var producer = new ProducerBuilder<string, int>(config).Build())
    {
        Console.WriteLine("Hello, World!");
        int i = 0;
        while(true)
        {
            await producer.ProduceAsync("testTopic", new Message<string, int> { Key = "testKey", Value = i }/*, Handler*/);
            Console.WriteLine(i);
            i++;
            Thread.Sleep(10); //without it the queue got full, somewhow, after a while and the software crashed (sued procuider.Produce) at that time
        }
        producer.Flush(TimeSpan.FromSeconds(10));
    }
    Console.WriteLine("Hello, World!");

    static void Handler(DeliveryReport<string, int> deliveryReport)
    {
        if (deliveryReport.Error.Code is not ErrorCode.NoError)
        {
            Console.WriteLine(deliveryReport.Error.Reason);
        }
        else
        {
            Console.WriteLine("did it");
        }
    }
}
catch (Exception e)
{
    Console.WriteLine(e.Message);
}