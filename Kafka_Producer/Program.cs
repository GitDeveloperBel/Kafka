using Confluent.Kafka;
using Shared;
using Serilogger;
using Shared;

//try
//{
//    // See https://aka.ms/new-console-template for more information

//    var config = new ProducerConfig
//    {
//        BootstrapServers = "localhost:9092",
//    };
//    var logger = SeriloggerService.GenerateLogger();
//    using (var producer = new ProducerBuilder<string, int>(config).Build())
//    {
//        Console.WriteLine("Hello, World!");
//        int i = 0;
//        while(true)
//        {
//            await producer.ProduceAsync("testTopic", new Message<string, int> { Key = "testKey", Value = i }/*, Handler*/);
//            logger.Debug("{Data}", i);//Console.WriteLine(i);
//            i++;
//            Thread.Sleep(10); //without it the queue got full, somewhow, after a while and the software crashed (sued procuider.Produce) at that time
//        }
//        producer.Flush(TimeSpan.FromSeconds(10));
//    }

//    void Handler(DeliveryReport<string, int> deliveryReport)
//    {
//        if (deliveryReport.Error.Code is not ErrorCode.NoError)
//        {
//            //Console.WriteLine(deliveryReport.Error.Reason);
//            logger.Error("{@Error}", deliveryReport.Error);
//        }
//        else
//        {
//            Console.WriteLine("did it");
//        }
//    }
//}
//catch (Exception e)
//{
//    Console.WriteLine(e.Message);
//}


var topic = Topics.TOPIC_ORDER;
var logger = SeriloggerService.GenerateLogger();
var producerHandler = new ProducerHandler<Order>(topic, logger);
for(int i = 0; i < 10000; i++)
{
    var order = GenerateRandomOrder();
    await producerHandler.ProduceAsync(order);
    logger.Debug("Order {@Order} transmitted", order);
}

static Order GenerateRandomOrder()
{
    var orderIds = new (Guid, string)[] { (Guid.NewGuid(), "Menu1"), (Guid.NewGuid(), "Menu2"), (Guid.NewGuid(), "Menu3"), };
    var idx = Random.Shared.Next(0, orderIds.Length);
    var order = orderIds[idx];
    return new Order { CustomerId = Guid.NewGuid(), Id = order.Item1, Name = order.Item2 };
}