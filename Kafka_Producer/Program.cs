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


var logger = SeriloggerService.GenerateLogger();

Thread t1 = new(ThreadStarter1);
Thread t2 = new(ThreadStarter2);
t1.Start();
t2.Start();

void ThreadStarter1()
{
    var topic = Topics.TOPIC_ORDER;
    var producerHandler = new ProducerHandler<Order>(topic, logger);
    for (int i = 0; i < 10000; i++)
    {
        var order = GenerateRandomOrder();
        var t = producerHandler.ProduceCarrierAsync(order);
        t.Wait();
        logger.Debug("Order {@Order} transmitted", order);
    }
}


void ThreadStarter2()
{
    var dishes = new Dish[]
    {
        new(Guid.NewGuid(), 10, 0),
        new(Guid.NewGuid(), 1, 1),
        new(Guid.NewGuid(), 0, 5),
        new(Guid.NewGuid(), 3, 2),
    };
    var topic = Topics.TOPIC_DISH;
    var producerHandler = new DishProducerHandler(topic, logger);
    for (int i = 0; i < 10000; i++)
    {
        var idx = Random.Shared.Next(0, dishes.Length);
        var dish = dishes[idx];
        var t = producerHandler.ProduceAsync(dish);
        t.Wait();
        logger.Debug("Dish {@Dish} transmitted", dish);
    }
}

static Order GenerateRandomOrder()
{
    var orderIds = new (Guid, string)[] { (Guid.NewGuid(), "Menu1"), (Guid.NewGuid(), "Menu2"), (Guid.NewGuid(), "Menu3"), };
    var idx = Random.Shared.Next(0, orderIds.Length);
    var order = orderIds[idx];
    return new Order { CustomerId = Guid.NewGuid(), Id = order.Item1, Name = order.Item2 };
}