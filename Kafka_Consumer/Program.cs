// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;
using Kafka_Consumer;
using Serilogger;
using Shared;

//IConsumer<string, int> consumer = null!;

var logger = SeriloggerService.GenerateLogger();


//Console.WriteLine("Hello, World!");
//Thread.Sleep(100);
//var config = new ConsumerConfig
//{
//    BootstrapServers = "localhost:9092",
//    GroupId = "test",
//    AutoOffsetReset = AutoOffsetReset.Earliest
//};


//using (consumer = new ConsumerBuilder<string, int>(config).Build())
//{
//    consumer.Subscribe("testTopic");

//    while (true)
//    {
//        var consumerResult = consumer.Consume();
//        var value = consumerResult.Message.Value;
//        //Console.WriteLine($"{consumerResult.Message.Key},{value}");
//        logger.Information("{Key},{Value}", consumerResult.Message.Key, value);
//    }
//    consumer.Close();
//}
List<IShutdown> services = [];



CancellationTokenSource cts = new();

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
    Shutdown();
};

AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;
AppDomain.CurrentDomain.ProcessExit += CurrentDomain_ProcessExit;


//seems like if a consumer is not closed, the Kafka broker will attempt to contact it? for a minute, before discarding it. If starting a new consumer before then, Kafka will not transmit message until to then.

var groupId = "Shared";
object lockObject = new();
int eggs = 5;
int breads = 31;
var topic = Topics.TOPIC_ORDER;
Thread.Sleep(4000);
Thread t1 = new(ThreadStarter1);
Thread t2 = new(ThreadStarter2);
Thread t3 = new(ThreadStarter3);
Thread t4 = new(ThreadStarter4);
Thread t5 = new(ThreadStarter5);
t1.Start();
t2.Start();
t3.Start();
t4.Start();
t5.Start();

while (true)
    Thread.Sleep(TimeSpan.FromMinutes(1));

void ThreadStarter1()
{
    var consumeHandler = new ConsumerHandler<OrderPlaced>(topic, Handler1, logger, groupId, "client1");
    services.Add(consumeHandler);
    consumeHandler.ConsumeCarrier(cts);
}

void ThreadStarter2()
{
    var consumeHandler = new ConsumerHandler<OrderPlaced>(topic, Handler2, logger, groupId, "client2");
    services.Add(consumeHandler);
    consumeHandler.ConsumeCarrier(cts);
}

void ThreadStarter3()
{
    var consumerHandler = new DishConsumerHandler(Topics.TOPIC_DISH, Handler3, logger, "Single 1", "client3");
    services.Add(consumerHandler);
    consumerHandler.Consume(cts);
}

void ThreadStarter4()
{
    var consumeHandler = new ConsumerHandler<OrderPlaced>(topic, Handler4, logger, "Single 2", "client4");
    services.Add(consumeHandler);
    consumeHandler.ConsumeCarrier(cts);
}

void ThreadStarter5()
{
    var consumeHandler = new ConsumerHandler<OrderPlaced>(topic, Handler5, logger, groupId, "client5");
    services.Add(consumeHandler);
    consumeHandler.ConsumeCarrier(cts);
}

void Handler1(OrderPlaced order)
{
    logger.Information("Shared groupId {Consumer}: Order", "Consumer 1");
}

void Handler2(OrderPlaced order)
{
    logger.Information("Shared groupId {Consumer}: Order", "Consumer 2");
}

void Handler3(DishPlaced dish)
{
    lock (lockObject)
    {
        eggs -= dish.AmountOfEggs;
        breads -= dish.AmountOfBread;
    }
    logger.Information("Dish part of ordered menu requires {Breads} and {Eggs}", dish.AmountOfBread, dish.AmountOfEggs);
    if(eggs < 0)
    {
        //logger.Warning("Missing {MissingEgg} eggs", eggs);
    }
    if (breads < 0)
    {
        //logger.Warning("Missing {MissingBreads} breads", breads);
    }

}
void Handler4(OrderPlaced order)
{
    logger.Information("Own groupId: {@Order} for {Customer}", order, order.CustomerId);
}

void Handler5(OrderPlaced order)
{
    logger.Information("Shared groupId {Consumer}: Order", "Consumer 3");
}


void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
{
    cts.Cancel();
    Shutdown();
}

void CurrentDomain_ProcessExit(object? sender, EventArgs e)
{
    cts.Cancel();
    Shutdown();
}

void Shutdown()
{
    foreach (var service in services)
        service.Shutdown();
}

public interface IShutdown
{
    public void Shutdown();
}
