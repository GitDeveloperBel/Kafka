﻿// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;
using Kafka_Consumer;
using Serilogger;
using Shared;

//IConsumer<string, int> consumer = null!;

var logger = SeriloggerService.GenerateLogger();
AppDomain.CurrentDomain.ProcessExit += CurrentDomain_ProcessExit;


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



void CurrentDomain_ProcessExit(object? sender, EventArgs e)
{
    //consumer?.Close();
}
//seems like if a consumer is not closed, the Kafka broker will attempt to contact it? for a minute, before discarding it. If starting a new consumer before then, Kafka will not transmit message until to then.


var topic = Topics.TOPIC_ORDER;
Thread.Sleep(1000);
Thread t1 = new(ThreadStarter1);
Thread t2 = new(ThreadStarter2);
t1.Start();
t2.Start();

while (true)
    Thread.Sleep(TimeSpan.FromHours(4));


void ThreadStarter1()
{
    var consumeHandler = new ConsumerHandler<Order>(topic, Handler1, logger);
    consumeHandler.Consume(new CancellationTokenSource());
}

void ThreadStarter2()
{
    var consumeHandler = new ConsumerHandler<Order>(topic, Handler2, logger);
    consumeHandler.Consume(new CancellationTokenSource());
}

void Handler1(Order order)
{
    logger.Information("Order {@order} saved to database", order);
}

void Handler2(Order order)
{
    logger.Information("Something different to do with {Order}", order.Id);
}

