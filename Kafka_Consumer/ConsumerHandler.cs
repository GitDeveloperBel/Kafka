using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Serilog;
using Shared;
using System.Text.Json;

namespace Kafka_Consumer;

internal class ConsumerHandler<T> 
{
    protected readonly string _topic;
    protected readonly SchemaRegistryConfig _schemaRegistryConfig;
    protected readonly ConsumerConfig _consumerConfig;
    protected readonly AvroDeserializerConfig _serialiserConfig;
    protected readonly ILogger _logger;

    protected readonly Action<T> _action;
    public ConsumerHandler(string topic, Action<T> handle, ILogger logger, string groupId)
    {
        _topic = topic;
        _schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "localhost:8081",
        };
        _consumerConfig = new ConsumerConfig
        {
            GroupId = groupId,
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Latest,
            //AllowAutoCreateTopics = true,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.RoundRobin,
        };
        _serialiserConfig = new();
        _action = handle;
        _logger = logger;
    }

    public void ConsumeCarrier(CancellationTokenSource cts)
    {
        using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
        using var consumer = new ConsumerBuilder<string, Carrier>(_consumerConfig).SetValueDeserializer(new AvroDeserializer<Carrier>(schemaRegistry, _serialiserConfig).AsSyncOverAsync()).Build();
        consumer.Subscribe(_topic);
        while (true)
        {
            var cr = consumer.Consume(cts.Token);
            var carrier = cr.Message.Value; // TODO: try catch
            //_logger.Debug("{Key}", cr.Key);
            _action.Invoke(JsonSerializer.Deserialize<T>(carrier.Data)!);
        }
        consumer.Close();
    }
}


internal class DishConsumerHandler : ConsumerHandler<DishPlaced>
{
    public DishConsumerHandler(string topic, Action<DishPlaced> handle, ILogger logger, string groupId) : base(topic, handle, logger, groupId)
    {
    }

    public void Consume(CancellationTokenSource cts)
    {
        using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
        using var consumer = new ConsumerBuilder<Null, DishPlaced>(_consumerConfig).SetValueDeserializer(new AvroDeserializer<DishPlaced>(schemaRegistry, _serialiserConfig).AsSyncOverAsync()).Build();
        consumer.Subscribe(_topic);
        while (true)
        {
            var cr = consumer.Consume(cts.Token);
            var dish = cr.Message.Value; // TODO: try catch
            _action.Invoke(dish);
        }
        consumer.Close();
    }
}
