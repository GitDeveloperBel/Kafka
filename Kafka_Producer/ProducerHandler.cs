using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Serilog;
using Serilog.Core;
using System.Diagnostics;

namespace Shared;

internal class ProducerHandler<T>
{
    protected readonly string _topic;
    protected readonly SchemaRegistryConfig _schemaRegistryConfig;
    protected readonly ProducerConfig _producerConfig;
    protected readonly AvroSerializerConfig _serialiserConfig;
    protected readonly ILogger _logger;

    public ProducerHandler(string topic, ILogger logger)
    {
        _topic = topic;
        _schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "localhost:8081",
        };
        _producerConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
        };
        _serialiserConfig = new();
        _logger = logger;
    }

    public async Task ProduceCarrierAsync(T @event, string key, TopicPartition partition)
    {
        Stopwatch sw = Stopwatch.StartNew();
        using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
        using var producer = new ProducerBuilder<string, Carrier>(_producerConfig).SetValueSerializer(new AvroSerializer<Carrier>(schemaRegistry, _serialiserConfig)).Build();
        var carrier = Carrier.Create(@event);
        var message = new Message<string, Carrier>
        {
            Key = key,
            Value = carrier
        };
        await producer.ProduceAsync(partition, message);
        producer.Flush(TimeSpan.FromSeconds(2));
        sw.Stop();
        _logger.Information("Message {@message} transmitted to topic {Topic} key {Key}, took {Time}", message, _topic, key, sw.Elapsed);
    }
}

internal class DishProducerHandler : ProducerHandler<DishPlaced>
{
    public DishProducerHandler(string topic, ILogger logger) : base(topic, logger)
    {
    }

    public async Task ProduceAsync(DishPlaced @event)
    {
        Stopwatch sw = Stopwatch.StartNew();
        using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
        using var producer = new ProducerBuilder<Null, DishPlaced>(_producerConfig).SetValueSerializer(new AvroSerializer<DishPlaced>(schemaRegistry, _serialiserConfig)).Build();
        var message = new Message<Null, DishPlaced>
        {
            Value = @event
        };
        await producer.ProduceAsync(_topic, message);
        producer.Flush(TimeSpan.FromSeconds(2));
        sw.Stop();
        _logger.Information("Dish Message {@message} transmitted to topic {Topic}, took {Time}", message, _topic, sw.Elapsed);
    }
}
