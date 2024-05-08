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

    public async Task ProduceCarrierAsync(T @event)
    {
        Stopwatch sw = Stopwatch.StartNew();
        using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
        using var producer = new ProducerBuilder<Null, Carrier>(_producerConfig).SetValueSerializer(new AvroSerializer<Carrier>(schemaRegistry, _serialiserConfig)).Build();
        var carrier = Carrier.Create(@event);
        var message = new Message<Null, Carrier>
        {
            Value = carrier
        };
        await producer.ProduceAsync(_topic, message);
        producer.Flush(TimeSpan.FromSeconds(2));
        sw.Stop();
        _logger.Information("Message {@message} transmitted to topic {Topic}, took {Time}", message, _topic, sw.Elapsed);
    }
}

internal class DishProducerHandler : ProducerHandler<Dish>
{
    public DishProducerHandler(string topic, ILogger logger) : base(topic, logger)
    {
    }

    public async Task ProduceAsync(Dish @event)
    {
        Stopwatch sw = Stopwatch.StartNew();
        using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
        using var producer = new ProducerBuilder<Null, Dish>(_producerConfig).SetValueSerializer(new AvroSerializer<Dish>(schemaRegistry, _serialiserConfig)).Build();
        var message = new Message<Null, Dish>
        {
            Value = @event
        };
        await producer.ProduceAsync(_topic, message);
        producer.Flush(TimeSpan.FromSeconds(2));
        sw.Stop();
        _logger.Information("Dish Message {@message} transmitted to topic {Topic}, took {Time}", message, _topic, sw.Elapsed);
    }
}
