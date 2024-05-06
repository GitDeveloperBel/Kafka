using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Serilog;

namespace Shared;

internal class ProducerHandler<T>
{
    private readonly string _topic;
    private readonly SchemaRegistryConfig _schemaRegistryConfig;
    private readonly ProducerConfig _producerConfig;
    private readonly AvroSerializerConfig _serialiserConfig;
    private readonly ILogger _logger;

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

    public async Task ProduceAsync(T @event)
    {
        using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
        using var producer = new ProducerBuilder<Null, Carrier>(_producerConfig).SetValueSerializer(new AvroSerializer<Carrier>(schemaRegistry, _serialiserConfig)).Build();
        var message = new Message<Null, Carrier>
        {
            Value = Carrier.Create(@event)
        };
        await producer.ProduceAsync(_topic, message);
        producer.Flush(TimeSpan.FromSeconds(2));
    }
}
