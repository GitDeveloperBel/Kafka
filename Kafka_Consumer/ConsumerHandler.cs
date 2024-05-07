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
    private readonly string _topic;
    private readonly SchemaRegistryConfig _schemaRegistryConfig;
    private readonly ConsumerConfig _consumerConfig;
    private readonly AvroDeserializerConfig _serialiserConfig;
    private readonly ILogger _logger;

    private readonly Action<T> _action;
    public ConsumerHandler(string topic, Action<T> handle, ILogger logger)
    {
        _topic = topic;
        _schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "localhost:8081",
        };
        _consumerConfig = new ConsumerConfig
        {
            GroupId = Guid.NewGuid().ToString(),
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Latest,
            //AllowAutoCreateTopics = true,
        };
        _serialiserConfig = new();
        _action = handle;
        _logger = logger;
    }

    public void Consume(CancellationTokenSource cts)
    {
        using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
        using var consumer = new ConsumerBuilder<Null, Carrier>(_consumerConfig).SetValueDeserializer(new AvroDeserializer<Carrier>(schemaRegistry, _serialiserConfig).AsSyncOverAsync()).Build();
        consumer.Subscribe(_topic);
        while (true)
        {
            var cr = consumer.Consume(cts.Token);
            var carrier = cr.Message.Value; // TODO: try catch
            _action.Invoke(JsonSerializer.Deserialize<T>(carrier.Data)!);
        }
        consumer.Close();
    }
}
