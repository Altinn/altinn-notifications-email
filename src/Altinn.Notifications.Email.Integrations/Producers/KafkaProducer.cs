using Altinn.Notifications.Email.Integrations.Configuration;

using Confluent.Kafka;

using Microsoft.Extensions.Logging;

namespace Altinn.Notifications.Email.Integrations.Producers;

/// <summary>
/// Implementation of a Kafka producer
/// </summary>
public class KafkaProducer : IDisposable
{
    private readonly IProducer<Null, string> _producer;
    private readonly KafkaSettings _kafkaSettings;
    private readonly ILogger<KafkaProducer> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaProducer"/> class.
    /// </summary>
    public KafkaProducer(KafkaSettings kafkaSettings, ILogger<KafkaProducer> logger)
    {
        _kafkaSettings = kafkaSettings;
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = _kafkaSettings.BrokerAddress,
            Acks = Acks.All,
            EnableDeliveryReports = true,
            EnableIdempotence = true,
            MessageSendMaxRetries = 3,
            RetryBackoffMs = 1000
        };

        _producer = new ProducerBuilder<Null, string>(config).Build();
    }

    /// <summary>
    /// Write a message to a Kafka topic.
    /// </summary>
    /// <param name="topic">The name of the topic to write to.</param>
    /// <param name="message">The string message to write.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    public async Task<bool> ProduceAsync(string topic, string message)
    {
        try
        {
            DeliveryResult<Null, string> result = await _producer.ProduceAsync(topic, new Message<Null, string>
            {
                Value = message
            });

            if (result.Status != PersistenceStatus.Persisted)
            {
                _logger.LogError("// KafkaProducer // ProduceAsync // Message not ack'd by all brokers (value: '{message}'). Delivery status: {result.Status}", message, result.Status);
                return false;
            }
        }
        catch (ProduceException<long, string> ex)
        {
            _logger.LogError(ex, "// KafkaProducer // ProduceAsync // Permanent error: {Message} for message (value: '{DeliveryResult}')", ex.Message, ex.DeliveryResult.Value);
            throw;
        }

        return true;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Flushs the producer
    /// </summary>
    protected virtual void Dispose(bool disposing)
    {
        _producer.Flush();
    }
}
