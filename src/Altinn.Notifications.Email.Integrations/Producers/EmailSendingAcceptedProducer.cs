using Altinn.Notifications.Email.Integrations.Configuration;

namespace Altinn.Notifications.Email.Integrations.Producers;

/// <summary>
/// Implementation of a Kafka producer
/// </summary>
public sealed class EmailSendingAcceptedProducer : IEmailSendingAcceptedProducer
{
    private readonly KafkaSettings _kafkaSettings;
    private readonly ICommonProducer _commonProducer;

    /// <summary>
    /// Initializes a new instance of the <see cref="EmailSendingAcceptedProducer"/> class.
    /// </summary>
    public EmailSendingAcceptedProducer(
        KafkaSettings kafkaSettings, ICommonProducer commonProducer)
    {
        _kafkaSettings = kafkaSettings;
        _commonProducer = commonProducer;
    }

    /// <inheritdoc/>
    public async Task<bool> ProduceAsync(string message)
    {
        return await _commonProducer.ProduceAsync(
            _kafkaSettings.EmailSendingAcceptedProducerSettings.TopicName, message);
    }
}