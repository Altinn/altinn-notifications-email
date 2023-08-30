using Altinn.Notifications.Email.Core;
using Altinn.Notifications.Email.Core.Dependencies;
using Altinn.Notifications.Email.Integrations.Configuration;
using Altinn.Notifications.Integrations.Kafka.Consumers;

using Microsoft.Extensions.Logging;

namespace Altinn.Notifications.Email.Integrations.Consumers;

/// <summary>
/// Kafka consumer class for handling the email queue.
/// </summary>
public sealed class EmailSendingConsumer : KafkaConsumerBase<EmailSendingConsumer>
{
    private readonly IEmailService _emailService;
    private readonly ICommonProducer _producer;
    private readonly string _retryTopicName;

    /// <summary>
    /// Initializes a new instance of the <see cref="EmailSendingConsumer"/> class.
    /// </summary>
    public EmailSendingConsumer(
        KafkaSettings kafkaSettings,
        IEmailService emailService,
        ICommonProducer producer,
        ILogger<EmailSendingConsumer> logger)
        : base(kafkaSettings, logger, kafkaSettings.SendEmailQueueTopicName)
    {
        _emailService = emailService;
        _producer = producer;
        _retryTopicName = kafkaSettings.SendEmailQueueTopicName;
    }

    /// <inheritdoc/>
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() => ConsumeMessage(ConsumeEmail, RetryEmail, stoppingToken), stoppingToken);
    }

    private async Task ConsumeEmail(string message)
    {
        bool succeeded = Core.Models.Email.TryParse(message, out Core.Models.Email email);

        if (!succeeded)
        {
            return;
        }

        await _emailService.SendAsync(email);
    }

    private async Task RetryEmail(string message)
    {
        // TODO: create seperate retry topic 
        await _producer.ProduceAsync(_retryTopicName, message);
    }
}