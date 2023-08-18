using Altinn.Notifications.Email.Core;

using Altinn.Notifications.Email.Integrations.Configuration;
using Altinn.Notifications.Integrations.Kafka.Consumers;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Altinn.Notifications.Email.Integrations.Consumers;

/// <summary>
/// Kafka consumer class for handling the email queue.
/// </summary>
public sealed class EmailSendingConsumer : KafkaConsumerBase<EmailSendingConsumer>
{
    private readonly IEmailService _emailService;

    /// <summary>
    /// Initializes a new instance of the <see cref="EmailSendingConsumer"/> class.
    /// </summary>
    public EmailSendingConsumer(
        KafkaSettings kafkaSettings,
        IEmailService emailService,
        ILogger<EmailSendingConsumer> logger)
        : base(kafkaSettings, logger, kafkaSettings.EmailSendingConsumerSettings.TopicName)
    {
        _emailService = emailService;
    }

    /// <inheritdoc/>
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() => ConsumeMessage(ConsumeEmail, RetryEmail, stoppingToken), stoppingToken);
    }

    private async Task ConsumeEmail(string message)
    {
        bool succeeded = Core.Models.Email.TryParse(message, out Email.Core.Models.Email email);

        if (!succeeded)
        {
            return;
        }

        await _emailService.SendEmail(email);
    }

    private Task RetryEmail(string message)
    {
        return Task.CompletedTask;
    }
}