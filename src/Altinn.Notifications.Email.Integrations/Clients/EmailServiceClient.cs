using Altinn.Notifications.Email.Core;
using Altinn.Notifications.Email.Integrations.Configuration;
using Altinn.Notifications.Email.Integrations.Producers;

using Azure.Communication.Email;

using Microsoft.Extensions.Logging;

namespace Altinn.Notifications.Email.Integrations.Clients;

/// <summary>
/// Represents an implementation of <see cref="IEmailServiceClient"/> that will use Azure Communication
/// Services to produce an email.
/// </summary>
public class EmailServiceClient : IEmailServiceClient
{
    private readonly CommunicationServicesSettings _communicationServicesSettings;
    private readonly IEmailSendingAcceptedProducer _producer;
    private readonly EmailClient _emailClient;
    private readonly ILogger<EmailServiceClient> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="EmailServiceClient"/> class.
    /// </summary>
    /// <param name="communicationServicesSettings">Settings for integration against Communication Services.</param>
    /// <param name="producer">A producer that can write a string to a KafkaTopic.</param>
    /// <param name="logger">A logger the class can use for logging.</param>
    public EmailServiceClient(
        CommunicationServicesSettings communicationServicesSettings, IEmailSendingAcceptedProducer producer, ILogger<EmailServiceClient> logger)
    {
        _communicationServicesSettings = communicationServicesSettings;
        _producer = producer;
        _emailClient = new EmailClient(_communicationServicesSettings.ConnectionString);
        _logger = logger;
    }

    /// <summary>
    /// Send an email
    /// </summary>
    /// <param name="email">The email</param>
    /// <returns>A Task representing the asyncrhonous operation.</returns>
    public async Task SendEmail(Core.Models.Email email)
    {
        EmailContent emailContent = new EmailContent(email.Subject);
        switch (email.ContentType)
        {
            case Core.Models.EmailContentType.Plain:
                emailContent.PlainText = email.Body;
                break;
            case Core.Models.EmailContentType.Html:
                emailContent.Html = email.Body;
                break;
            default:
                break;
        }

        EmailMessage emailMessage = new EmailMessage(email.FromAddress, email.ToAddress, emailContent);
        EmailSendOperation emailSendOperation = await _emailClient.SendAsync(Azure.WaitUntil.Completed, emailMessage);

        await _producer.ProduceAsync(emailSendOperation.Id);
    }
}
