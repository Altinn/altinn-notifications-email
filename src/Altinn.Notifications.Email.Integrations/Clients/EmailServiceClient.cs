using Altinn.Notifications.Email.Core;
using Altinn.Notifications.Email.Core.Enums;
using Altinn.Notifications.Email.Integrations.Configuration;

using Azure;
using Azure.Communication.Email;

namespace Altinn.Notifications.Email.Integrations.Clients;

/// <summary>
/// Represents an implementation of <see cref="IEmailServiceClient"/> that will use Azure Communication
/// Services to produce an email.
/// </summary>
public class EmailServiceClient : IEmailServiceClient
{
    private readonly EmailClient _emailClient;

    /// <summary>
    /// Initializes a new instance of the <see cref="EmailServiceClient"/> class.
    /// </summary>
    /// <param name="communicationServicesSettings">Settings for integration against Communication Services.</param>
    public EmailServiceClient(CommunicationServicesSettings communicationServicesSettings)
    {
        _emailClient = new EmailClient(communicationServicesSettings.ConnectionString);
    }

    /// <summary>
    /// Send an email
    /// </summary>
    /// <param name="email">The email</param>
    /// <returns>A Task representing the asyncrhonous operation.</returns>
    public async Task<string> SendEmail(Core.Models.Email email)
    {
        EmailContent emailContent = new(email.Subject);
        switch (email.ContentType)
        {
            case EmailContentType.Plain:
                emailContent.PlainText = email.Body;
                break;
            case EmailContentType.Html:
                emailContent.Html = email.Body;
                break;
            default:
                break;
        }

        EmailMessage emailMessage = new(email.FromAddress, email.ToAddress, emailContent);

        EmailSendOperation emailSendOperation = await _emailClient.SendAsync(WaitUntil.Started, emailMessage);
        return emailSendOperation.Id;
    }

    /// <summary>
    /// Check the email sending operation status
    /// </summary>
    /// <returns></returns>
    public async Task<Core.Enums.EmailSendResult> GetOperationUpdate(string operationId)
    {
        var operation = new EmailSendOperation(operationId, _emailClient);
        await operation.UpdateStatusAsync();
        if (operation.HasCompleted && operation.HasValue)
        {
            if (operation.Value.Status == EmailSendStatus.Succeeded)
            {
                return Core.Enums.EmailSendResult.Succeeded;
            }
            else if (operation.Value.Status == EmailSendStatus.Failed || operation.Value.Status == EmailSendStatus.Canceled)
            {
                var response = operation.WaitForCompletionResponse();

                // TODO: check the reasons for failure to create reasonable types
                Console.WriteLine(response.ReasonPhrase);
                return Core.Enums.EmailSendResult.Failed;
            }
        }

        return Core.Enums.EmailSendResult.Sending;
    }
}
