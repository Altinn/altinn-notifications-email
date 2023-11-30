﻿using System.Diagnostics.CodeAnalysis;
using System.Text.RegularExpressions;
using Altinn.Notifications.Email.Core.Dependencies;
using Altinn.Notifications.Email.Core.Models;
using Altinn.Notifications.Email.Core.Sending;
using Altinn.Notifications.Email.Integrations.Configuration;

using Azure;
using Azure.Communication.Email;

using Microsoft.Extensions.Logging;

namespace Altinn.Notifications.Email.Integrations.Clients;

/// <summary>
/// Represents an implementation of <see cref="IEmailServiceClient"/> that will use Azure Communication
/// Services to produce an email.
/// </summary>
[ExcludeFromCodeCoverage]
public class EmailServiceClient : IEmailServiceClient
{
    private readonly EmailClient _emailClient;
    private readonly ILogger<IEmailServiceClient> _logger;

    private readonly string _failedInvalidEmailFormatErrorMessage = "Invalid format for email address";

    /// <summary>
    /// Initializes a new instance of the <see cref="EmailServiceClient"/> class.
    /// </summary>
    /// <param name="communicationServicesSettings">Settings for integration against Communication Services.</param>
    /// <param name="logger">A logger</param>
    public EmailServiceClient(CommunicationServicesSettings communicationServicesSettings, ILogger<IEmailServiceClient> logger)
    {
        _emailClient = new EmailClient(communicationServicesSettings.ConnectionString);
        _logger = logger;
    }

    /// <summary>
    /// Send an email
    /// </summary>
    /// <param name="email">The email</param>
    /// <returns>A Task representing the asyncrhonous operation.</returns>
    public async Task<Result<string, EmailSendFailResponse>> SendEmail(Core.Sending.Email email)
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
        try
        {
            EmailSendOperation emailSendOperation = await _emailClient.SendAsync(WaitUntil.Started, emailMessage);

            return emailSendOperation.Id;
        }
        catch (RequestFailedException e)
        {
            _logger.LogError(e, "// EmailServiceClient // SendEmail // Failed to send email, NotificationId {NotificationId}", email.NotificationId);
            EmailSendFailResponse emailSendFailResponse = new();

            if (e.ErrorCode == "TooManyRequests")
            {
                Regex regex = new(@"\d+", RegexOptions.None, TimeSpan.FromMilliseconds(10));
                Match match = regex.Match(e.Message);

                emailSendFailResponse.SendDelay = match.Success ? match.Value : "60";
                emailSendFailResponse.SendResult = Core.Status.EmailSendResult.Failed_TransientError;
            }
            else if (e.Message.Contains(_failedInvalidEmailFormatErrorMessage))
            {
                emailSendFailResponse.SendResult = Core.Status.EmailSendResult.Failed_InvalidEmailFormat;
            }
            else
            {
                emailSendFailResponse.SendResult = Core.Status.EmailSendResult.Failed;
            }

            return emailSendFailResponse;
        }
    }

    /// <summary>
    /// Check the email sending operation status
    /// </summary>
    /// <returns>An email send result</returns>
    public async Task<Core.Status.EmailSendResult> GetOperationUpdate(string operationId)
    {
        var operation = new EmailSendOperation(operationId, _emailClient);
        try
        {
            await operation.UpdateStatusAsync();

            if (operation.HasCompleted && operation.HasValue)
            {
                var status = operation.Value.Status;
                if (status == EmailSendStatus.Succeeded)
                {
                    return Core.Status.EmailSendResult.Succeeded;
                }
                else if (status == EmailSendStatus.Failed || status == EmailSendStatus.Canceled)
                {
                    // TODO: check the reasons for failure to create reasonable types
                    var response = operation.WaitForCompletionResponse();
                    _logger.LogError(
                        "// EmailServiceClient // GetOperationUpdate // Operation {OperationId} failed with status {Status} and reason phrase {Reason}",
                        operationId,
                        status,
                        response.ReasonPhrase);
                    return Core.Status.EmailSendResult.Failed;
                }
            }
        }
        catch (RequestFailedException e)
        {
            _logger.LogError(e, "// EmailServiceClient // GetOperationUpdate // Exception thrown when getting status, OperationId {OperationId}", operationId);
            return Core.Status.EmailSendResult.Failed;
        }

        return Core.Status.EmailSendResult.Sending;
    }
}
