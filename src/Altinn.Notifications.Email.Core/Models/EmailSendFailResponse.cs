using Altinn.Notifications.Email.Core.Status;

namespace Altinn.Notifications.Email.Core.Models;

/// <summary>
/// A class representing a email send response object
/// </summary>
public class EmailSendFailResponse
{
    /// <summary>
    /// Result for the email send operation
    /// </summary>
    public EmailSendResult? SendResult { get; set; }

    /// <summary>
    /// The delay in seconds before Azure Commuuication Services can receive new emails
    /// </summary>
    public string? SendDelay { get; set; }
}
