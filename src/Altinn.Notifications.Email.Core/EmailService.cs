using Microsoft.Extensions.Logging;

namespace Altinn.Notifications.Email.Core;

/// <summary>
/// Dummy class
/// </summary>
public class EmailService : IEmailService
{
    private readonly IEmailServiceClient _emailServiceClient;
    private readonly ILogger<EmailService> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="EmailService"/> class.
    /// </summary>
    /// <param name="emailServiceClient">A client that can perform actual mail sending.</param>
    /// <param name="logger">A logger</param>
    public EmailService(IEmailServiceClient emailServiceClient, ILogger<EmailService> logger)
    {
        _emailServiceClient = emailServiceClient;
        _logger = logger;
    }

    /// <inheritdoc/>
    public async Task SendEmail(Models.Email email)
    {
        await _emailServiceClient.SendEmail(email);
    }
}
