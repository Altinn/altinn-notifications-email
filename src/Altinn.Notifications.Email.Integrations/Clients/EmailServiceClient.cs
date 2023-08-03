using Altinn.Notifications.Email.Core;

namespace Altinn.Notifications.Email.Integrations.Clients;

/// <summary>
/// Represents an implementation of <see cref="IEmailServiceClient"/> that will use Azure Communication
/// Services to produce an email.
/// </summary>
public class EmailServiceClient : IEmailServiceClient
{
    /// <summary>
    /// Send an email
    /// </summary>
    /// <param name="email">The email</param>
    /// <returns>A Task representing the asyncrhonous operation.</returns>
    /// <exception cref="NotImplementedException">Implementation pending</exception>
    public Task SendEmail(Core.Models.Email email)
    {
        throw new NotImplementedException();
    }
}
