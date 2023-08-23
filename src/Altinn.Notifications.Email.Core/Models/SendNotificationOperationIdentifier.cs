﻿using System.Text.Json;

namespace Altinn.Notifications.Email.Core.Models
{
    /// <summary>
    /// Class grouping identifiers of a notificaiton and operation
    /// </summary>
    public class SendNotificationOperationIdentifier
    {
        /// <summary>
        /// The notification id
        /// </summary>
        public Guid NotificationId { get; set; }

        /// <summary>
        /// The send operation id
        /// </summary>
        public string OperationId { get; set; } = string.Empty;

        /// <summary>
        /// Try to parse a json string into a<see cref="SendNotificationOperationIdentifier"/>
        /// </summary>
        public static bool TryParse(string input, out SendNotificationOperationIdentifier value)
        {
            SendNotificationOperationIdentifier? parsedOutput;
            value = new SendNotificationOperationIdentifier();

            if (string.IsNullOrEmpty(input))
            {
                return false;
            }

            try
            {
                parsedOutput = JsonSerializer.Deserialize<SendNotificationOperationIdentifier>(
                input!,
                new JsonSerializerOptions()
                {
                    PropertyNameCaseInsensitive = true
                });

                value = parsedOutput!;
                return value.NotificationId != Guid.Empty;
            }
            catch
            {
                // try parse, we simply return false if fails
            }

            return false;
        }
    }
}
