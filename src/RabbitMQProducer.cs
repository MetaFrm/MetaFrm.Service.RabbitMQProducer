using RabbitMQ.Client;
using System.Text;

namespace MetaFrm.Service
{
    /// <summary>
    /// RabbitMQProducer
    /// </summary>
    public class RabbitMQProducer : IServiceString, IDisposable
    {
        private string ConnectionString { get; set; }
        private string QueueName { get; set; }

        private IConnection? _connection;
        private IChannel? _channel;

        /// <summary>
        /// RabbitMQProducer
        /// </summary>
        public RabbitMQProducer(string connectionString, string queueName)
        {
            this.ConnectionString = connectionString;
            this.QueueName = queueName;
           
            Task.Run(() => this.Init());
        }

        private async Task Init()
        {
            this.Close();

            this._connection = await new ConnectionFactory
            {
                Uri = new(this.ConnectionString)
            }.CreateConnectionAsync();

            this._channel = await _connection.CreateChannelAsync();
            await this._channel.QueueDeclareAsync(queue: this.QueueName, durable: false, exclusive: false, autoDelete: false, arguments: null);
        }
        private void Close()
        {
            if (this._channel != null && this._channel.IsOpen)
            {
                this._channel.CloseAsync();
                this._channel = null;
            }
            if (this._connection != null && this._connection.IsOpen)
            {
                this._connection.CloseAsync();
                this._connection = null;
            }
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        /// <summary>
        /// Dispose
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
                this.Close();
        }

        //https://dotnetblog.asphostportal.com/how-to-make-sure-your-asp-net-core-keep-running-on-iis/
        string IServiceString.Request(string data)
        {
            Task.Run(() => this.BasicPublishAsync(data));
           
            return "";
        }
        private async void BasicPublishAsync(string data)
        {
            if (this._channel == null)
                return;

            await this._channel.BasicPublishAsync(exchange: string.Empty, routingKey: this.QueueName, body: Encoding.UTF8.GetBytes(data));
        }
    }
}