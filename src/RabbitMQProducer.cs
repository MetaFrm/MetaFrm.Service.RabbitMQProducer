using Microsoft.Extensions.Logging;
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

        private readonly SemaphoreSlim _initLock = new(1, 1);
        private bool _initialized;

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

            if (this._initialized) return;

            await this._initLock.WaitAsync();

            try
            {
                if (this._initialized) return;

                this.Close();

                this._connection = await new ConnectionFactory
                {
                    Uri = new(this.ConnectionString),
                    AutomaticRecoveryEnabled = true,
                    NetworkRecoveryInterval = TimeSpan.FromSeconds(10)
                }.CreateConnectionAsync();

                this._channel = await _connection.CreateChannelAsync();
                await this._channel.QueueDeclareAsync(queue: this.QueueName, durable: true, exclusive: false, autoDelete: false, arguments: null);

                this._initialized = true;
            }
            finally
            {
                this._initLock.Release();
            }
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
            _ = this.BasicPublishAsync(data);
            return string.Empty;
        }
        private async Task BasicPublishAsync(string data, int runCount = 1)
        {
            if (!this._initialized)
            {
                if (Factory.Logger.IsEnabled(LogLevel.Error))
                    Factory.Logger.LogError("Producer is not started. {runCount}", runCount);

                if (runCount <= 2)
                {
                    await Task.Delay(300);

                    //초기화 다시 시도
                    await this.Init();

                    await this.BasicPublishAsync(data, runCount + 1);
                    return;
                }
                else
                    throw new InvalidOperationException("Producer is not started.");
            }

            if (this._channel == null || !this._channel.IsOpen)
                return;

            var properties = new BasicProperties
            {
                Persistent = true// DeliveryMode = 2
            };

            await this._channel.BasicPublishAsync(exchange: string.Empty, routingKey: this.QueueName, mandatory: false, basicProperties: properties, body: Encoding.UTF8.GetBytes(data));
        }

        Task<string> IServiceString.RequestAsync(string data)
        {
            string result = ((IServiceString)this).Request(data);

            return Task.FromResult(result);
        }
    }
}