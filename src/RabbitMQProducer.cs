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
        private IModel? _model;

        /// <summary>
        /// RabbitMQProducer
        /// </summary>
        public RabbitMQProducer(string connectionString, string queueName)
        {
            this.ConnectionString = connectionString;
            this.QueueName = queueName;

            this.Init();
        }

        private void Init()
        {
            this.Close();

            this._connection = new ConnectionFactory
            {
                Uri = new(this.ConnectionString)
            }.CreateConnection();

            this._model = _connection.CreateModel();
            this._model.QueueDeclare(queue: this.QueueName, durable: false, exclusive: false, autoDelete: false, arguments: null);
        }
        private void Close()
        {
            if (_model != null && _model.IsOpen)
            {
                _model.Close();
                _model = null;
            }
            if (_connection != null && _connection.IsOpen)
            {
                _connection.Close();
                _connection = null;
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
            if (_model == null)
                return "";

            _model.BasicPublish(string.Empty, this.QueueName, null, Encoding.UTF8.GetBytes(data));

            return "";
        }
    }
}