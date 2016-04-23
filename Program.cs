using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Messaging;
using System.Threading;
using System.Collections.Concurrent;

namespace MultiMessageQueue
{
	class Program
	{
		static void Main(string[] args)
		{
			MultiMessageQueue queue = new MultiMessageQueue("wil097\\TestQueue", "wil097\\TestQueue1");

			queue.Send(new string[] { "wil097\\TestQueue", "wil097\\TestQueue1" }, "Test message " + Thread.CurrentThread.ManagedThreadId);
			//new Thread(() =>
			//{
			//	Thread.Sleep(10000);
			//	queue.Send("wil097\\TestQueue", "Test message " + Thread.CurrentThread.ManagedThreadId);

			//}).Start();

			object data = queue.ReceiveAny();
			//object data = queue.Receive("wil097\\TestQueue1");
			//Console.WriteLine("Data: {0}", data.ToString());
			//ConcurrentDictionary<string, object> data = queue.ReceiveAll();
		}
	}

	class MultiMessageQueue
	{
		// this class will handle more than one message queue at a time.
		// it will wait to receive message from any of the queue
		private ManualResetEvent[] _resetEvent = new ManualResetEvent[1];
		private Dictionary<Guid, IAsyncResult> _asyncResults = new Dictionary<Guid, IAsyncResult>();

		private QueueDataCollection _queueDataCol = null;

		public MultiMessageQueue(params string[] paths)
		{
			this._resetEvent[0] = new ManualResetEvent(false);
			this._queueDataCol = new QueueDataCollection();

			paths.AsParallel().ForAll((item) =>
			{
				MessageQueue queue = new MessageQueue(item);

				this._queueDataCol.Add(new QueueData { Name = item, Queue = queue, Message = null });
			});
		}

		public object ReceiveAny()
		{
			// Start receive for all the message queues.
			foreach (var queueData in this._queueDataCol.GetAll())
			{
				queueData.Queue.ReceiveCompleted += queue_ReceiveCompleted;
				IAsyncResult result = queueData.Queue.BeginReceive();
				this._asyncResults.Add(queueData.Queue.Id, result);
			}

			// Wait for any completion
			WaitHandle.WaitAny(this._resetEvent);

			this.AbortInCompletedRequest();

			// Fetch message
			var message = (from item in this._queueDataCol.GetAll()
						   where item.Message != null
						   select item).FirstOrDefault();

			return message.Message.Body;
		}

		public object Receive(string name)
		{
			return this._queueDataCol[name].Queue.Receive().Body;
		}

		public ConcurrentDictionary<string, object> ReceiveAll()
		{
			ConcurrentDictionary<string, object> result = new ConcurrentDictionary<string, object>();

			this._queueDataCol.GetAll().AsParallel().ForAll((data) => {

				var message = data.Queue.Receive();
				result.TryAdd(data.Name, message.Body);
			});

			return result;
		}

		public void Send(string[] queues, string message)
		{
			foreach (var item in queues)
			{
				this._queueDataCol[item].Queue.Send(message);
			}
		}

		void queue_ReceiveCompleted(object sender, ReceiveCompletedEventArgs e)
		{
			if (!e.AsyncResult.AsyncWaitHandle.SafeWaitHandle.IsClosed)
			{
				MessageQueue queue = sender as MessageQueue;
				this._queueDataCol[queue.Path].Message = e.Message;
				this._asyncResults[queue.Id] = e.AsyncResult;
			}

			this.SetEvent();
		}

		private void SetEvent()
		{
			if (!this._resetEvent[0].WaitOne(0))
			{
				this._resetEvent[0].Set();
			}
		}

		void AbortInCompletedRequest()
		{
			// End all the pending request.
			foreach (var item in this._asyncResults)
			{
				if (!item.Value.IsCompleted)
				{
					item.Value.AsyncWaitHandle.WaitOne(TimeSpan.FromMilliseconds(0));
				}

				item.Value.AsyncWaitHandle.Close();
			}
		}

		class QueueData
		{
			// stores all the queue, id, and name
			public MessageQueue Queue;
			public Message Message;
			public string Name;
		}

		class QueueDataCollection
		{
			public List<QueueData> _queueData = null;
			public QueueDataCollection()
			{
				this._queueData = new List<QueueData>();
			}

			public QueueDataCollection(List<QueueData> queueData)
			{
				this._queueData = queueData;
			}

			public QueueData this[string name]
			{
				get
				{
					return this.Get(name);
				}
			}

			public QueueData this[Guid guid]
			{
				get
				{
					return this.Get(guid);
				}
			}

			public void Add(QueueData data)
			{
				this._queueData.Add(data);
			}

			private QueueData Get(string name)
			{
				var queueData = (from data in this._queueData
								 where data.Name == name
								 select data).SingleOrDefault();

				return queueData;
			}

			private QueueData Get(Guid guid)
			{
				var queueData = (from data in this._queueData
								 where data.Queue.Id == guid
								 select data).SingleOrDefault();

				return queueData;
			}

			public ICollection<QueueData> GetAll()
			{
				return this._queueData;
			}
		}
	}
}