using System;
using Google.Protobuf;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using NPitaya.Models;
using NPitaya.Serializer;
using NPitaya.Protos;
using NPitaya.Metrics;
using static NPitaya.Utils.Utils;

// TODO profiling
// TODO better reflection performance in task async call
// TODO support to sync methods
namespace NPitaya
{
    // Allows making RPC calls to other Pitaya servers.
    public partial class PitayaCluster
    {
        private static ISerializer _serializer = new JSONSerializer();
        static ProtobufSerializer _remoteSerializer = new ProtobufSerializer();
        public delegate string RemoteNameFunc(string methodName);
        delegate void OnSignalFunc();
        static readonly Dictionary<string, RemoteMethod> RemotesDict = new Dictionary<string, RemoteMethod>();
        static readonly Dictionary<string, RemoteMethod> HandlersDict = new Dictionary<string, RemoteMethod>();
        static IntPtr pitaya;
        static HandleRpcCallbackFunc _handleRpcCallback;
        static ClusterNotificationCallbackFunc _clusterNotificationCallback;
        static LogFunction _logFunctionCallback;
        static RpcClient _rpcClient;
        static Action _onSignalEvent;
        static MetricsReporter? _metricsReporter;

        public enum ServiceDiscoveryAction
        {
            ServerAdded,
            ServerRemoved,
        }

        public class ServiceDiscoveryListener
        {
            public Action<ServiceDiscoveryAction, Server> onServer;
            public ServiceDiscoveryListener(Action<ServiceDiscoveryAction, Server> onServer)
            {
                Debug.Assert(onServer != null);
                this.onServer = onServer;
            }
        }

        static ServiceDiscoveryListener? _serviceDiscoveryListener;

        public static void AddSignalHandler(Action cb)
        {
            _onSignalEvent += cb;
        }

        static void OnSignal()
        {
            Logger.Info("Invoking signal handler");
            _onSignalEvent?.Invoke();
        }

        static void ClusterNotificationCallback(IntPtr userData, NotificationType notificationType, IntPtr serverHandle)
        {
            if (_serviceDiscoveryListener == null)
            {
                return;
            }

            var server = new Server(serverHandle);
            _serviceDiscoveryListener.onServer(
                notificationType == NotificationType.ServerAdded
                    ? ServiceDiscoveryAction.ServerAdded
                    : ServiceDiscoveryAction.ServerRemoved,
                server
            );
        }

        static void HandleRpcCallback(IntPtr userData, IntPtr rpc)
        {
            Int32 len;
            IntPtr rawData = pitaya_rpc_request(rpc, out len);
            byte[] data = GetDataFromRawPointer(rawData, len);

            try
            {
                var req = new Protos.Request();
                req.MergeFrom(new CodedInputStream(data));

                DispatchRpc(_rpcClient, rpc, req);
            }
            catch (Exception e)
            {
                Logger.Error("Failed to decode request, error:{0}", e.Message);
            }
        }

        public static void Initialize(string envPrefix,
                                      string configFile,
                                      Server serverInfo,
                                      NativeLogLevel logLevel,
                                      NativeLogKind logKind,
                                      Action<string> logFunction,
                                      MetricsConfiguration metricsConfig,
                                      ServiceDiscoveryListener serviceDiscoveryListener = null)
        {
            _serviceDiscoveryListener = serviceDiscoveryListener;
            _handleRpcCallback = new HandleRpcCallbackFunc(HandleRpcCallback);
            _clusterNotificationCallback = new ClusterNotificationCallbackFunc(ClusterNotificationCallback);
            _logFunctionCallback = new LogFunction(LogFunctionCallback);
            var logCtx = GCHandle.Alloc(logFunction, GCHandleType.Normal);
            var pitayaMetrics = IntPtr.Zero;
            if (metricsConfig.IsEnabled)
            {
                _metricsReporter = new MetricsReporter(metricsConfig);
                pitayaMetrics = _metricsReporter.GetPitayaPtr();
            }

            IntPtr err = pitaya_initialize_with_nats(
                IntPtr.Zero,
                envPrefix,
                configFile,
                Marshal.GetFunctionPointerForDelegate(_handleRpcCallback),
                Marshal.GetFunctionPointerForDelegate(_clusterNotificationCallback),
                logLevel,
                logKind,
                Marshal.GetFunctionPointerForDelegate(_logFunctionCallback),
                GCHandle.ToIntPtr(logCtx),
                pitayaMetrics,
                serverInfo.Handle,
                out pitaya
            );

            if (err != IntPtr.Zero)
            {
                var pitayaError = new PitayaError(pitaya_error_code(err), pitaya_error_message(err));
                pitaya_error_drop(err);
                throw new PitayaException($"Initialization failed: code={pitayaError.Code} msg={pitayaError.Message}");
            }

            _rpcClient = new RpcClient(pitaya, _serializer);
            _metricsReporter?.Start();
        }

        static void LogFunctionCallback(IntPtr ctx, IntPtr msg)
        {
            if (ctx == IntPtr.Zero)
            {
                return;
            }
            var handle = GCHandle.FromIntPtr(ctx);
            var logFn = (Action<string>)handle.Target;
            var msgStr = Marshal.PtrToStringAnsi(msg);
            logFn(msgStr);
        }

        public static void RegisterRemote(BaseRemote remote)
        {
            string className = DefaultRemoteNameFunc(remote.GetName());
            RegisterRemote(remote, className, DefaultRemoteNameFunc);
        }

        public static void RegisterRemote(BaseRemote remote, string name)
        {
            RegisterRemote(remote, name, DefaultRemoteNameFunc);
        }

        public static void RegisterRemote(BaseRemote remote, string name, RemoteNameFunc remoteNameFunc) // TODO remote function name func
        {
            Dictionary<string, RemoteMethod> m = remote.GetRemotesMap();
            foreach (KeyValuePair<string, RemoteMethod> kvp in m)
            {
                var rn = remoteNameFunc(kvp.Key);
                var remoteName = $"{name}.{rn}";
                if (RemotesDict.ContainsKey(remoteName))
                {
                    throw new PitayaException($"tried to register same remote twice! remote name: {remoteName}");
                }

                Logger.Info("registering remote {0}", remoteName);
                RemotesDict[remoteName] = kvp.Value;
            }
        }

        public static void RegisterHandler(BaseHandler handler)
        {
            string className = DefaultRemoteNameFunc(handler.GetName());
            RegisterHandler(handler, className, DefaultRemoteNameFunc);
        }

        public static void RegisterHandler(BaseHandler handler, string name)
        {
            RegisterHandler(handler, name, DefaultRemoteNameFunc);
        }

        public static void RegisterHandler(BaseHandler handler, string name, RemoteNameFunc remoteNameFunc)
        {
            Dictionary<string, RemoteMethod> m = handler.GetRemotesMap();
            foreach (KeyValuePair<string, RemoteMethod> kvp in m)
            {
                var rn = remoteNameFunc(kvp.Key);
                var handlerName = $"{name}.{rn}";
                if (HandlersDict.ContainsKey(handlerName))
                {
                    throw new PitayaException($"tried to register same remote twice! remote name: {handlerName}");
                }

                Logger.Info("registering handler {0}", handlerName);
                HandlersDict[handlerName] = kvp.Value;
            }
        }

        public static void SetSerializer(ISerializer s)
        {
            _serializer = s;
        }

        public static void WaitShutdownSignal()
        {
            pitaya_wait_shutdown_signal(pitaya);
        }

        public static void Terminate()
        {
            pitaya_shutdown(pitaya);
            pitaya = IntPtr.Zero;
        }

        class ServerIdContext
        {
            public TaskCompletionSource<Server> t;
            public string serverId;
        }

        static void GetServerByIdCallback(IntPtr userData, IntPtr serverHandle)
        {
            var handle = GCHandle.FromIntPtr(userData);
            var context = (ServerIdContext)handle.Target;

            if (serverHandle == IntPtr.Zero)
            {
                Logger.Error($"There are no servers with id {context.serverId}");
                context.t.TrySetResult(null);
                return;
            }

            var sv = new Server(serverHandle);
            context.t.TrySetResult(sv);
        }

        public static Task<Server> GetServerById(string serverId, string serverKind)
        {
            return Task.Run(() =>
            {
                var context = new ServerIdContext
                {
                    t = new TaskCompletionSource<Server>(),
                    serverId = serverId,
                };

                var handle = GCHandle.Alloc(context, GCHandleType.Normal);
                var callback = new ServerByIdCallback(GetServerByIdCallback);

                pitaya_server_by_id(pitaya, serverId, serverKind, callback, GCHandle.ToIntPtr(handle));

                return context.t.Task;
            });
        }

        public static Task SendPushToUser(
            string frontendId,
            string serverKind,
            string route,
            string uid,
            object pushMsg)
        {
            return _rpcClient.SendPushToUser(frontendId, serverKind, route, uid, pushMsg);
        }

        public static Task SendKickToUser(string frontendId, string serverKind, KickMsg kick)
        {
            return _rpcClient.SendKickToUser(frontendId, serverKind, kick);
        }

        public static Task<T> Rpc<T>(string serverId, Route route, IMessage msg)
        {
            return _rpcClient.Rpc<T>(serverId, route, msg);
        }

        public static Task<T> Rpc<T>(Route route, IMessage msg)
        {
            return Rpc<T>("", route, msg);
        }

        public static void IncCounter(string name, string[]? labels = null)
        {
            _metricsReporter?.IncCounter(name, labels);
        }

        public static void SetGauge(string name, float value, string[]? labels = null)
        {
            _metricsReporter?.SetGauge(name, value, labels);
        }

        public static void ObserveHistogram(string name, float value, string[]? labels = null)
        {
            _metricsReporter?.ObserveHistogram(name, value, labels);
        }

        static void OnServerAddedOrRemovedNativeCb(int serverAdded, IntPtr serverPtr, IntPtr user)
        {
            var pitayaClusterHandle = (GCHandle)user;
            var serviceDiscoveryListener = pitayaClusterHandle.Target as ServiceDiscoveryListener;

            if (serviceDiscoveryListener == null)
            {
                Logger.Warn("The service discovery listener is null!");
                return;
            }

            var server = (Server)Marshal.PtrToStructure(serverPtr, typeof(Server));
            if (serverAdded == 1)
                serviceDiscoveryListener.onServer(ServiceDiscoveryAction.ServerAdded, server);
            else
                serviceDiscoveryListener.onServer(ServiceDiscoveryAction.ServerRemoved, server);
        }
    }
}