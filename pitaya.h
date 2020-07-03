#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef enum {
    PitayaClusterNotification_ServerAdded = 0,
    PitayaClusterNotification_ServerRemoved = 1,
} PitayaClusterNotification;

typedef enum {
    PitayaLogKind_Console = 0,
    PitayaLogKind_Json = 1,
} PitayaLogKind;

typedef enum {
    PitayaLogLevel_Trace = 0,
    PitayaLogLevel_Debug = 1,
    PitayaLogLevel_Info = 2,
    PitayaLogLevel_Warn = 3,
    PitayaLogLevel_Error = 4,
    PitayaLogLevel_Critical = 5,
} PitayaLogLevel;

typedef struct Pitaya Pitaya;

typedef struct PitayaBuffer PitayaBuffer;

typedef struct PitayaError PitayaError;

typedef struct PitayaRpc PitayaRpc;

typedef struct {
    char *addr;
    int64_t connection_timeout_ms;
    int32_t request_timeout_ms;
    int32_t server_shutdown_deadline_ms;
    int32_t server_max_number_of_rpcs;
    int32_t max_reconnection_attempts;
    int32_t max_pending_msgs;
} PitayaNATSConfig;

typedef struct {
    char *endpoints;
    char *etcd_prefix;
    char *server_type_filters;
    int32_t heartbeat_ttl_sec;
    int32_t log_heartbeat;
    int32_t log_server_sync;
    int32_t log_server_details;
    int32_t sync_servers_interval_sec;
    int32_t max_number_of_retries;
} PitayaSDConfig;

typedef struct {
    char *id;
    char *kind;
    char *metadata;
    char *hostname;
    int32_t frontend;
} PitayaServer;

typedef void (*PitayaHandleRpcCallback)(void*, PitayaRpc*);

typedef void (*PitayaClusterNotificationCallback)(void*, PitayaClusterNotification, void*);

const uint8_t *pitaya_buffer_data(PitayaBuffer *buf, int32_t *len);

void pitaya_buffer_drop(PitayaBuffer *buf);

PitayaBuffer *pitaya_buffer_new(const uint8_t *data, int32_t len);

const char *pitaya_error_code(PitayaError *err);

void pitaya_error_drop(PitayaError *error);

const char *pitaya_error_message(PitayaError *err);

PitayaError *pitaya_initialize_with_nats(PitayaNATSConfig *nc,
                                         PitayaSDConfig *sd_config,
                                         PitayaServer *sv,
                                         PitayaHandleRpcCallback handle_rpc_cb,
                                         void *handle_rpc_data,
                                         PitayaLogLevel log_level,
                                         PitayaLogKind log_kind,
                                         PitayaClusterNotificationCallback cluster_notification_callback,
                                         void *cluster_notification_data,
                                         Pitaya **pitaya);

void pitaya_rpc_drop(PitayaRpc *rpc);

uint8_t *pitaya_rpc_request(PitayaRpc *rpc, int32_t *len);

PitayaError *pitaya_rpc_respond(PitayaRpc *rpc, const uint8_t *response_data, int32_t response_len);

PitayaError *pitaya_send_rpc(Pitaya *pitaya_server,
                             char *server_id,
                             char *route,
                             PitayaBuffer *request_buffer,
                             PitayaBuffer **response_buffer);

bool pitaya_server_by_id(Pitaya *pitaya_server,
                         char *server_id,
                         char *server_kind,
                         PitayaServer *server);

void pitaya_server_drop(PitayaServer *pitaya_server);

void pitaya_shutdown(Pitaya *pitaya_server);

void pitaya_wait_shutdown_signal(Pitaya *pitaya_server);