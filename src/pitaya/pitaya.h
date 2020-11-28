#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct Pitaya Pitaya;

typedef enum {
    PitayaClusterNotification_ServerAdded = 0,
    PitayaClusterNotification_ServerRemoved = 1,
} PitayaClusterNotification;

typedef enum {
    PitayaLogKind_Console = 0,
    PitayaLogKind_Json = 1,
    PitayaLogKind_Function = 2,
} PitayaLogKind;

typedef enum {
    PitayaLogLevel_Trace = 0,
    PitayaLogLevel_Debug = 1,
    PitayaLogLevel_Info = 2,
    PitayaLogLevel_Warn = 3,
    PitayaLogLevel_Error = 4,
    PitayaLogLevel_Critical = 5,
} PitayaLogLevel;

/**
 * Pitaya represent a pitaya server.
 *
 * It will registers itself using a service discovery client in order
 * to be discovered and send RPCs to other Pitaya servers.
 */
typedef struct Pitaya Pitaya;

typedef struct PitayaBuffer PitayaBuffer;

typedef struct PitayaError PitayaError;

typedef struct PitayaMetricsReporter PitayaMetricsReporter;

typedef struct PitayaRpc PitayaRpc;

typedef struct PitayaServerInfo PitayaServerInfo;

typedef void (*PitayaHandleRpcCallback)(void*, PitayaRpc*);

typedef void (*PitayaClusterNotificationCallback)(void*, PitayaClusterNotification, PitayaServerInfo*);

typedef struct {
    const char *kind;
    double start;
    double inc;
    uint32_t count;
} PitayaHistBucketOpts;

typedef struct {
    const char *namespace_;
    const char *subsystem;
    const char *name;
    const char *help;
    const char **variable_labels;
    uint32_t variable_labels_count;
    PitayaHistBucketOpts buckets;
} PitayaMetricsOpts;

typedef void (*PitayaRegisterFn)(void *user_data, PitayaMetricsOpts opts);

typedef void (*PitayaIncCounterFn)(void *user_date, const char *name, const char **labels, uint32_t labels_count);

typedef void (*PitayaObserveHistFn)(void *user_data, const char *name, double value, const char **labels, uint32_t labels_count);

typedef void (*PitayaSetGaugeFn)(void *user_data, const char *name, double value, const char **labels, uint32_t labels_count);

typedef void (*PitayaAddGaugeFn)(void *user_data, const char *name, double value, const char **labels, uint32_t labels_count);

const uint8_t *pitaya_buffer_data(PitayaBuffer *buf, int32_t *len);

void pitaya_buffer_drop(PitayaBuffer *buf);

PitayaBuffer *pitaya_buffer_new(const uint8_t *data, int32_t len);

const char *pitaya_error_code(PitayaError *err);

void pitaya_error_drop(PitayaError *error);

const char *pitaya_error_message(PitayaError *err);

PitayaError *pitaya_initialize_with_nats(void *user_ctx,
                                         char *env_prefix,
                                         char *config_file,
                                         PitayaHandleRpcCallback handle_rpc_cb,
                                         PitayaClusterNotificationCallback cluster_notification_callback,
                                         PitayaLogLevel log_level,
                                         PitayaLogKind log_kind,
                                         void (*log_function)(void*, char*),
                                         void *log_ctx,
                                         PitayaMetricsReporter *raw_metrics_reporter,
                                         PitayaServerInfo *server_info,
                                         Pitaya **pitaya);

void pitaya_metrics_add_gauge(Pitaya *p,
                              char *name,
                              double value,
                              char **labels,
                              uint32_t labels_count,
                              void (*callback)(void*),
                              void *user_data);

void pitaya_metrics_inc_counter(Pitaya *p,
                                char *name,
                                char **labels,
                                uint32_t labels_count,
                                void (*callback)(void*),
                                void *user_data);

void pitaya_metrics_observe_hist(Pitaya *p,
                                 char *name,
                                 double value,
                                 char **labels,
                                 uint32_t labels_count,
                                 void (*callback)(void*),
                                 void *user_data);

void pitaya_metrics_reporter_drop(PitayaMetricsReporter *ptr);

PitayaMetricsReporter *pitaya_metrics_reporter_new(PitayaRegisterFn register_counter_fn,
                                                   PitayaRegisterFn register_histogram_fn,
                                                   PitayaRegisterFn register_gauge_fn,
                                                   PitayaIncCounterFn inc_counter_fn,
                                                   PitayaObserveHistFn observe_hist_fn,
                                                   PitayaSetGaugeFn set_gauge_fn,
                                                   PitayaAddGaugeFn add_gauge_fn,
                                                   void *user_data);

void pitaya_metrics_set_gauge(Pitaya *p,
                              char *name,
                              double value,
                              char **labels,
                              uint32_t labels_count,
                              void (*callback)(void*),
                              void *user_data);

void pitaya_rpc_drop(PitayaRpc *rpc);

uint8_t *pitaya_rpc_request(PitayaRpc *rpc, int32_t *len);

PitayaError *pitaya_rpc_respond(PitayaRpc *rpc, const uint8_t *response_data, int32_t response_len);

void pitaya_send_kick(Pitaya *p,
                      char *server_id,
                      char *server_kind,
                      PitayaBuffer *kick_buffer,
                      void (*callback)(void*, PitayaError*, PitayaBuffer*),
                      void *user_data);

void pitaya_send_push_to_user(Pitaya *p,
                              char *_server_id,
                              char *server_kind,
                              PitayaBuffer *push_buffer,
                              void (*callback)(void*, PitayaError*),
                              void *user_data);

void pitaya_send_rpc(Pitaya *p,
                     char *server_id,
                     char *route_str,
                     PitayaBuffer *request_buffer,
                     void (*callback)(void*, PitayaError*, PitayaBuffer*),
                     void *user_data);

void pitaya_server_by_id(Pitaya *p,
                         char *server_id,
                         char *server_kind,
                         void (*callback)(void*, PitayaServerInfo*),
                         void *user_data);

void pitaya_server_drop(PitayaServerInfo *pitaya_server);

int32_t pitaya_server_frontend(PitayaServerInfo *server);

const char *pitaya_server_hostname(PitayaServerInfo *server);

const char *pitaya_server_id(PitayaServerInfo *server);

const char *pitaya_server_kind(PitayaServerInfo *server);

const char *pitaya_server_metadata(PitayaServerInfo *server);

PitayaServerInfo *pitaya_server_new(char *id,
                                    char *kind,
                                    char *metadata,
                                    char *hostname,
                                    int32_t frontend);

void pitaya_shutdown(Pitaya *p);

void pitaya_wait_shutdown_signal(Pitaya *p);
