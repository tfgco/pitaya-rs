/* Automatically generated nanopb header */
/* Generated by nanopb-0.4.2-dev */

#ifndef PB_PROTOS_DOC_PB_H_INCLUDED
#define PB_PROTOS_DOC_PB_H_INCLUDED
#include <pb.h>

#if PB_PROTO_HEADER_VERSION != 40
#error Regenerate this file with the current version of nanopb generator.
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* Struct definitions */
typedef struct _protos_Doc {
    pb_callback_t doc;
} protos_Doc;


/* Initializer values for message structs */
#define protos_Doc_init_default                  {{{NULL}, NULL}}
#define protos_Doc_init_zero                     {{{NULL}, NULL}}

/* Field tags (for use in manual encoding/decoding) */
#define protos_Doc_doc_tag                       1

/* Struct field encoding specification for nanopb */
#define protos_Doc_FIELDLIST(X, a) \
X(a, CALLBACK, SINGULAR, STRING,   doc,               1)
#define protos_Doc_CALLBACK pb_default_field_callback
#define protos_Doc_DEFAULT NULL

extern const pb_msgdesc_t protos_Doc_msg;

/* Defines for backwards compatibility with code written before nanopb-0.4.0 */
#define protos_Doc_fields &protos_Doc_msg

/* Maximum encoded size of messages (where known) */
/* protos_Doc_size depends on runtime parameters */

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif