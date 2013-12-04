/*
 * Copyright (C) 2011 NHN Business Corporation. All rights reserved.
 *
 * nBase distributed database system for high availability, high scalability and high performance
 */

/*
 * nbase.h - nBase main header
 */

#ifndef _NBASE_H_
#define _NBASE_H_

#ifdef  __cplusplus
namespace nbase_t {
#endif

/********************************************************************
 NStore client library initialization functions
********************************************************************/
// NStore client parameter structure
typedef struct _nbase_param_st {
	int worker_thread;		// Max worker thread count for multi-RPC
	int rpc_timeout;		// RPC timeout
	int max_handles;		/* maximum pooling handles of client RPC */
	int optflags;			// Option flags bit mask
	char *syslog_ident;		/* To open syslog by log, specify it */
	int log_fac;			/* Syslog facility to log */
	int log_opts;			/* Additional nbase log options */
	char *log_path;			/* You can specify new log_path. it overrides default path */
	int log_level;			/* Default log level to be printed */
} nbase_param_t;

// Option flag values
#define NSC_OPT_NONE				0
#define NSC_OPT_FIXED_REP_READ		0x1000000

// Syslog facility special values or 0 ~ 7 for LOCAL0 ~ LOCAL7
#define NSC_LOGFAC_NONE				(-1)
#define NSC_LOGFAC_TO_MINE			9
// Log options
#define NSC_LOGOPT_NONE				0
#define NSC_LOGOPT_OUT_TO_CONSOLE	1
#define NSC_LOGOPT_WITH_PID			2
#define NSC_LOGOPT_WITH_TID			4
#define NSC_LOGOPT_OUT_TO_SYSLOG	8
#define NSC_LOGOPT_CATEGORY			16
#define NSC_LOGOPT_DEFAULT			NSC_LOGOPT_NONE

typedef enum _nbase_result_format {
	NBASE_RESULT_FORMAT_BEGIN = 0,
	NBASE_RESULT_FORMAT_JSON,
	NBASE_RESULT_FORMAT_MULTI_BSON,
	NBASE_RESULT_FORMAT_END
} nbase_result_format_t;

/* Get current NStore parameters */
extern int nbase_get_param(nbase_param_t * param);

/* Initialize NStore client library with param */
extern int nbase_init(nbase_param_t * param);

/* Finalize NStore client library. It may free some resources */
extern int nbase_finalize(void);

/********************************************************************
 NStore client Query functions
********************************************************************/
/* query interfaces */
typedef struct {
	void *ptr;
} nquery_res;

#define IPSTR_LEN	256
typedef char nbase_ipstr[IPSTR_LEN]; 

/* import ns_rpc_tx_res_t */
#pragma pack(4)
typedef struct {
	int ret;
	unsigned int ip;
	unsigned short rpc_port;
	unsigned short http_port;
	unsigned long long txid;
} tx_res_t;

typedef struct {
	tx_res_t rpc_ret;
	nbase_ipstr ipstr;
	int in_tran;
} nbase_tx;

typedef struct {
	void *mgmt;
} nbase_mgmt;

#pragma pack()

void *nbase_zone_list_t;


/*
 * nBase client API
 */

typedef void nbase_callback_t (nquery_res result, void *arg);

extern int nbase_query(const char *ipstr, const int port, const char *keyspace, const char *ckey, 
			const char *nsql, nquery_res *result);

extern int nbase_query_opts(const char *ipstr, const int port, const char *keyspace, const char *ckey, 
			const char *nsql, const int timeout_msec, const nbase_result_format_t format, nquery_res *result);

extern int nbase_query_callback(const char *ipstr, const int port, const char *keyspace, const char *ckey,
			const char *nsql, nbase_callback_t *fn, void *arg);

extern int nbase_query_callback_opts(const char *ipstr, const int port, const char *keyspace, const char *ckey,
			const char *nsql, const int timeout_msec, const nbase_result_format_t format, 
			nbase_callback_t *fn, void *arg);

extern int nbase_begin_tx(const char *ipstr, const int port, const char *ckey, const int tx_timeout_msec, 
			nbase_tx *handle);

extern int nbase_get_cs_addr_of(nbase_tx *handle, unsigned int *ip_ptr, unsigned short *port_ptr);

extern int nbase_query_with_tx(nbase_tx *handle, const char *keyspace, const char *ckey, const char *nsql, 
			nquery_res *result);

extern int nbase_query_opts_with_tx(nbase_tx *handle, const char *keyspace, const char *ckey, const char *nsql, 
			const int timeout_msec, const nbase_result_format_t format, nquery_res *result);

extern int nbase_query_callback_with_tx(nbase_tx *handle, const char *keyspace, const char *ckey, const char *nsql,
			nbase_callback_t *fn, void *arg);

extern int nbase_query_callback_opts_with_tx(nbase_tx *handle, const char *keyspace, const char *ckey, 
			const char *nsql, const int timeout_msec, const nbase_result_format_t format, 
			nbase_callback_t *fn, void *arg);

extern int nbase_end_tx(nbase_tx *handle, const int do_commit);

extern int nbase_query_all(const char *ipstr, const int port, const char *keyspace, const char *query, 
			int timeout_msec, nbase_result_format_t format, nquery_res * result);

extern int nbase_query_all_ckey_list(const int port, const char *keyspace, const char *ckey_list[], 
			const char *nsql, const int timeout_msec, nbase_result_format_t format,
			nbase_callback_t *fn, void *arg);

extern int nbase_mgmt_query_all_ckey_list(nbase_mgmt *hdl, const int port, const char *keyspace, const char *ckey_list[], 
			const char *nsql, const int timeout_msec, nbase_result_format_t format,
			nbase_callback_t *fn, void *arg);


/* Get NSQL result string from nquery_res */
extern char *nbase_get_result_str(nquery_res result);

/* Get NSQL result code from nquery_res */
extern int nbase_get_result_code(nquery_res result);

/* Get error message for result code */
extern char *nbase_get_result_errmsg(int result_code);

/* Get NSQL result length from nquery_res */
extern int nbase_get_result_len(nquery_res result);

/* Free NSQL query result */
extern void nbase_free_result(nquery_res result);

/* escaping a string enclosed with double quotation */
char *nbase_escape_dq_str(char *src, int src_size, char *outbuf, int buf_size);

/* escaping a string enclosed with single quotation */
char *nbase_escape_sq_str(char *src, int src_size, char *outbuf, int buf_size);

/* managing cs list */
int nbase_register_cs_list(const char *mgmt_ipstr, const unsigned short mgmt_port);
int nbase_unregister_cs_list(void);
int nbase_get_rand_cs(const char *hinted_ck, nbase_ipstr out_ipstr, unsigned short *out_port, const char *ex_ip_list);
int nbase_get_rand_slave_cs(const char *hinted_ck, nbase_ipstr ipstr, unsigned short *port, const char *ex_str_ip_list);

/* multi-mgmt version */
int nbase_mgmt_register_cs_list(const char *mgmt_ipstr, const unsigned short mgmt_port, nbase_mgmt *handle);
int nbase_mgmt_unregister_cs_list(nbase_mgmt *handle);
int nbase_mgmt_get_rand_cs(nbase_mgmt *handle, const char *hinted_ck, nbase_ipstr out_ipstr, unsigned short *out_port, const char *ex_ip_list);
int nbase_mgmt_get_rand_slave_cs(nbase_mgmt *handle, const char *hinted_ck, nbase_ipstr ipstr, unsigned short *port, const char *ex_str_ip_list);

/* zone version */
int nbase_get_rand_cs_in_zone(const int zone, const char *hinted_ck, nbase_ipstr out_ipstr, unsigned short *out_port, const char *ex_ip_list);
int nbase_get_rand_slave_cs_in_zone(const int zone, const char *hinted_ck, nbase_ipstr ipstr, unsigned short *port, const char *ex_str_ip_list);
int nbase_mgmt_get_rand_cs_in_zone(nbase_mgmt *handle, const int zone, const char *hinted_ck, nbase_ipstr out_ipstr, unsigned short *out_port, const char *ex_ip_list);
int nbase_mgmt_get_rand_slave_cs_in_zone(nbase_mgmt *handle, const int zone, const char *hinted_ck, nbase_ipstr ipstr, unsigned short *port, const char *ex_str_ip_list);
int nbase_query_all_ckey_list_in_zone(const int zone, const int port, const char *keyspace, const char *ckey_list[], 
			const char *nsql, const int timeout_msec, nbase_result_format_t format,
			nbase_callback_t *fn, void *arg);
int nbase_mgmt_query_all_ckey_list_in_zone(nbase_mgmt *hdl, const int zone, const int port, const char *keyspace, const char *ckey_list[], 
			const char *nsql, const int timeout_msec, nbase_result_format_t format,
			nbase_callback_t *fn, void *arg);

#ifdef  __cplusplus
}
#endif
#endif							// _NBASE_H_
