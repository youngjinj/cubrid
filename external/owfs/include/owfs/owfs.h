#ifndef __OWFS_H__
#define __OWFS_H__

#include "owfs/owfs_errno.h"

#if defined(_WIN32) || defined(_WIN64)
#include <winsock2.h>
#include <winbase.h>

#ifndef OWFS_EXPORT_DEFINED
#define OWFS_EXPORT_DEFINED
#ifndef OWFS_WINDOWS_STATIC_LIB
#ifdef OWFS_WINDOWS_DLL_BUILD
#define OWFS_EXPORT	__declspec (dllexport)
#else 	/* OWFS_WINDOWS_DLL_BUILD */
#define OWFS_EXPORT	__declspec (dllimport)
#endif 	/* OWFS_WINDOWS_DLL_BUILD */
#else 	/* OWFS_WINDOWS_STATIC_LIB */
#define OWFS_EXPORT
#endif 	/* OWFS_WINDOWS_STATIC_LIB */
#define OWFS_CDECL	__cdecl

#endif	/* OWFS_EXPORT_DEFINED */

#else	/* !__WINDOWS__ */
#include <sys/socket.h>
#include <arpa/inet.h>

#ifndef OWFS_EXPORT_DEFINED
#define OWFS_EXPORT_DEFINED

#define OWFS_EXPORT
#define OWFS_CDECL	

#endif	/* OWFS_EXPORT_DEFINED */

#endif	/* __WINDOWS__ */

#ifdef  __cplusplus
extern "C" {
#endif

/* boolean */
#define OWFS_TRUE				1
#define OWFS_FALSE				0

/* size limits */
#define MAXOIDLEN				256
#define MAXOIDLEN_POOL			128
#define MAXPATHNAMELEN			4000
#define MAXFIDLEN				256
#define MAXSVCCODELEN			64

/* buffer sizes */
#define MAXBUFSIZE				32768		/* 32KB */
#define MAXTCPBUFSIZE			1048576		/* 1MB */

#define IP_ADDR_SIZE			16

#pragma pack(4)

/* type definition */
typedef void *fs_handle[2];
typedef void *owner_handle;
typedef void *file_handle;
typedef void *owfs_list;
typedef void *owfs_copy_handle;
typedef void *owfs_op_handle;
typedef void *pwrite_file_handle;		/* for writing file parallelly */

/* Definitions for owfs log */
/* OwFS Log Facility definition for log */
#define OWFS_LOG_FAC_NONE		-1		/* that does not log to syslog */
#define OWFS_LOG_FAC_TO_MINE	9		/* log to syslog already opened by user */

/* OwFS Log Options definition */
#define OLO_NO_STDERR			0
#define OLO_STDERR				1
#define OLO_WITH_PID			2
#define OLO_NELO				4
#if defined(_WIN32) || defined(_WIN64)
#define OLO_ENABLE_EVENT_LOG	0
#define OLO_DISABLE_EVENT_LOG	0x10000000
#define OLO_DEFAULT				OLO_ENABLE_EVENT_LOG
#else	// !__WINDOWS__
#define OLO_DEFAULT				OLO_NO_STDERR
#endif

/* File Write Method definition */
#define WRITE_METHOD_AUTO		0
#define WRITE_METHOD_PARALLEL	1
#define WRITE_METHOD_PIPELINED	2
#define WRITE_METHOD_DEFAULT	WRITE_METHOD_PARALLEL
/* If client does not specify write_method, WRITE_METHOD_PARALLEL is selected by default. */

/****************************************************************************/
/* IDC Related value definition: owfs_param_t.IDC_location */
/* IDC definition */
#define OWFS_IDC_IGNORE				0	/* Don't care */
#define OWFS_IDC_CHUNCHEON			1	/* ÃáÃµ IDC */
#define OWFS_IDC_MABUK				2	/* ¸¶ºÏ IDC */
#define OWFS_IDC_GASAN				3	/* °¡»ê IDC */
#define OWFS_IDC_MOKDONG			4	/* ¸ñµ¿ IDC */
/****************************************************************************/
/* Read priority realated value : owfs_param_t.read_priority */
#define OWFS_READ_ANY_REP			0	/* read from any replica */
#define OWFS_READ_IDC_FIRST			1	/* read from closer IDC */
#define OWFS_READ_PRIO_FIRST		2	/* read from high priority */

typedef struct {
	int		use_mdcache;			/* Default: false */
	int		worker_thread;			/* Default: 64 */
	int		timeout;				/* Default: 10 sec */
									/* This parameter changes RPC timeout to both MDS and DS */
	int		retrycount;				/* Default: 5 */
									/* This parameter changes RPC retry count to both MDS and DS */
	int		mds_timeout;			/* Default: -1 */
									/* This parameter(> 0) only changes RPC timeout to MDS */
	int		mds_retrycount;			/* Default: -1 */
									/* This parameter(> 0) only changes RPC retry count to MDS */
	int		ds_write_timeout;		/* Default: -1 */
									/* This parameter(> 0) only changes RPC timeout for write operation to DS */
	int		ds_write_retrycount;	/* Default: -1 */
									/* This parameter(>= 5) only changes RPC retry count for write operation to DS */
	int		ds_read_timeout;		/* Default: -1 */
									/* This parameter(> 0) only changes RPC timeout for read operation to DS */
	int		ds_read_retrycount;		/* Default: -1 */
									/* This parameter(> 0) only changes RPC retry count for read operation to DS */
	int		max_handles;			/* maximum pooling handles of client RPC */
									/* Deafult: 512 */
	char	*syslog_ident;			/* To open syslog by owfslog, specify it */
	int		log_fac;				/* Syslog facility to log */
	int		owfs_log_opts;			/* Additional owfs log options */
	int		use_write_lock;			/* Default: true */
	int 	file_write_method;		/* WRITE_METHOD_AUTO, WRITE_METHOD_PARALLEL(Default), */
									/* or WRITE_METHOD_PIPELINED */
	int		handle_pool_lifetime;	/* Lifetime of pooling handles of client RPC */
	char	*nelo_report_id;		/* To log with NELO, */
									/* specify NELO Report ID (or Project ID) */
	int		use_read_ahead;			/* Default: false */
	int		read_ahead_buf_size;	/* Read ahead buffer size (Default: 524288 Bytes) */
	int		use_long_filename;		/* use if filename length >= 256 (Default: false) */
	int		IDC_location;			/* IDC location of this OwFS client */
	int		use_copy_on_close;		/* use Copy-On-Close if multi-IDC configuraiton */
	int 	read_priority;			/* read option (IDC, tiering, or any) */
									/* Deafault: OWFS_READ_ANY_REP */
} owfs_param_t;

/* File types for s_type field */
#define OWFS_REGFILE			1		/* regular file */
#define OWFS_DIRECTORY			4		/* directory */

typedef struct {
	unsigned int	s_mode;
	unsigned int	s_uid;
	unsigned int	s_gid;
	long long 		s_size;
	unsigned int	s_atime;
	unsigned int	s_mtime;
	unsigned int	s_ctime;
	int				s_type;
} owfs_file_stat;

typedef struct {
	char			d_name[MAXFIDLEN];
	owfs_file_stat	d_stat;
} owfs_direntry;

typedef struct {
	unsigned int	u_atime;
	unsigned int	u_mtime;
} owfs_utimebuf;

typedef struct {
	unsigned long long open_fs_cnt;
	unsigned long long close_fs_cnt;
	unsigned long long open_owner_cnt;
	unsigned long long close_owner_cnt;
	unsigned long long open_file_cnt;
	unsigned long long close_file_cnt;
	unsigned long long opendir_cnt;
	unsigned long long closedir_cnt;
	unsigned long long open_owner_list_cnt;
	unsigned long long close_owner_list_cnt;
	unsigned long long open_copy_handle_cnt;
	unsigned long long close_copy_handle_cnt;
	unsigned long long open_op_handle_cnt;
	unsigned long long close_op_handle_cnt;
} owfs_handle_stat;

/* initialization/finalization */
extern OWFS_EXPORT int OWFS_CDECL owfs_get_param(owfs_param_t *param);
extern OWFS_EXPORT int OWFS_CDECL owfs_init(const owfs_param_t *param);
extern OWFS_EXPORT int OWFS_CDECL owfs_finalize(void);

/* file system handle-related interfaces */
/* If OwFS service doesn't use pool mode, specify svc_code argument to SVC_CODE_NONE */
#define SVC_CODE_NONE			""

extern OWFS_EXPORT int OWFS_CDECL owfs_open_fs(char *MDS_addr, char *svc_code, fs_handle *fsh);
extern OWFS_EXPORT int OWFS_CDECL owfs_close_fs(fs_handle fsh);
extern OWFS_EXPORT int OWFS_CDECL owfs_get_fs_param(fs_handle fsh, owfs_param_t *param);
extern OWFS_EXPORT int OWFS_CDECL owfs_set_fs_param(fs_handle fsh, const owfs_param_t *param);

/* owner-related interfaces */
extern OWFS_EXPORT int OWFS_CDECL owfs_create_owner(fs_handle fsh, char *owner_id, owner_handle *oh);

/* Group priority definition: input of prio argument */
#define OWFS_GRP_PRIO_NORMAL	0
#define OWFS_GRP_PRIO_HIGH		1
#define OWFS_GRP_PRIO_HIGHER	2
#define OWFS_GRP_PRIO_HIGHEST	3
extern OWFS_EXPORT int OWFS_CDECL owfs_create_owner_extended(fs_handle fsh, char *owner_id, owner_handle *oh, int prio);
extern OWFS_EXPORT int OWFS_CDECL owfs_open_owner(fs_handle fsh, char *owner_id, owner_handle *oh);
extern OWFS_EXPORT int OWFS_CDECL owfs_close_owner(owner_handle oh);
extern OWFS_EXPORT int OWFS_CDECL owfs_delete_owner(fs_handle fsh, char *owner_id);
extern OWFS_EXPORT int OWFS_CDECL owfs_destroy_owner(fs_handle fsh, char *owner_id);
extern OWFS_EXPORT int OWFS_CDECL owfs_rename_owner(fs_handle fsh, char *old_owner_id, char *new_owner_id);
extern OWFS_EXPORT int OWFS_CDECL owfs_owner_exists(fs_handle fsh, char *owner_id);
extern OWFS_EXPORT int OWFS_CDECL owfs_undelete_owner(fs_handle fsh, char *owner_id);
extern OWFS_EXPORT int OWFS_CDECL owfs_open_owner_list(fs_handle fsh, owfs_list *ol, char *regex_filter);
extern OWFS_EXPORT int OWFS_CDECL owfs_read_owner_list(owfs_list ol, owfs_direntry *owfs_dent);
extern OWFS_EXPORT int OWFS_CDECL owfs_close_owner_list(owfs_list ol);
extern OWFS_EXPORT int OWFS_CDECL owfs_is_empty_owner(owner_handle oh);
extern OWFS_EXPORT int OWFS_CDECL owfs_isolate_owner(fs_handle fsh, char *owner_id, char *isolate_svc_code, char *isolate_owner_id);

/* file-related interfaces */
/* flag definitions for file open */
#define OWFS_CREAT				0x01	/* create and sequential write */
#define OWFS_EXIST				0x02	/* random read */
#define OWFS_READ				OWFS_EXIST	
#define OWFS_RESUME				0x03	/* resume stopped write */
#define OWFS_OVERWRITE			0x08	/* full overwrite */

extern OWFS_EXPORT int OWFS_CDECL owfs_open_file(owner_handle oh, char *file_pathname, int flag, file_handle *fh);
extern OWFS_EXPORT int OWFS_CDECL owfs_close_file(file_handle fh);
extern OWFS_EXPORT int OWFS_CDECL owfs_release_file(file_handle fh);
extern OWFS_EXPORT int OWFS_CDECL owfs_read_file(file_handle fh, char *buf, unsigned int size);
extern OWFS_EXPORT int OWFS_CDECL owfs_write_file(file_handle fh, char *buf, unsigned int size);
extern OWFS_EXPORT int OWFS_CDECL owfs_delete_file(owner_handle oh, char *file_pathname);
extern OWFS_EXPORT int OWFS_CDECL owfs_destroy_file(owner_handle oh, char *file_pathname);

/* whence definitions for lseek */
#define OWFS_SEEK_SET			0
#define OWFS_SEEK_CUR			1
#define OWFS_SEEK_END			2

extern OWFS_EXPORT int OWFS_CDECL owfs_lseek(file_handle fh, long long offset, int whence);
extern OWFS_EXPORT long long OWFS_CDECL owfs_get_file_pos(file_handle fh);
extern OWFS_EXPORT int OWFS_CDECL owfs_truncate_file(owner_handle oh, char *file_pathname, long long offset);
extern OWFS_EXPORT int OWFS_CDECL owfs_append_file_and_get_pos(owner_handle oh, char *file_pathname, char *buf, unsigned int size, long long *pPos);
#define owfs_append_file(oh, file_pathname, buf, size)	\
		owfs_append_file_and_get_pos(oh, file_pathname, buf, size, NULL)
extern OWFS_EXPORT int OWFS_CDECL owfs_undelete_file(owner_handle oh, char *file_pathname);
extern OWFS_EXPORT int OWFS_CDECL owfs_make_reference_file(owner_handle oh, char *old_file_pathname, char *new_reference_pathname);

/* For file handle with OWFS_READ opening flag, you can set read ahead option.
 * If use_read_ahead is TRUE, read_ahead_buf_size value must be 0 < read_ahead_buf_size <=
 * MAXTCPBUFSIZE. Otherwise, read_ahead_buf_size value will be ignored. */
extern OWFS_EXPORT int OWFS_CDECL owfs_set_read_ahead(file_handle fh, int use_read_ahead, int read_ahead_buf_size);

/* New parallel file write interfaces */
#define MIN_PARALLEL_INSTANCES	2
#define MAX_PARALLEL_INSTANCES	16
#define MIN_PARALLEL_FILE_SIZE	1048576

typedef struct {
	int			instance_id;
	long long	start_offset;
	long long	size_to_write;
	long long	current_offset;
} pwrite_file_handle_info_t;
 
/* flag argument can be OWFS_CREAT, OWFS_OVERWRITE, OWFS_CREAT | OWFS_OVERWRITE. But no RESUME option */
extern OWFS_EXPORT int OWFS_CDECL owfs_open_parallel_file(owner_handle oh, char *file_pathname, int flag,
											int concurrency, long long file_size, pwrite_file_handle *pfh);
extern OWFS_EXPORT pwrite_file_handle_info_t * OWFS_CDECL owfs_get_pwrite_file_handle_info(pwrite_file_handle pfh, int instance_id);
extern OWFS_EXPORT int OWFS_CDECL owfs_write_parallel_file(pwrite_file_handle pfh, int instance_id, char *buf, unsigned int size);
extern OWFS_EXPORT int OWFS_CDECL owfs_close_parallel_file(pwrite_file_handle pfh);
extern OWFS_EXPORT int OWFS_CDECL owfs_release_parallel_file(pwrite_file_handle pfh);

/* directory-related interfaces */
extern OWFS_EXPORT int OWFS_CDECL owfs_mkdir(owner_handle oh, char *dir_pathname);
extern OWFS_EXPORT int OWFS_CDECL owfs_rmdir(owner_handle oh, char *dir_pathname);
extern OWFS_EXPORT int OWFS_CDECL owfs_is_empty_dir(owner_handle oh, char *dir_pathname);
extern OWFS_EXPORT int OWFS_CDECL owfs_destroy_dir(owner_handle oh, char *dir_pathname);

/* file and directory (both)-related interfaces */
extern OWFS_EXPORT int OWFS_CDECL owfs_exists(owner_handle oh, char *pathname);
extern OWFS_EXPORT int OWFS_CDECL owfs_stat(owner_handle oh, char *pathname, owfs_file_stat *pstat);
extern OWFS_EXPORT int OWFS_CDECL owfs_rename(owner_handle oh, char *old_pathname, char *new_pathname);
extern OWFS_EXPORT int OWFS_CDECL owfs_utime(owner_handle oh, char *pathname, owfs_utimebuf *utimebuf);

/* file list-related interfaces */
extern OWFS_EXPORT int OWFS_CDECL owfs_opendir(owner_handle oh, char *dir_pathname, owfs_list *ol);
extern OWFS_EXPORT int OWFS_CDECL owfs_readdir(owfs_list ol, owfs_direntry *entry);
extern OWFS_EXPORT int OWFS_CDECL owfs_closedir(owfs_list ol);

/* file copy interfaces */
extern OWFS_EXPORT int OWFS_CDECL owfs_open_copy_handle(owner_handle oh, char *src_file_pathname, char *dest_file_pathname, int flag, owfs_copy_handle *ch);
extern OWFS_EXPORT int OWFS_CDECL owfs_copy_file(owfs_copy_handle ch, unsigned int size);
extern OWFS_EXPORT int OWFS_CDECL owfs_close_copy_handle(owfs_copy_handle ch);
extern OWFS_EXPORT int OWFS_CDECL owfs_release_copy_handle(owfs_copy_handle ch);

/* interfaces for file copy to other owner */
extern OWFS_EXPORT int OWFS_CDECL owfs_open_copy_operation(owner_handle src_oh, char *src_file_pathname, owner_handle dest_oh, char *dest_file_pathname, int flag, owfs_op_handle *oph);
extern OWFS_EXPORT int OWFS_CDECL owfs_copy_operation(owfs_op_handle oph, unsigned int size);
extern OWFS_EXPORT int OWFS_CDECL owfs_close_copy_operation(owfs_op_handle oph);
extern OWFS_EXPORT int OWFS_CDECL owfs_release_copy_operation(owfs_op_handle oph);

/* interfaces for file move to other owner */
extern OWFS_EXPORT int OWFS_CDECL owfs_move_operation(owner_handle src_oh, char *src_file_pathname, owner_handle dest_oh, char *dest_file_pathname);

/* handle statistics interfaces */
extern OWFS_EXPORT int OWFS_CDECL owfs_get_handle_stat(owfs_handle_stat *stat);

/* miscellaneous interfaces */
extern OWFS_EXPORT char * OWFS_CDECL owfs_perror(int err);
extern OWFS_EXPORT int OWFS_CDECL owfs_get_current_time(fs_handle fsh, unsigned int *ptime);

/* debugging interfaces */
extern OWFS_EXPORT char * OWFS_CDECL owfs_get_svc_code(fs_handle fsh);
extern OWFS_EXPORT char * OWFS_CDECL owfs_get_svc_name(fs_handle fsh);
extern OWFS_EXPORT char * OWFS_CDECL owfs_get_owner_id_from_oh(owner_handle oh);
extern OWFS_EXPORT char * OWFS_CDECL owfs_get_owner_id_from_fh(file_handle fh);
extern OWFS_EXPORT char * OWFS_CDECL owfs_get_file_id_from_fh(file_handle fh);
extern OWFS_EXPORT char * OWFS_CDECL owfs_get_file_pathname_from_fh(file_handle fh);


/* deprecated interfaces */
#ifndef DEPRECATED
#define DEPRECATED
#endif	/* DEPRECATED */

typedef DEPRECATED struct {
	long long		a_size;
	unsigned int	a_mtime;
#define a_ctime		a_mtime
	unsigned int	a_atime;
} owfs_attr;

typedef DEPRECATED struct {
	char			e_name[MAXFIDLEN];
	long long		e_size;
	unsigned int	e_mtime;
#define e_ctime		e_mtime
	unsigned int	e_atime;

	/* not used anymore. remained for backward compatibility */
	int				e_type;
	unsigned int	e_checksum;
} owfs_listentry;

/* Use owfs_rename instead of owfs_rename_file. */
DEPRECATED extern OWFS_EXPORT int OWFS_CDECL owfs_rename_file(owner_handle oh, char *old_file_id, char *new_file_id);
/* Use owfs_exists instead of owfs_file_exists. */
DEPRECATED extern OWFS_EXPORT int OWFS_CDECL owfs_file_exists(owner_handle oh, char *file_id);
/* Use owfs_stat instead of owfs_getattr. */
DEPRECATED extern OWFS_EXPORT int OWFS_CDECL owfs_getattr(owner_handle oh, char *file_id, owfs_attr *pattr);
/* Use owfs_opendir, owfs_readdir, owfs_closedir instead of owfs_open_list, owfs_read_list, owfs_close_list. */
DEPRECATED extern OWFS_EXPORT int OWFS_CDECL owfs_open_list(owner_handle oh, owfs_list *ol);
DEPRECATED extern OWFS_EXPORT int OWFS_CDECL owfs_read_list(owfs_list ol, owfs_listentry *entry);
DEPRECATED extern OWFS_EXPORT int OWFS_CDECL owfs_close_list(owfs_list ol);

/* Use owfs_open_fs instead of DEPRECATED owfs_open_fs_with_svc_code. */
#define owfs_open_fs_with_svc_code		owfs_open_fs

/* Use owfs_lseek instead of DEPRECATED owfs_lseek_file. */
#define owfs_lseek_file(fh, offset)		owfs_lseek((fh), (offset), OWFS_SEEK_SET)

/* Symbolic link features spec out */
typedef DEPRECATED struct {
	char			l_mds_addr[IP_ADDR_SIZE];
	char			l_owner_id[MAXOIDLEN];
	char			l_file_pathname[MAXPATHNAMELEN];
} owfs_symlink;
#define owfs_make_symbolic_link_with_svc_code(a, b, c, d, e, f)	(-OWFS_ENOTSUPP)
#define owfs_make_symbolic_link(a, b, c, d, e)					(-OWFS_ENOTSUPP)
#define owfs_remove_symbolic_link(a, b)							(-OWFS_ENOTSUPP)
#define owfs_read_symbolic_link(a, b, c)						(-OWFS_ENOTSUPP)
#define owfs_lstat(a, b, c)										(-OWFS_ENOTSUPP)
#define owfs_getattr_symbolic_link(a, b, c)						(-OWFS_ENOTSUPP)


#undef DEPRECATED

#pragma pack()

#ifdef  __cplusplus
}
#endif

#endif	/* __OWFS_H__ */
