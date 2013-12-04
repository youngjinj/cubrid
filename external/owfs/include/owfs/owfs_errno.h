#ifndef __OWFS_ERRNO_H__
#define __OWFS_ERRNO_H__

#define OWFS_SUCCESS		0

#define OWFS_EBASE			10000
#define OWFS_FBASE			20000

enum owfs_errno {
	/* error that can be avoided when try with valid arguments */
	__OWFS_ESTART__ = OWFS_EBASE,
	OWFS_EINVAL,		/*  1: invalid arguments */
	OWFS_ENOENT,		/*  2: no entry */
	OWFS_EBUSY,			/*  3: target is busy */
	OWFS_ENOPERM,		/*  4: permission error */
	OWFS_EMISMATCH,		/*  5: state mismatch */
	OWFS_EEXIST,		/*  6: already exist */
	OWFS_ENOENTOWNER,	/*  7: no owner exist */
	OWFS_EMUSTIGNORE,	/*  8: must ignore */
	OWFS_ENOTINIT,		/*  9: not initialized */
	OWFS_EAGAIN,		/* 10: try again */
	OWFS_EACCESS,		/* 11: ACL or access permission denied */
	OWFS_ENOTSYMLINK,	/* 12: symbolic link operation with regular file: NOT USED */
	OWFS_ERPC,			/* 13: error of RPC layer */
	OWFS_ELOCK,			/* 14: fail to acquire a lock from DS */
	OWFS_ENOTEMPTY,		/* 15: directory is not empty */
	OWFS_ENOTDIR,		/* 16: pathname is not a directory */
	OWFS_EISDIR,		/* 17: pathname is a directory */
	OWFS_EPROGRESS,		/* 18: same operation is already progressing */
	OWFS_ESTOPPED, 		/* 19: operation is stopped */
	OWFS_ENOTSUPP,		/* 20: operation is not supported */
	OWFS_ENOTLINK,		/* 21: pathname is not a link */
	OWFS_EISLINK,		/* 22: pathname is a link */
	OWFS_EFILECHANGED,	/* 23: file is changed during operation */
	OWFS_EISREFERENCE,	/* 24: operation not permitted for a reference file */
	__OWFS_EEND__,

	/* error that should trigger some recovery mechanism */
	__OWFS_FSTART__ = OWFS_FBASE,
	OWFS_EFATAL,		/*  1: fatal error */
	OWFS_ENOMEM,		/*  2: not sufficient memory */
	OWFS_EDISK,			/*  3: server disk failure */
	OWFS_ECONN,			/*  4: network failure */
	OWFS_ECHECKSUM,		/*  5: checksum error */
	OWFS_ENOREP,		/*  6: insufficient replica to allocate */
	OWFS_ENOSPC,		/*  7: insufficient storage space */
	OWFS_EDQUOT,		/*  8: Quota exceeded */
	__OWFS_FEND__
};

/* check error */
#define OWFS_ERROR(e)		(e < OWFS_SUCCESS)

#define OWFS_EERROR(e)		(e < -__OWFS_ESTART__ && e > -__OWFS_EEND__)
#define OWFS_FERROR(e)		(e < -__OWFS_FSTART__ && e > -__OWFS_FEND__)

#endif	// __OWFS_ERRNO_H__
