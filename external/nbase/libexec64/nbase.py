#
# Copyright (C) 2011 NHN Business Corporation. All rights reserved.
#
# nBase distributed database system for high availability, high scalability and high performance
#

import nbase_impl
import demjson
import socket
import datetime
import os
import re
import time
import httplib
import urllib
import random
import popen2
import sys
import threading
import subprocess
import ConfigParser
import socket

VERBOSE = False
HTTP_OK = 200
DEFAULT_MANAGEMENT_NODE_PORT = 6219
DEFAULT_CS_RPC_PORT = 6220
DEFAULT_SLAVE_PORT = 6221
DEFAULT_CS_REST_PORT = 16000
DEFAULT_MIGRATOR_PORT = 6216
DEFAULT_RPC_TIMEOUT_MSEC = 30000
MGMT_BIN_NAME = 'nbase_mgmt'
CS_BIN_NAME = 'nbase_cs'
JSON_TYPE_NUM = 1
g_sync_mgr = None


class RestPool:
	def __init__(self):
		self.map = {}
		self.lock = threading.RLock()

	def put(self, rest):
		self.lock.acquire()
		try:
			if self.map.has_key(rest.ip):
				list = self.map[rest.ip]
				list.append(rest)
			else:
				list = []
				list.append(rest)
				self.map[rest.ip] = list
		finally:
			self.lock.release()

	def get(self, ip):
		self.lock.acquire()
		try:
			if self.map.has_key(ip):
				list = self.map[ip]
				if len(list) > 0:
					return list.pop(0)
			return None
		finally:
			self.lock.release()

class Schema:
	def __init__(self, name, columns, primary_keys, indices, is_local):
		self.name = name
		self.columns = columns
		self.primary_keys = primary_keys
		self.indices = indices
		self.is_local = is_local

g_rest_pool = RestPool()

def init():
	nbase_impl.nc_admin_nbase_init()
	nbase_impl.nc_admin_set_console_out(0)

def fini():
	nbase_impl.nc_admin_nbase_finalize()

def is_ip_address(addr):
	return len(re.findall('[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+', addr)) > 0

def errstr(errno):
	return nbase_impl.no_print_error_short(errno)

def set_verbose(flag):
	VERBOSE = flag

def read_mbr(ip, port):
	mbr_dict = nbase_impl.nc_admin_read_mbr_from(ip, port)
	if mbr_dict == None:
		exmsg = 'fail to read mbr from %s:%d' % (ip, port)
		raise NbaseException(exmsg)
	node_list = []
	for node in mbr_dict['node_list']:
		cs_info_list = []
		for cs in node['cs_list']:
			cs_info_list.append((cs['ip'], cs['port'], DEFAULT_CS_REST_PORT, node, cs['state'], cs['flags']))
		node_list.append(Node(node['nid'], cs_info_list))
	return Membership(mbr_dict['ver'], node_list)

def read_cgmap(ip, port):
	cgmap_dict = nbase_impl.nc_admin_read_cgmap_from(ip, port)
	if cgmap_dict == None:
		exmsg = 'fail to read cgmap from %s:%d' % (ip, port)
		raise NbaseException(exmsg)
	item_list = []
	for item in cgmap_dict['item_list']:
		item_list.append(CGMapItem(item['cgid'], item['nid'], ip, port))
	return CGMap(cgmap_dict['ver'], item_list)

def get_CS_from(cs_info):
	return CS(cs_info[0], cs_info[1], cs_info[2], cs_info[3], cs_info[4], cs_info[5])

def decode_to_json(data):
	try:
		return demjson.decode(data)
	except demjson.JSONDecodeError, ex:
		raise NbaseException("fail to decode json errmsg:%s, data:%s" % (str(ex), data))


class NbaseException(Exception):
	def __init__(self, msg, ret = -1):
		self.msg = msg
		self.ret = ret

	def get_ret(self):
		return self.ret

	def __str__(self):
		if self.ret != -1:
			return "ret:%d (%s) is raised. msg:%s" % (self.ret, errstr(self.ret), self.msg)
		return self.msg


class Remote:
	def __init__(self, addr):
		self.ip = is_ip_address(addr) and addr or socket.gethostbyname(addr)

	def __build_cmd(self, cmd, stdout_on = False, stderr_on = False, check_verbose = False):
		if cmd == None:
			raise NbaseException('Fail to execute remote command. cmd is None.')

		local_ip = socket.gethostbyname(socket.gethostname())	
		if local_ip != socket.gethostbyname(self.ip):
			# Set CUBRID env in shell rc if it is necessary
			# ssh user ID is current user. it is more flexible
			cmd = "ssh %s '%s'" % (self.ip, cmd)

		if check_verbose:
			if VERBOSE:
				stdout_on = True
				stderr_on = True

		if not stdout_on:
			cmd += ' > /dev/null '

		if not stderr_on:
			cmd += ' 2> /dev/null '

		return cmd


	def exec_cmd_out(self, cmd):
		cmd = self.__build_cmd(cmd, True, False, False)
		if VERBOSE:
			print "EXECUTE [%s]" % cmd

		child_in, child_out = os.popen2(cmd)
		lines = child_out.readlines()
		child_in.close()
		child_out.close()
		return lines

	def exec_cmd_fp(self, cmd):
		cmd = self.__build_cmd(cmd, True, False, False)
		if VERBOSE:
			print "EXECUTE [%s]" % cmd

		child_in, child_out = os.popen2(cmd)
		child_in.close()
		return child_out

	def exec_cmd_in(self, cmd, input_str = None):
		cmd = self.__build_cmd(cmd, True, True, False)
		if VERBOSE:
			print "EXECUTE [%s]" % cmd

		p = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		out, err = p.communicate(input_str)
		return out

	def exec_cmd(self, cmd, stdouterr = False):
		cmd = self.__build_cmd(cmd, stdouterr, stdouterr, True)
		if VERBOSE:
			print "EXECUTE [%s]" % cmd

		return os.system(cmd)

	def __get_ps_result(self, process_name):
		cmd = "ps --no-headers -C %s -f" % process_name
		lines = self.exec_cmd_out(cmd)
		if len(lines) == 0:
			return None

		return lines[0].split()

	def get_pidof(self, process_name):
		ret = self.__get_ps_result(process_name)
		if ret:
			return int(ret[1])
		return None

	def get_cmdof(self, process_name):
		ret = self.__get_ps_result(process_name)
		if ret:
			return ret[8:]
		return None


class Database (Remote):
	def __init__(self, ip, cs_port):
		Remote.__init__(self, ip)

		dbinfo = nbase_impl.nc_admin_get_dbinfo_detail(self.ip, cs_port)
		if dbinfo is None:
			raise NbaseException("Fail to connect %s:%d" % (self.ip, cs_port))

		self.storage_type = dbinfo['storage_type']
		self.name = dbinfo['db_name']
		self.user = dbinfo['db_user']
		self.passwd = dbinfo['db_passwd']  if len(dbinfo['db_passwd']) > 0 else None 
		self.port = dbinfo['db_port']
		self.slave_port = dbinfo['db_slave_port']

		self.cmd = self.__make_cmd()


	def __make_cmd(self):
		if self.storage_type != 'cubrid':
			return None
		cmd = "$CUBRID/bin/csql -u " + self.user + " -p " + (self.passwd if self.passwd is not None else '""') + \
			   " " + self.name + "@" + socket.gethostbyname(self.ip)
		return cmd

	def init_from_ini(self):
		fp = self.exec_cmd_fp('cat $NBASE/etc/ns.ini')
		cparser = ConfigParser.RawConfigParser()
		cparser.readfp(fp)
		fp.close()

		storage_type = cparser.get('STORAGE', 'type')
		name = cparser.get('STORAGE', 'db_name')
		user = cparser.get('STORAGE', 'db_user')

		try:
			passwd = cparser.get('STORAGE', 'db_passwd')
		except ConfigParser.NoOptionError:
			passwd = '' 

		self.storage_type = storage_type
		self.name = name
		self.user = user
		self.passwd = passwd if len(passwd) > 0 else None
		self.cmd = self.__make_cmd()

	def exec_cmd_in(self, cmd, input_str=None):
		p = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		out, err = p.communicate(input_str)
		return out

	def exec_cmd_fp(self, cmd):
		child_in, child_out = os.popen2(cmd)
		child_in.close()
		return child_out

	def exec_query(self, qstr):
		return self.exec_cmd_in(self.cmd, qstr)
		
	def exec_sql_file(self, filepath):
		cmd = self.cmd + ' < ' + filepath
		return self.exec_cmd_in(cmd)

	def get_table_names(self):
		cmd = self.cmd + ' -c "show tables"'
		fp = self.exec_cmd_fp(cmd)
		result = fp.read()
		fp.close()

		regex_obj = re.compile("'.+'", re.MULTILINE) # parse strings only in single-quotes
		tn_list = regex_obj.findall(result)

		for i in range(len(tn_list)):
			tn_list[i] = tn_list[i].replace("'", "")  # remove single-quotes wrapping a table name
		
		return tn_list

	def clear(self):
		tbl_list = self.get_table_names()
		query_format = 'drop table %s;'
		queries = ""
		for tbl_name in tbl_list:
			queries += (query_format % tbl_name)
		return self.exec_query(queries)

	def get_schema(self):
		tbl_list = self.get_table_names()
		results = []
		for tbl_name in tbl_list:
			res = self.exec_query(";sc %s" % tbl_name)
			results.extend(res.split("\n"))
		return results
	
	def create_sys_table(self):
		self.exec_sql_file('$NBASE/etc/ns_catalog.sql')
		self.exec_sql_file('$NBASE/etc/ns_culler_info.sql')


class Daemon (Remote):
	def __init__(self, ip, port):
		Remote.__init__(self, ip)
		self.port = port

	def __str__(self):
		return '%s:%d' % (self.ip, self.port)
		
	def inject_fault(self, fault_id):
		ret = nbase_impl.nc_admin_inject_fault(self.ip, self.port, fault_id)
		if errstr(ret) == 'INVAL':
			ex_msg = "Daemon at %s:%d does not allow fault injection. Check nbase build option" % (self.ip, self.port)
			raise NbaseException(ex_msg)


	def clear_fault(self, fault_id):
		ret = nbase_impl.nc_admin_clear_fault(self.ip, self.port, fault_id)
		if errstr(ret) == 'INVAL':
			ex_msg = "Daemon at %s:%d does not allow fault injection. Check nbase build option" % (self.ip, self.port)
			raise NbaseException(ex_msg)


	def __get_bin_path_cwd(self, pid):
		try:
			lines = self.exec_cmd_out('ls -l /proc/%d | grep "exe\|cwd"' % pid)
			return lines[1].split()[-1], lines[0].split()[-1]
		except IndexError:
			return None, None

	def get_pid_cmd(self):
		pid = self.get_pidof(self.get_bin_name())
		process_cmd = self.get_cmdof(self.get_bin_name())
		if pid == None or process_cmd == None:
			return -1, None

		bin_path, cwd = self.__get_bin_path_cwd(pid)
		if bin_path == None and cwd == None:
			return -1, None

		cmd = 'cd %s;%s ' % (cwd, bin_path)
		cmd += self.filter_options(process_cmd)

		return pid, cmd

	def filter_options(self, option_list):
		return ' '.join(option_list)

	def kill(self):
		pid, revive_cmd = self.get_pid_cmd()
		if pid == -1 and revive_cmd == None:
			ex_msg = "fail to kill process:%s at %s:%d. Please check whether it is able to execute remote command by ssh without authentication, or whether the process is running" % (self.get_bin_name(), self.ip, self.port)
			raise NbaseException(ex_msg)
		
		kill_cmd = 'kill %d' % pid
		ret = self.exec_cmd(kill_cmd)
		if ret == 0:
			for i in range(0, 5):
				time.sleep(1)
				pid, cmd = self.get_pid_cmd()
				if pid == -1:
					return revive_cmd
		else:
			ex_msg = "fail to execute ssh command [%s]" % kill_cmd
			raise NbaseException(ex_msg)

		ex_msg = "fail to kill process pid:%d. It may not exist" % pid
		raise NbaseException(ex_msg)

	def run(self, revive_cmd):
		time.sleep(1)
		ret = self.exec_cmd(revive_cmd)
		time.sleep(1)
		if ret != 0:
			ex_msg = "fail to run process:%s at %s:%d via ssh with cmd [%s]" % (self.get_bin_name(), self.ip, self.port, revive_cmd)
			raise NbaseException(ex_msg)
		return 0

	def is_alive(self):
		try:
			self.get_lclock()		
			return True
		except NbaseException:
			return False

	def get_lclock(self):
		clock = nbase_impl.nc_admin_lc_get(self.ip, self.port)
		if clock > 0:
			return clock
		ex_msg = "fail to connect to %s:%d" % (self.ip, self.port)
		raise NbaseException(ex_msg)

	def set_lclock(self, clock):
		ret = nbase_impl.nc_admin_lc_apply(self.ip, self.port, clock)
		if ret <> 0:
			raise NbaseException("fail to set clock")


class MembershipDaemon(Daemon):
	def __init__(self, ip, port, load_mbr_cgmap):
		Daemon.__init__(self, ip, port)
		self.lock = threading.RLock()
		self.mbr = None
		self.cgmap = None
		self.ip = ip
		self.port = port

		if load_mbr_cgmap:
			self.__load_mbr()
			self.__load_cgmap()

	def __load_mbr(self):
		self.mbr = read_mbr(self.ip, self.port)

	def __load_cgmap(self):
		self.cgmap = read_cgmap(self.ip, self.port)

	def __eq__(self, other):
		if isinstance(other, self.__class__):
			return self.ip == other.ip and self.port == other.port
		else:
			return False

	def select_available_cs(self):
		return self.get_mbr().select_available_cs()

	def get_mbr(self):
		self.lock.acquire()
		try:
			if self.mbr == None:
				self.__load_mbr()
			return self.mbr
		finally:
			self.lock.release()

	def get_cgmap(self):
		self.lock.acquire()
		try:
			if self.cgmap == None:
				self.__load_cgmap()
			return self.cgmap
		finally:
			self.lock.release()

	def get_node_list(self):
		return self.get_mbr().node_list

	def get_cs_list(self):
		cs_list = []
		for node in self.get_mbr().node_list:
			for cs_info in node.cs_info_list:
				cs_list.append(get_CS_from(cs_info))

		return cs_list

	def get_node_from_nid(self, nid):
		return self.get_mbr().get_node_from_nid(nid)

	def get_cs_list_of_nid(self, nid):
		return self.get_node_from_nid(nid).get_cs_list()

	def get_cgmap_item(self, ckey):
		return self.get_cgmap().get_cgmap_item_by_ckey(ckey)

	def __str__(self):
		return "%s:%d" % (self.ip, self.port)



class Membership:
	def __init__(self, ver, node_list):
		self.ver = ver
		self.node_list = node_list

	def get_node_from_nid(self, nid):
		for node in self.node_list:
			if node.nid == nid:
				return node
		return None

	def select_available_cs(self):
		node = random.choice(self.node_list)
		return node.select_available_cs()
		
	def get_node_count(self):
		return len(self.node_list)

	def get_node_from_cs_ip_port(self, ip, port):
		for node in self.node_list:
			for cs in node.get_cs_list():
				if cs.ip == ip and cs.port == port:
					return node
		return None

	def get_cs(self, ip, port):
		for node in self.node_list:
			for cs in node.get_cs_list():
				if cs.ip == ip and cs.port == port:
					return cs
		return None

	def get_cs_state(self, ip, port):
		for node in self.node_list:
			for cs in node.get_cs_list():
				if cs.ip == ip and cs.port == port:
					return cs.state
		return None
		  
	def __str__(self):
		ret = 'mbr ver:%d\n' % self.ver
		for node in self.node_list:
			ret += str(node)
			ret += '\n'
		return ret


class Node:
	def __init__(self, nid, cs_info_list):
		self.nid = nid
		self.cs_info_list = cs_info_list

	def __str__(self):
		list = ''
		for cs_info in self.cs_info_list:
			list += str(cs_info)
			list += '\n'
		return "nid:%d\n%s" % (self.nid, list)

	def has_available_cs(self):
		return len([cs_info for cs_info in self.cs_info_list if cs_info[4] == 'N']) > 0

	def select_available_cs(self):
		l = [cs_info for cs_info in self.cs_info_list if cs_info[4] == 'N']
		random.shuffle(l)
		return CS(l[0][0], l[0][1], node = self)

	def get_cs_list(self):
		cs_list = []
		for cs_info in self.cs_info_list:
			cs_list.append(get_CS_from(cs_info))

		return cs_list


class CGMap:
	def __init__(self, ver, item_list):
		self.ver = ver
		self.item_list = item_list

	def get_cgmap_item_by_ckey(self, ckey):
		return self.get_cgmap_item_by_cgid(nbase_impl.nc_admin_get_nid_of_ckey(ckey, len(self.item_list)))

	def get_cgmap_item_by_cgid(self, cgid):
		for item in self.item_list:
			if item.cgid == cgid:
				return item
		return None

	def get_cgmap_items_of(self, nid):
		ret = []
		for item in self.item_list:
			if item.nid == nid:
				ret.append(item)
		return ret

	def __str__(self):
		ret = ""
		for item in self.item_list:
			ret += str(item)
			ret += '\n'
		return ret

class CGMapItem:
	def __init__(self, cgid, nid, ip, port):
		self.cgid = cgid
		self.nid = nid
		self.ip = ip
		self.port = port

	def get_cs_list(self):
		return read_mbr(self.ip, self.port).get_node_from_nid(self.nid).get_cs_list()

	def __str__(self):
		return "cgid:%d  nid:%d" % (self.cgid, self.nid)


def process_result(ret, qres, return_by_map):
	if ret >= 0:
		try:
			if return_by_map:
				return nbase_impl.nbase_get_result_code(qres), decode_to_json(nbase_impl.nbase_get_result_str(qres))
			else:
				return nbase_impl.nbase_get_result_code(qres), nbase_impl.nbase_get_result_str(qres)
		finally:
			nbase_impl.nbase_free_result(qres)
	else:
		return ret, None


class Query:
	def __init__(self, ip, port, keyspace, ckey, format="json"):
		self.ip = ip
		self.port = port
		self.keyspace = keyspace
		self.ckey = ckey
		self.format = format

	def run(self, query, return_by_map, timeout_msec = 0):
		if query == None:
			raise NbaseException("query is none")
		qres = nbase_impl.nquery_res()
		ret = nbase_impl.nbase_query_opts(self.ip, self.port, self.keyspace, self.ckey, query, timeout_msec, nbase_impl.default_format, qres)
		return process_result(ret, qres, return_by_map)

	def run_ddl(self, query, quite):
		options = ' -m %s:%d -q "%s" ' % (self.ip, self.port, query)
		if self.keyspace <> None and self.keyspace <> "":
			options += ' -k "%s" ' % self.keyspace

		if quite:
			options += ' -Q '
		p = popen2.Popen3('$NBASE/bin/nbase_query_admin %s' % options)
		p.wait()
		if not quite:
			for line in p.fromchild.readlines():
				sys.stdout.write(line)

		ret = p.poll()

		ret_codes = { 60160:-1301, 60416:-1300, 59392:-1304, 59136:-1305, 59648:-1303, 5888:-1001, 4864:-1005, 50432:-19003 }
		if ret_codes.has_key(ret):
			ret = ret_codes[ret]

		return ret

	def run_all(self, query, timeout_msec = 0):
		qres = nbase_impl.nquery_res()
		ret = nbase_impl.nc_admin_query_all(self.ip, self.port, self.keyspace, query, timeout_msec, JSON_TYPE_NUM, qres)
		return process_result(ret, qres, True)

	def run_all_ckey_list(self, file_path, mgmt_addr, mgmt_port, ckey_list, query, timeout_msec = 0):
		return nbase_impl.nc_admin_query_all_ckey_list(file_path, mgmt_addr, mgmt_port, self.port, self.keyspace, ckey_list, query, timeout_msec, JSON_TYPE_NUM)

class Restful:
	def __init__(self, ip, keyspace, ckey, format="json", port = DEFAULT_CS_REST_PORT):
		self.ip = str(ip)
		self.conn = httplib.HTTPConnection(ip, int(port))
		self.conn.connect()

		self.set(keyspace, ckey, format)

	def set(self, keyspace, ckey, format="json"):
		self.keyspace = keyspace
		self.ckey = ckey
		self.format = format

	def send(self, path):
		self.conn.putrequest('GET', path)
		self.conn.putheader('Connection', 'Keep-Alive')
		self.conn.endheaders()

		self.conn.send('')

	def recv(self):
		res = self.get_response()
		return res.status, res.read()

	def run(self, path):
		self.send(path)
		return self.recv()

	def get_response(self):
		"""return HTTPResponse object, you must close the object after using it."""
		return self.conn.getresponse()

	def close(self):
		self.conn.close()

	def make_http_path(self, method, timeout_msec = 0):
		http_path = "/nbase/%s/%s?ckey=%s&format=%s" % (self.keyspace, method, self.ckey, self.format)
		if timeout_msec > 0:
			http_path += '&timeout_msec=%d' % timeout_msec

		return http_path


class RestQuery:
	def __init__(self, rest):
		self.rest = rest

	def make_http_path(self, nsql, timeout_msec = 0, old_format=False):
		path = self.rest.make_http_path('query', timeout_msec)
		if old_format:
			path += "&nsql='%s'" % urllib.quote_plus(nsql)
		else:
			path += "&nsql=%s" % urllib.quote_plus(nsql)
		return path

	def run(self, nsql, timeout_msec = 0):
		http_path = self.make_http_path(nsql, timeout_msec)
		return self.rest.run(http_path)

	def send(self, nsql, timeout_msec = 0):
		http_path = self.make_http_path(nsql, timeout_msec)
		self.rest.send(http_path)

	def get_response(self):
		return self.rest.get_response()

	def close(self):
		self.rest.close()



class HttpTransactionQuery:
	def __init__(self, id, rest, rpc_port):
		self.id = id
		self.rest = rest
		self.rest_query = RestQuery(rest)
		self.rpc_port = rpc_port

	def __split_ip_port(self, str):
		sp = str.split(':')
		return sp[0], sp[1]

	def get_target_host(self):
		return self.rest.ip

	def is_alive(self):
		if self.id == None:
			return False
		return nbase_impl.nc_admin_is_txid_valid(self.rest.ip, self.rpc_port, self.id) == 1

	def run(self, nsql, timeout_msec = 0):
		http_path = self.rest_query.make_http_path(nsql, timeout_msec)
		http_path += '&txid=%d' % self.id
		ret, data = self.rest.run(http_path)
		if ret != HTTP_OK:
			raise NbaseException("fail to run nsql. http ret:%d" % ret);
		return decode_to_json(data)

	def __end_tx(self, is_commit):
		http_path = "/nbase/%s/%s_tx?ckey=%s&txid=%d" % (self.rest.keyspace, is_commit and 'commit' or 'rollback', self.rest.ckey, self.id)
		ret, data = self.rest.run(http_path)
		if ret != HTTP_OK:
			raise NbaseException("fail to end tx. http ret:%d" % ret);
		return decode_to_json(data)

	def commit(self):
		ret = self.__end_tx(True)
		g_rest_pool.put(self.rest)
		self.rest = None
		return ret

	def rollback(self):
		ret = self.__end_tx(False)
		g_rest_pool.put(self.rest)
		self.rest = None
		return ret


class RpcTransactionQuery:

	def __init__(self, id, ip, port, keyspace, ckey):
		self.id = id
		self.ip = ip
		self.port = port
		self.keyspace = keyspace
		self.ckey = ckey

	def is_alive(self):
		if self.id == None:
			return False
		return nbase_impl.nc_admin_is_txid_valid(self.ip, self.port, self.id) == 1

	def run(self, nsql, timeout_msec = 0, return_by_map = True):
		qres = nbase_impl.nquery_res()
		ret = nbase_impl.nc_admin_query_opts_with_txid_for_test(self.ip, self.port, self.id, self.keyspace, self.ckey, nsql, timeout_msec, JSON_TYPE_NUM, qres)
		ret, str = process_result(ret, qres, return_by_map)
		if ret < 0:
			raise NbaseException("ret: %d is raised" % ret, ret)
		return str

	def query_large(self, nsql):
		return LargeQuery(self.ip, self.port, self.keyspace, self.ckey, nsql, self.id)

	def __end_tx(self, is_commit, timeout_msec):
		ret = nbase_impl.nc_admin_end_tx(self.ip, self.port, self.id, is_commit and 1 or 0, timeout_msec);
		return {'retcode':ret, 'retdata':errstr(ret)}

	def commit(self, timeout_msec = 0):
		return self.__end_tx(True, timeout_msec)

	def rollback(self, timeout_msec = 0):
		return self.__end_tx(False, timeout_msec)


class LargeQuery:
	def __init__(self, ip, port, keyspace, ckey, nsql, txid=-1):
		self.file_path = "/tmp/large_query_result.%d" % int(time.time())
		if txid == -1:
			self.ret = nbase_impl.nc_admin_query_callback(self.file_path, ip, port, keyspace, ckey, nsql);
		else:
			self.ret = nbase_impl.nc_admin_tx_query_callback(self.file_path, ip, port, keyspace, ckey, nsql, txid);
			
		if self.ret < 0:
			raise NbaseException("fail to query callback, ret:%d" % self.ret, self.ret)

		self.f = open(self.file_path, 'r')

	def read(self, num_read):
		return self.f.read(num_read)

	def close(self):
		self.f.close()
		os.remove(self.file_path)
		

class CS (MembershipDaemon):
	def __init__(self, ip, port = DEFAULT_CS_RPC_PORT, rest_port = DEFAULT_CS_REST_PORT, node = None, state = 'N', flags = 0):
		MembershipDaemon.__init__(self, ip, port, False)
		if node == None:
			node = self.get_mbr().get_node_from_cs_ip_port(ip, port)	

		self.node = node
		self.query_port = port
		self.rest_port = rest_port
		self.state = state
		self.flags = flags

	def filter_options(self, option_list):
		try:
			option_list.remove('-A')
		except ValueError:
			pass

		return ' '.join(option_list)

	def run_gc(self, duration=14400, interval=-1, del_unit=100):
		ret = nbase_impl.nc_admin_gc_start(self.ip, self.port, duration, interval, del_unit)
		if ret < 0:
			raise NbaseException("fail to start gc at %s:%d, ret:%d" % (self.ip, self.port, ret))

	def get_bin_name(self):
		return CS_BIN_NAME

	def __str__(self):
		s = self.get_bin_name() + "@" + MembershipDaemon.__str__(self)
		#s += ', nid:%s, state:%s' % (self.nid, self.state)
		return s

	def is_normal(self):
		return self.state == 'N'

	def get_zone(self):
		if self.is_normal() == False: 
			return False

		return self.flags & 0x0f

	def get_database(self):
		db = Database(self.ip, self.port) 
		return db

	def is_on_master_db(self):
		out_lines = self.exec_cmd_out('cubrid heartbeat status | grep current')
		if len(out_lines) == 0:
			raise NbaseException("No CUBRID HA settings at %s@%s" % (ssh_username == None and "" or ssh_username, self.ip))

		return out_lines[0].split()[-1].startswith('master')
		
	def get_query(self, keyspace, ckey, format="json"):
		return Query(self.ip, self.query_port, keyspace, ckey, format)

	def get_rest_query(self, keyspace, ckey, format="json", rest_port=DEFAULT_CS_REST_PORT):
		return RestQuery(Restful(self.ip, keyspace, ckey, format, rest_port))

	def get_restful(self, keyspace, ckey, format="json", rest_port=DEFAULT_CS_REST_PORT):
		return Restful(self.ip, keyspace, ckey, format, rest_port)

	def exec_query(self, keyspace, ckey, query_str, timeout_msec = DEFAULT_RPC_TIMEOUT_MSEC, return_by_map=True):
		return self.get_query(keyspace, ckey).run(query_str, return_by_map, timeout_msec)

	def exec_http_query(self, keyspace, ckey, query_str, format="json", rest_port=DEFAULT_CS_REST_PORT, timeout_msec=DEFAULT_RPC_TIMEOUT_MSEC, return_by_map =True):
		rest_query = self.get_rest_query(keyspace, ckey, format, rest_port)
		try:
			return rest_query.run(query_str, timeout_msec)
		finally:
			rest_query.close()

	def exec_query_large(self, keyspace, ckey, query_str, to_slave=False):
		return LargeQuery(self.ip, to_slave and DEFAULT_SLAVE_PORT or self.query_port, keyspace, ckey, query_str)

	def exec_query_all(self, keyspace, query, timeout_msec = 0):
		return self.get_query(keyspace, None).run_all(query, timeout_msec);

	def get_num_dbcon(self, rest_port=DEFAULT_CS_REST_PORT):
		ret, json = Restful(self.ip, "", "", "json", rest_port).run("/nbase_mon?item=all")
		return int(eval(json)['connection']['db_connection'])

	def get_num_inuse_stmt(self, rest_port=DEFAULT_CS_REST_PORT):
		ret, json = Restful(self.ip, "", "", "json", rest_port).run("/nbase_mon?item=all")
		return int(eval(json)['connection']['inuse_stmt'])

	def begin_tx(self, keyspace, ckey, timeout_msec = 0, format="json", use_http = False):
		func = use_http and self.__begin_tx_with_http or self.__begin_tx_with_rpc
		return func(keyspace, ckey, timeout_msec, format)

	def __begin_tx_with_http(self, keyspace, ckey, timeout_msec = 0, format="json"):
		rest = g_rest_pool.get(self.ip)
		if rest == None:
			rest = Restful(self.ip, keyspace, ckey, format)
		else:
			rest.set(keyspace, ckey, format)

		http_path = "/nbase/%s/begin_tx?ckey=%s" % (rest.keyspace, rest.ckey)
		if timeout_msec > 0:
			http_path += '&tx_timeout_msec=%d' % timeout_msec

		ret, data = rest.run(http_path)
		if ret != HTTP_OK:
			raise NbaseException("fail to begin tx. http ret:%d" % ret);

		json = decode_to_json(data)
		if json['retcode'] <> 0:
			raise NbaseException("fail to begin tx. retcode:%d" % json['retcode'])

		id = json['txid']
		g_rest_pool.put(rest)
		#rest.close()

		rest = g_rest_pool.get(json['ip'])
		if rest == None:
			rest = Restful(json['ip'], keyspace, ckey, format, json['http_port'])
		else:
			rest.set(keyspace, ckey, format)

		return HttpTransactionQuery(id, rest, self.query_port)

	def __begin_tx_with_rpc(self, keyspace, ckey, timeout_msec = 0, format="json"):
		rpc_ret = nbase_impl.nc_admin_alloc_ns_rpc_tx_res_t();
		ret = nbase_impl.nc_admin_begin_tx(self.ip, self.query_port, ckey, timeout_msec, rpc_ret)
		if ret < 0:
			nbase_impl.nc_admin_free_ns_rpc_tx_res_t(rpc_ret)
			raise NbaseException("fail to begin tx. retcode:%d" % ret)
		ret = nbase_impl.nc_admin_get_id_ip_port_from(rpc_ret)
		return RpcTransactionQuery(ret['id'], ret['ip'], ret['rpc_port'], keyspace, ckey)

	def is_tx_alive(self, txid):
		return nbase_impl.nc_admin_is_txid_valid(self.ip, self.query_port, txid) == 1

	def stop(self):
		return self.kill()		

	def start(self, mgmt_ip, mgmt_port, revive_cmd=None):
		if revive_cmd is None:
			self.exec_cmd_in('$NBASE/etc/init.d/nbase-server start')
		else:
			self.run(revive_cmd)

		num_iter = 10
		for cnt in range(num_iter):
			state = read_mbr(mgmt_ip, mgmt_port).get_cs_state(self.ip, self.port)
			if state == 'N':
				return True
			time.sleep(1)
		return False
	
	def get_schema_cubrid(self, mgmt_ip, mgmt_port):
		cmd = "$NBASE/bin/nbase_query_admin -m " + mgmt_ip + ":" + str(mgmt_port) + ' -q "show schema cubrid"'
		results = self.exec_cmd_out(cmd)
		if "show schema cubrid" in results[0]:
			del results[0]
		if "Successfully done" in results[len(results)-1]:
			del results[len(results)-1]
		schema = ''.join(results)
		return schema

class MigrationSummary:
	def __init__(self, mgmt_addr, mig_sum):
		self.mgmt_addr = mgmt_addr
		self.num_q0 = mig_sum.n_q0
		self.num_runq = mig_sum.n_runq
		self.num_failed = mig_sum.n_failed
		self.concurrency = mig_sum.concurrency
	
	def ends(self):
		return self.num_q0 == 0 and self.num_runq == 0 and self.num_failed == 0

	def __str__(self):
		return "mig status q0:%d, run:%d, failed:%d" % (self.num_q0, self.num_runq, self.num_failed)

	def get_detail(self):
		s = '======================================================================\n'
		s += ' Migration Summary of nStore at [%s]\n' % self.mgmt_addr
		s += '======================================================================\n'
		s += '  In Queue 0      : %10u\n' % self.num_q0
		s += '  Running jobs    : %10u\n' % self.num_runq
		s += '  Jobs retrying   : %10u\n' % self.num_failed
		s += '  Concurrency     : %10u\n' % self.concurrency
		s += '======================================================================\n'
		return s

class SyncMgr:
	def __init__(self, mgmt_ip, mgmt_port = DEFAULT_MANAGEMENT_NODE_PORT):
		self.mgmt_ip = mgmt_ip
		self.mgmt_port = mgmt_port
		ret = nbase_impl.nc_admin_register_cs_list(mgmt_ip, mgmt_port)
		if ret <> 0 and ret <> -13003: #EXIST
			raise NbaseException("fail to register cs list, mgmt from %s:%d, ret:%d" % (mgmt_ip, mgmt_port, ret), ret)

	def close(self):
		ret = nbase_impl.nc_admin_unregister_cs_list()
		g_sync_mgr_initialized = False

	def get_rand_cs(self, ckey=None, ex_ip_list=None):
		ret = nbase_impl.nc_admin_get_rand_cs(ckey, ex_ip_list and ';'.join(ex_ip_list) or None)
		if ret['ret'] == 0:
			return CS(ret['ip'], ret['port'])
		return None

	def get_rand_slave_cs(self, ckey=None, ex_ip_list=None):
		ret = nbase_impl.nc_admin_get_rand_slave_cs(ckey, ex_ip_list and ';'.join(ex_ip_list) or None)
		if ret['ret'] == 0:
			cs = CS(ret['ip'], ret['port'] - 1)
			cs.query_port = ret['port']
			return cs
		return None

class Mgmt (MembershipDaemon):
	def __init__(self, ip, port = DEFAULT_MANAGEMENT_NODE_PORT):
		MembershipDaemon.__init__(self, ip, port, False)
	
	def empty_node(self, nid):
		ret = nbase_impl.nc_admin_empty_node(self.ip, self.port, nid)
		return ret

	def delete_node(self, nid):
		ret = nbase_impl.nc_admin_delete_node(self.ip, self.port, nid)
		return ret

	def add_node(self): 
		ret = nbase_impl.nc_admin_add_node(self.ip, self.port)
		return ret

	def delete_cs(self, cs_addr, cs_port, nid):
		ret = nbase_impl.nc_admin_delete_cs(self.ip, self.port, cs_addr, cs_port, nid)
		return ret

	def add_cs(self, cs_addr, cs_port, nid):
		ret = nbase_impl.nc_admin_add_cs(self.ip, self.port, cs_addr, cs_port, nid)
		return ret

	def change_cs_state(self, cs_addr, cs_port, state):
		if state <> 'f' and state <> 'F' and state <> 'I' and state <> 'S' and state <> 'N' and state <> 'U':
			ex_msg = "state change is only allowed 'I', 'U', 'N', 'S', 'F' or 'f'"
			raise NbaseException(ex_msg)

		ret = nbase_impl.nc_admin_set_mbr_cs_state(self.ip, self.port, cs_addr, cs_port, state)
		time.sleep(1)
		self.reload_sync_mgr()
		return ret
		
	def exec_global_query(self, keyspace, query_str, timeout_msec = DEFAULT_RPC_TIMEOUT_MSEC, ckey=None):
		cs = self.select_available_cs()
		q = Query(cs.ip, cs.port, keyspace, ckey, "json")
		return q.run(query_str, timeout_msec)
	
	def exec_query_all_ckey_list(self, keyspace, ckey_list, query_str, timeout_msec = DEFAULT_RPC_TIMEOUT_MSEC):
		cs = self.select_available_cs()
		q = Query(cs.ip, cs.port, keyspace, None, "json")

		file_path = "/tmp/exec_query_all_ckey_list_result.%d" % int(time.time()) 
		ret = q.run_all_ckey_list(file_path, self.ip, self.port, ckey_list, query_str, timeout_msec)
		if ret < 0:
			return ret, None

		fp = open(file_path, 'r')
		results = fp.readline().split('\x00')
		results_json = []
		for res in results:
			if len(res) > 0:
				results_json.append(decode_to_json(res))
		return ret, results_json 

	def get_sync_mgr(self):
		global g_sync_mgr
		if g_sync_mgr == None:
			g_sync_mgr = SyncMgr(self.ip, self.port)
		return g_sync_mgr

	def reload_sync_mgr(self):
		global g_sync_mgr
		if g_sync_mgr != None:
			g_sync_mgr.close()
		g_sync_mgr = SyncMgr(self.ip, self.port)
		return g_sync_mgr

	def get_rand_cs(self, ckey=None, ex_ip_list=None):
		return self.get_sync_mgr().get_rand_cs(ckey, ex_ip_list)

	def get_rand_slave_cs(self, ckey=None, ex_ip_list=None):
		return self.get_sync_mgr().get_rand_slave_cs(ckey, ex_ip_list)

	def get_nid_of(self, ckey):
		ret = nbase_impl.nc_admin_get_nid_of_ckey_from(self.ip, self.port, ckey)
		if ret < 0:
			raise NbaseException("fail to get nid of ckey:%s from %s:%d, ret:%d" % (ckey, self.ip, self.port, ret))
		return ret

	def __get(self, key, str):
		pos = str.find(key) + len(key) + 2
		end = str.find('<', pos)
		if end < 0:
			return None
		return str[pos:end]

	def __wrap(self, value):
		if value <> None:
			value = value.replace('\\n', '')
			return value.replace('\\t', '')
		return value

	def get_schema(self, keyspace, table_name):
		cs = self.select_available_cs()
		conn = httplib.HTTPConnection(cs.ip, cs.rest_port)
		conn.connect()
		try:
			
			conn.putrequest('GET', "/nbase/%s/query?nsql=%s" % (keyspace, urllib.quote_plus('show table %s' % table_name)))
			conn.putheader('Connection', 'Keep-Alive')
			conn.endheaders()

			conn.send('')

			res = conn.getresponse()
			if res.status == 200:
				ret = res.read()[24:-2]
				h = {}
				name = self.__wrap(self.__get('<name>', ret))
				is_local=True
				scope = self.__get('<scope>', ret)
				if scope <> None and self.__wrap(scope) == 'GLOBAL':
					is_local=False

				#h['scope'] = self.__wrap(self.__get('<scope>', ret))
				#h['prop'] = self.__wrap(self.__get('<prop>', ret))
				#print self.__get('<columns>', ret)[2:-2].strip()
				columns = self.__get('<columns>', ret)[2:-2].split('\\n\\t')
				#print columns
				columns = [c.replace('\\n', '') for c in columns]
				columns = [c.replace('\\t', '') for c in columns]
				columns = [c.replace('\\"', '"') for c in columns]
				columns = [c.strip() for c in columns]
				primary_keys = self.__wrap(self.__get('<primary keys>', ret))
				indices = ret[ret.find('<indices>') + len('<indices>'):].split('\\n\\t')
				indices = [i.replace('\\n', '') for i in indices]
				indices = [i.replace('\\t', '') for i in indices]
				indices = indices[1:]
				return Schema(name, columns, primary_keys, indices, is_local)

			raise NbaseException("fail to get schema of keyspace:%s, table:%s from %s:%d, http_res:%d" % (keyspace, table_name, self.ip, DEFAULT_CS_REST_PORT, res.status))
		finally:
			conn.close()

	def get_normal_cs(self, ex_cs_list = []):
		node_list = self.get_node_list()
		normal_cs = None
		for node in node_list:
			normal_cs = self.get_normal_cs_of_nid(node.nid, ex_cs_list)
			if normal_cs is not None:
				break
		return normal_cs

	def get_normal_cs_of_nid(self, nid, ex_cs_list = []):
		l = [cs for cs in self.get_mbr().get_node_from_nid(nid).get_cs_list() if cs.state == 'N' and not cs in ex_cs_list]
		if len(l) == 0:
			return None 
		return random.choice(l)

	def get_ckey_info(self, ckey):
		cgid = nbase_impl.nc_admin_get_ck_cgmapid_of_ckey(self.ip, self.port, ckey)
		if cgid > 0:
			return nbase_impl.nc_admin_get_ck_hash_of_ckey(ckey), cgid
		raise NbaseException("fail to get info of ckey:%s from %s:%d, ret:%d" % (ckey, self.ip, self.port, cgid))

	def filter_options(self, option_list):
		try:
			option_list.remove('-A')
		except ValueError:
			pass

		try:
			option_list.remove('-I')
		except ValueError:
			pass

		return ' '.join(option_list)

	def get_bin_name(self):
		return MGMT_BIN_NAME

	def __str__(self):
		return self.get_bin_name() + "@" + MembershipDaemon.__str__(self)

	def exec_ddl(self, keyspace, query_str, quite=True):
		return Query(self.ip, self.port, keyspace, None).run_ddl(query_str, quite)

	def run_gc(self):
		mbr = self.get_mbr()
		for node in mbr.node_list:
			cs = node.get_cs_list()[0]
			cs.run_gc()

	def request_rebalance(self):
		ret = nbase_impl.nc_admin_request_rebalance(self.ip, self.port)
		if ret < 0:
			ex_msg = "fail to request rebalance, ret:%d (%s)" % (ret, errstr(ret))
			raise NbaseException(ex_msg)
		
	def request_rebalance_with_cg_admin(self):
		p = popen2.Popen3('$NBASE/bin/nbase_cg_admin -m %s:%d -B' % (self.ip, self.port)) 
		p.wait()
		ret = p.poll()
		return ret

	def request_migration(self, cgid, nid):
		item = self.get_cgmap().get_cgmap_item_by_cgid(cgid)
		if item == None:
			raise NbaseException("invalid cgid:%d" % cgid)
		elif item.nid == nid:
			raise NbaseException("same nid:%d was given" % nid)

		ret = nbase_impl.nc_admin_request_migration(self.ip, self.port, cgid, nid)
		if ret <> 0:
			raise NbaseException("fail to request migration. ret:%d" % ret)

	def get_replication_delay(self, nid):
		ret = nbase_impl.nc_admin_get_replication_delay(self.ip, self.port, nid)
		if ret < 0:
			raise NbaseException("fail to get replication delay of nid:%d. ret:%d" % (nid, ret))
		return ret


#	def change_state(self, nid, state):
#		u_state = state.upper()
#		if u_state <> 'F' and u_state <> 'I':
#			ex_msg = "state change is only allowed 'I', 'F' or 'f'"
#			raise NbaseException(ex_msg)
#		
#		return nbase_impl.nc_set_mbr_state(self.ip, self.port, nid, ord(state))

	def get_mig_sum(self):
		sum = nbase_impl.nm_rpc_mig_sum_t()
		nbase_impl.nc_get_mig_sum(self.ip, self.port, sum)
		return MigrationSummary(self.ip, sum)

	def wait_cs_state(self, nid, target_state, max_wait_sec = 600):
		tcs = self.new_CS_of(nid)
		if tcs.state == target_state:
			return
		for i in range(1, max_wait_sec):
			time.sleep(1)
			tcs = self.new_CS_of(nid)
			if tcs.state == target_state:
				return

		ex_msg = "Fail to wait state %s, but %s" % (target_state, tcs.state)
		raise NbaseException(ex_msg)

	def wait_until_migration_ends(self, log_func = None, max_wait_sec = 600, check_interval = 1):
		if log_func:
			log_func("wait until migration ends")

		time.sleep(1)
		if self.get_mig_sum().ends():
			if log_func:
				log_func("migration ends")
			return

		i = 0
		while i < max_wait_sec:
			time.sleep(check_interval)
			i += check_interval
			sum = self.get_mig_sum()
			if sum.ends():
				if log_func:
					log_func("migration ends")
				return

			if log_func:
				log_func(str(sum))

		ex_msg = "migration wait time exceeds. %d sec" % max_wait_sec
		raise NbaseException(ex_msg)



# globally init
init()
