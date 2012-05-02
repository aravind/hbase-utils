#!/usr/bin/env python

from __future__ import with_statement

import glob
import httplib
import os
import re
import socket
import string
import sys
import time
import xml.etree.ElementTree

from fabric.api import abort
from fabric.api import env
from fabric.api import get
from fabric.api import hosts
from fabric.api import local
from fabric.api import put
from fabric.api import roles
from fabric.api import run
from fabric.api import serial
from fabric.api import settings
from fabric.api import sudo
from fabric.context_managers import hide
from fabric.contrib.project import rsync_project
from fabric.state import connections

HBASE_HOME = "/home/aravind/hbase"
HADOOP_HOME = "/home/aravind/hadoop"
HOME_DIR = "/home/aravind"
FAB_DIR = os.path.dirname(env.real_fabfile)
MAX_GRACEFUL_ATTEMPTS = 5

if not env.hosts:
  regionservers = open(HOME_DIR + "/hbase_conf/regionservers", "r").readlines()
  env.hosts = filter(lambda x: x.find("#") != 0,
                     map(lambda x: string.split(x)[0].rstrip(),
                         regionservers))

def _parse_release(release_str):
  prod, rel_str = release_str.split("-", 1)
  version, revision = rel_str.rsplit("-", 1)
  return prod, version, revision

def _get_xml_prop(pname, eltree):
  """Get the (first seen) value of a property from the XML file."""
  prop_next = False
  for el in eltree.findall("property"):
    for c in el.getchildren():
      if prop_next and (c.tag == "value"):
        return c.text
      if c.text == pname:
        prop_next = True
  return None

def _get_zk_servers(eltree):
  """Generate the list of zookeeper servers in the host:port,... format. """
  zk_ensemble = _get_xml_prop("hbase.zookeeper.quorum", eltree)
  if zk_ensemble == None:
    zk_ensemble = ["localhost"]
  zk_client_port = _get_xml_prop("hbase.zookeeper.property.clientPort", eltree)
  if zk_client_port == None:
    zk_client_port = 2181
  zk_servers = []
  for server in zk_ensemble.split(","):
    zk_servers.append(server + ":" + str(zk_client_port))
  return ",".join(zk_servers)

eltree = xml.etree.ElementTree.parse(HOME_DIR + "/hbase_conf/hbase-site.xml")
root_znode = _get_xml_prop("zookeeper.znode.parent", eltree)
zk_client = FAB_DIR + "/zkclient.py " + _get_zk_servers(eltree)
rs_port = _get_xml_prop("hbase.regionserver.info.port", eltree)
if rs_port == None:
  rs_port = 60030

@hosts("localhost")
def _zkop(op, path):
  with settings(hide('warnings', 'stderr'),
                warn_only=True):
    output = local(" ".join([zk_client, op, path]), capture=True)
    return filter(lambda x: x != '', output.rstrip("\n").split("\n"))

#
# HBase deployment.
#

@hosts("face")
def prep_release(tarfile):
  """Copies the tar file from face and extract it.
  A new hbase-release-rev directory is created on the master server.
  It then creates a symlink from hbase-release.jar to hbase.jar.  It
  then wipes out the conf directory and makes it a symlink to
  ~hbase/hbase_conf.
  """
  release_dir = os.path.basename(tarfile).rstrip(".tar.gz")
  prod, version, revision = _parse_release(release_dir)
  release_dir = prod + "-" + version + "-" + revision
  release_dir = HOME_DIR + "/" + release_dir
  local("test ! -d " + release_dir)
  get(tarfile, HOME_DIR + "/hbase.tar.gz")
  local("mkdir " + release_dir)
  local("tar -xzf " + HOME_DIR + "/hbase.tar.gz --strip 1 -C " + release_dir)
  local("test -f " + release_dir + "/" + prod + "-" + version + ".jar")
  local("cd " + release_dir + " && ln -s " + prod + "-" + version + ".jar " + prod + ".jar")
  local("cd " + release_dir + " && rm -rf conf && ln -s ../" + prod + "_conf conf")
  with settings(hide('warnings', 'stdout', 'stderr'),
                warn_only=True):
    local("echo 'balance_switch false' | " + HBASE_HOME + "/bin/hbase shell")
  local("cd " + " && rm -f " + prod + " && ln -s " + release_dir + " " + prod)

def dist_release(release_dir, link_dir):
  """Rsyncs the release to the region servers.
  """
  new_rel_dir_name = os.path.basename(release_dir)
  with settings(hide('warnings', 'stdout', 'stderr'),
                warn_only=True):
    run("cp -LRp " + link_dir + " " + new_rel_dir_name)
    rsync_project(local_dir=release_dir,
                  remote_dir=HOME_DIR,
                  extra_opts="--links --quiet",
                  delete=True)
    rsync_project(local_dir=link_dir,
                  extra_opts="--links --quiet",
                  remote_dir=HOME_DIR)

def dist_hbase(release_dir):
  """Rsyncs the hbase release to the region servers.
  """
  dist_release(release_dir, HBASE_HOME)

def dist_hadoop(release_dir):
  """Rsyncs the hadoop release to the region servers.
  """
  dist_release(release_dir, HADOOP_HOME)

def unload_regions():
  """Un-load HBase regions on the server so it can be shut down."""
  with settings(hide('warnings', 'stdout', 'stderr'),
                warn_only=True):
    local("HBASE_NOEXEC=true " + HBASE_HOME + "/bin/hbase org.jruby.Main " +
          FAB_DIR + "/region_mover.rb unload " + env.host_string)

@hosts("localhost")
def clear_all_draining_nodes():
  """Remove all servers under the Zookeeper /draining znode"""
  draining_servers = _zkop("get", root_znode + "/draining")
  for node in draining_servers:
    _zkop("delete", root_znode + "/draining/" + node)

@hosts("localhost")
def list_draining_nodes():
  """List all servers under the Zookeeper /draining znode"""
  print "\n".join(_zkop("get", root_znode + "/draining"))

def _do(command):
  with settings(hide('warnings', 'stdout', 'stderr'),
                warn_only=True):
    run(command)

def do(command):
  """Run an arbitrary command on the remote host"""
  run(command)

def clear_rs_from_draining():
  """Remove the regionserver from the draining state."""
  for node in _zkop("get", root_znode + "/draining"):
    if env.host_string in node:
      _zkop("delete", root_znode + "/draining/" + node)

def _get_rs_from_zk():
  """Get the HBase representation of a hostname."""
  host_list = _zkop("get", root_znode + "/rs")
  return filter(lambda x: env.host_string + "," in x, host_list)

def add_rs_to_draining():
  """Put the regionserver into a draining state."""
  for node in _get_rs_from_zk():
    _zkop("create", root_znode + "/draining/" + node)

@hosts("localhost")
def disable_balancer():
  """Disable the balancer."""
  with settings(hide('warnings', 'stdout', 'stderr'),
                warn_only=True):
    local("echo 'balance_switch false' | " + HBASE_HOME + "/bin/hbase shell")

@hosts("localhost")
def enable_balancer():
  """Balance regions and enable the balancer."""
  with settings(hide('warnings', 'stdout', 'stderr'),
                warn_only=True):
    local("HBASE_NOEXEC=true " + HBASE_HOME + "/bin/hbase org.jruby.Main " +
          FAB_DIR + "/region_mover.rb balance -l 3")
    local("echo 'balance_switch true' | " + HBASE_HOME + "/bin/hbase shell")

def hbase_stop():
  """Stop hbase (WARNING: does not unload regions)."""
  _do(HBASE_HOME + "/bin/hbase-daemon.sh stop regionserver")

def hadoop_stop():
  """Start hadoop."""
  _do(HOME_DIR + "/hadoop/bin/hadoop-daemon.sh stop datanode")
  _do(HOME_DIR + "/hadoop/bin/hadoop-daemon.sh stop tasktracker")

def hadoop_start():
  """Start hadoop."""
  _do(HOME_DIR + "/hadoop/bin/hadoop-daemon.sh start datanode")
  _do("/usr/bin/cgexec -g memory:daemons/tt -g blkio:daemons/tt --sticky " +
     HOME_DIR + "/hadoop/bin/hadoop-daemon.sh start tasktracker")

def hbase_start():
  """Start hbase."""
  _do(HBASE_HOME + "/bin/hbase-daemon.sh start regionserver")

def jmx_kill():
  """Kill JMX collectors."""
  _do("pkill -f com.stumbleupon.monitoring.jmx")

def thrift_stop():
  """Stop thrift."""
  _do(HBASE_HOME + "/bin/hbase-daemon.sh stop thrift")

def thrift_start():
  """Start thrift."""
  _do(HBASE_HOME + "/bin/hbase-daemon.sh start thrift")

def thrift_restart():
  """Re-start thrift."""
  thrift_stop()
  thrift_start()

def reboot_server():
  """Reboot the box."""
  sudo("/sbin/reboot", shell=False)
  client = connections[env.host_string]
  client.close()
  del connections[env.host_string]

def sync_puppet():
  """Sync puppet on the box."""
  with settings(hide('warnings', 'stdout', 'stderr'),
                warn_only=True):
    sudo("/usr/sbin/puppetd --test", shell=False)

def _desired_state_check(desired_state=True, current_state=False, die=True):
  if (desired_state != current_state):
    if die:
      abort("Server state NOT okay.")
    print("Server state NOT okay.")
  else:
    print("Server state okay.")
  return desired_state == current_state

def _get_rs_status_page():
  """Returns the status page from the server.  Returns an empty string if
  the server is down.
  """
  try:
    conn =  httplib.HTTPConnection(env.host_string, rs_port)
    conn.request("GET", "/rs-status")
    response = conn.getresponse()
    data = response.read()
    return data
  except (httplib.HTTPResponse, socket.error):
    return ""

def assert_release(release, rev, same=True, die=True):
  """Check the release running on the server.
  Depending on the die flag, abort as needed.  This can be called from
  the command line, but you can't change the assert_state and die
  flags from the CL.  Fabric converts all arguments to strings and
  boolean flags and strings don't mix.  When called from the CL, this
  simply checks that the server is running the expected release and
  aborts otherwise.
  """
  data = _get_rs_status_page()
  state_string = " is "
  if not same:
    state_string = " is not "
  print("Checking that the server" + state_string + "running release: " +
        release + ", rev: " + rev)
  server_state = (string.find(data, release + ", r" + rev) != -1)
  return _desired_state_check(same, server_state, die)

def region_count():
  """Returns a count of the number of regions in the server.
  """
  data = _get_rs_status_page()
  re_matches = re.search(" numberOfOnlineRegions=(\d+),", data)
  if re_matches:
    print("Server has " + str(re_matches.group(1)) + " regions.")
    return int(re_matches.group(1))
  print("Could not contact server to get region count.")
  return -1

def assert_regions(empty=True, die=True):
  """Check that all the regions have been vacated from the server.
  Abort depending on the die flag.  From the CL, this can only be
  called to assert that the server doesn't have any regions, see the
  doc for assert_release.
  """
  state_string = " is not "
  if not empty:
    state_string = " is "
  print("Checking that the server" + state_string + "serving regions.")
  server_state = (region_count() == 0)
  return _desired_state_check(empty, server_state, die)

def assert_configs(same=True, die=False):
  """Check that all the region servers have the same config.
  Runs an md5sum on the config directory and dies if the configs are
  different.
  """
  with settings(hide('warnings', 'running', 'stdout', 'stderr')):
    current_md5 = local("/usr/bin/md5sum " + HOME_DIR + "/hbase_conf/* " + HOME_DIR +
                        "/hadoop_conf/* | /usr/bin/md5sum", capture=True)
  current_md5 = current_md5.split()[0]
  with settings(hide('warnings', 'running', 'stdout', 'stderr')):
    server_md5 = run("/usr/bin/md5sum " + HOME_DIR + "/hbase_conf/* " + HOME_DIR +
                     "/hadoop_conf/* | /usr/bin/md5sum")
  server_md5 = server_md5.split()[0]
  state_string = " the "
  if not same:
    state_string = " not the "
  print("Checking that the configs on the server are" + state_string +
        "same as those on master.")
  current_state = server_md5 == current_md5
  return _desired_state_check(same, current_state, die)

def deploy_hbase(tarfile):
  """Deploy the new hbase release to the regionserver.
  The script distributes the HBase release to the region servers, and
  gracefully restarts them after asserting that the new release is not
  already running on the regionserver.
  """
  release_dir = os.path.basename(tarfile).rstrip(".tar.gz")
  prod, version, revision = _parse_release(release_dir)
  release_dir = HOME_DIR + "/" + release_dir
  print("Deploying: " + prod +
        ", Version: " + version +
        ", Build: " + revision)
  dist_hbase(release_dir)
  if (assert_release(version, revision, False, die=False) or
      _get_rs_status_page() == ""):
    rolling_restart()
  assert_release(version, revision, True, die=True)

def _hbase_gstop():
  """HBase graceful stop.
  Runs the following sequence: disable_balancer, assert_configs,
  unload_regions, assert_regions, hbase_stop, hbase_start, load_regions,
  thrift_restart
  """
  if not assert_configs(same=True, die=False):
    sync_puppet()
    assert_configs(same=True, die=True)
  attempt = 0
  while (attempt < MAX_GRACEFUL_ATTEMPTS):
    unload_regions()
    attempt += 1
    time.sleep(5)
    if (region_count() != 0):
      print ("Unable to drain server after %s attempt(s)" % attempt)
    else:
      print ("Server drained in %s attempt(s)" % attempt)
      break
  hbase_stop()

def hbase_gstop():
  """HBase graceful stop.
  """
  disable_balancer()
  _hbase_gstop()

def rolling_restart():
  """Rolling restart of the whole cluster.
  Runs the following sequence: assert_configs, unload_regions, assert_regions,
  hbase_stop, hbase_start, load_regions, thrift_restart
  """
  add_rs_to_draining()
  _hbase_gstop()
  if (_get_rs_status_page() == ""):
    hbase_start()
    time.sleep(60)
    clear_rs_from_draining()
  thrift_restart()
  time.sleep(5)
  jmx_kill()

def rolling_reboot():
  """Rolling reboot of the whole cluster.
  Runs the following sequence: assert_configs, unload_regions, assert_regions,
  hbase_stop, hbase_start, load_regions, thrift_restart
  """
  add_rs_to_draining()
  _hbase_gstop()
  reboot_server()
  time.sleep(300)
  count = region_count()
  if (count == -1):
    time.sleep(60)
    hadoop_start()
    time.sleep(10)
    hbase_start()
    time.sleep(10)
    thrift_start()
    time.sleep(60)
    count = region_count()
    if (count == -1):
      abort("RS did NOT reboot/restart correctly.")
    clear_rs_from_draining()
