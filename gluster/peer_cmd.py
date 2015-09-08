# -*- coding: utf-8 -*-

import syslog
import traceback
import os
import time

import sys

import support.uuid_op
import support.cmd_exe
import vmd_utils

import system.network.sent_rpc

import operation.vcluster.cluster_db_op
import dbmodule.db_module_interface
from dbmodule.db_op import *

def do_detach_peer(peer_ip):

    cmd = 'gluster peer detach %s force' % (peer_ip)
    if 0 != os.system(cmd):
        return False,'gluster peer detach cmd failed'
    
    return True,''

def detach_peer(target_ip,host_ip):

    try:
        (flag, psh) = vmd_utils.get_rpcConnection(target_ip)
        if not flag:
            return False,psh
        
        flag,msg = psh.do_detach_peer(host_ip)
        if not flag:
            return False,msg
    except:
        syslog.syslog(syslog.LOG_ERR,'detach_peer '+str(traceback.format_exc()))
    return True,''


def do_probe_peer(peer_ip):
    
    flag,state,_ = do_get_peer_state(peer_ip)
    if flag and state:
        do_detach_peer(peer_ip)
        
    cmd = 'gluster peer probe %s' % (peer_ip)
    if 0 != os.system(cmd):
        return False,'gluster peer probe cmd failed'
    
    return True,''

def do_get_peer_state(host_ip):
    
    cmd = 'gluster peer status'
    output = support.cmd_exe.cmd_exe(cmd)
    if not output[0]:
        return False,''
    lines = output[1]['stdout']
    
    hostname = ''
    state = ''
    peer_state= ''
    
    for line in lines:
        if not line.strip():
            continue
        if line.find('No peers present') != -1:
            return True,state,peer_state  
        if line.startswith('Hostname'):
            hostname = line.strip().split(':')[1].strip()
            
        if line.startswith('State'):
            state = line.split('(')[-1].split(')')[0].strip()
            peer_state = line.split(':')[1].strip().split('(')[0].strip()
            if hostname and state:
                if hostname == host_ip:
                    return True,state,peer_state
                
    return True,'',''


def get_peer_states():
    
    #包含本机状态
    states = []
    
    cmd = 'gluster peer status'
    output = support.cmd_exe.cmd_exe(cmd)
    if not output[0]:
        return False,''
    lines = output[1]['stdout']
    
    hostname = ''
    conn_state = ''
    peer_state= ''
    
    for line in lines:
        if not line.strip():
            continue
        if line.find('No peers present') != -1:
            return True,states
        if line.startswith('Hostname'):
            hostname = line.strip().split(':')[1].strip()
            
        if line.startswith('State'):
            conn_state = line.split('(')[-1].split(')')[0].strip()
            peer_state = line.split(':')[1].strip().split('(')[0].strip()
            if hostname and conn_state:
                states.append((hostname,conn_state,peer_state))
                
    return True,states

