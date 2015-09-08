# -*- coding: utf-8 -*-

import system.network.dns_service_op

import operation.gluster.volume_db
import operation.gluster.volume_clr
import operation.gluster.volume_ifo
import operation.gluster.volume_cmd

import os
import time

def data_add(data,param):
    
    volume_uuid = param['volume_uuid']
    bricks = param['bricks']
    
    host_ip = system.network.dns_service_op.get_localhost_ip()
    
    volumeobj = operation.gluster.volume_db.get_gluster_volumeobj(volume_uuid)
    if not volumeobj:
        return False,host_ip+'volume does not exists'  
     
    volume_description = volumeobj['description']
    stripe_count = volumeobj['stripecount']
    replica_count = volumeobj['replicacount']
    distribute_count = volumeobj['distributedcount']
    brick_count = len(bricks)
    
    data['g_volume_uuid'] =  volume_uuid
    data['g_p_bricks'] = bricks
    data['g_host_ip'] = host_ip
    data['g_volume_description'] = volume_description
    data['g_stripe_count'] = stripe_count
    data['g_replica_count'] = replica_count
    data['g_distribute_count'] = distribute_count
    data['g_brick_count'] = brick_count
    return True,''

def pre_add(data):
    
    brick_count = data['g_brick_count']
    distribute_count = data['g_distribute_count']
    host_ip = data['g_host_ip']
    bricks = data['g_p_bricks']
    
    operation.gluster.volume_clr.clear_resources()
    
    if brick_count % distribute_count != 0:
        return False,host_ip+'bricks count error ' + str(brick_count)

    flag,msg = operation.gluster.volume_db.get_new_bricks(data)
    if not flag:
        return False,msg
    new_bricks = data['g_new_bricks']
    if len(bricks) != len(new_bricks):
        return False,host_ip+'get new bricks error'
    
    flag,volume_infos = operation.gluster.volume_ifo.get_volume_infos()
    if not flag:
        return False,host_ip+'get volume info failed' 
    
    flag,msg = operation.gluster.volume_clr.clear_used_bricks(volume_infos, new_bricks, host_ip)
    if not flag:
        return False,msg
    
    operation.gluster.volume_clr.delete_brick_attr(new_bricks,delete=True)
    
    data['g_new_bricks'] = new_bricks
    
    return True,''

def cmd_add(data):
    
    volume_description = data['g_volume_description']
    new_bricks = data['g_new_bricks']
    host_ip = data['g_host_ip']
    
    cmd = operation.gluster.volume_cmd.add_brick_cmd(volume_description, new_bricks)
    if 0 != os.system(cmd):
        return False,host_ip+'add brick failed'
    
    # volume rebalance <VOLNAME> {{fix-layout start} | {start [force]|stop|status}} - 
    # rebalance operations
    
    cmd = 'gluster volume rebalance %s fix-layout start' % (volume_description)
    if 0 != os.system(cmd):
        return False,host_ip+'rebalance volume to fix-layout failed'
    
    time.sleep(3)
    
    flag,volume_info = operation.gluster.volume_ifo.get_volume_infos()
    if not flag or not volume_info:
        return False,host_ip+'get volume info failed' 
    
    data['g_volume_info'] = volume_info
    
    return True,''

