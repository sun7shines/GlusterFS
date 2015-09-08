# -*- coding: utf-8 -*-

import syslog
import traceback
import subprocess

import support.uuid_op
import support.cmd_exe
import vmd_utils
import operation.gluster.peer_db
import operation.gluster.peer_cmd

import operation.vstorage.storage_db_op
import dbmodule.db_module_interface

from dbmodule.db_op import *

import os

import time
import commands


def cmd_result(cmd):
    
    flag , output = commands.getstatusoutput(cmd)
    otx = []
    if output.strip():
        otx = output.split('\n')
    if flag is 0:
        return (flag,{'stdout':otx,'stderr':[]})
    else :
        return (flag,{'stdout':[],'stderr':otx})
    
def volume_create_cmd(data):
    
    volume_description = data['g_volume_description']
    volume_type = data['g_volume_type']
    stripe_count = data['g_stripe_count']
    replica_count = data['g_replica_count']
    transport_type = data['g_transport_type']
    new_bricks = data['g_new_bricks']
    
    brick_lines = {}
    
    if stripe_count == 1:
        stripe_line = ''
    else:
        stripe_line = 'stripe %s' % (str(stripe_count))
    if replica_count == 1:
        replica_line = ''
    else:
        replica_line = 'replica %s' % (str(replica_count))
    
    for brick in new_bricks:
        distribute_id = brick['distributeId']
        replica_id = brick['replicaId']
        if not brick_lines.has_key(distribute_id):
            brick_lines[distribute_id] = {}
        
        if not brick_lines[distribute_id].has_key(replica_id):
            brick_lines[distribute_id][replica_id] = ''
            
        brick_lines[distribute_id][replica_id] = brick_lines[distribute_id][replica_id] + '%s:%s/glusterfs_storage ' % (brick['peer_ip'],brick['storage_path'])     
    
    #cmd_lines = {}
    brick_line = ''
    for key_distribute in brick_lines:
        distribute_lines = brick_lines[key_distribute]
        replica_lines = ''
        for key_replica in distribute_lines:
            replica_lines = replica_lines +  distribute_lines[key_replica]
        #cmd_lines[key_distribute] = replica_lines
        brick_line = brick_line + replica_lines
    
    # volume create <NEW-VOLNAME> [stripe <COUNT>] [replica <COUNT>] [disperse [<COUNT>]] [redundancy <COUNT>] 
    # [transport <tcp|rdma|tcp,rdma>] <NEW-BRICK>?<vg_name>... [force] 
    # - create a new volume of specified type with mentioned bricks
    cmd = "(echo 'y' ;) | gluster volume create %s %s %s transport %s %s" % (volume_description,stripe_line,replica_line,transport_type,brick_line) 
    syslog.syslog(syslog.LOG_ERR,'volume_create_cmd: '+cmd)
    return cmd

def volume_start_cmd(data):
    
    volume_description = data['g_volume_description']
    # # volume start <VOLNAME> [force] - start volume specified by <VOLNAME>
    
    cmd = 'gluster volume start %s' % (volume_description)
    return cmd

def add_brick_cmd(volume_description,bricks):
    
    brick_lines = {}
    for brick in bricks:
        distribute_id = brick['distributeId']
        replica_id = brick['replicaId']
        if not brick_lines.has_key(distribute_id):
            brick_lines[distribute_id] = {}
        
        if not brick_lines[distribute_id].has_key(replica_id):
            brick_lines[distribute_id][replica_id] = ''
            
        brick_lines[distribute_id][replica_id] = brick_lines[distribute_id][replica_id] + '%s:%s/glusterfs_storage ' % (brick['peer_ip'],brick['storage_path'])     
    
    brick_line = ''
    for key_distribute in brick_lines:
        distribute_lines = brick_lines[key_distribute]
        replica_lines = ''
        for key_replica in distribute_lines:
            replica_lines = replica_lines +  distribute_lines[key_replica]
        #cmd_lines[key_distribute] = replica_lines
        brick_line = brick_line + replica_lines
      
    # volume add-brick <VOLNAME> [<stripe|replica> <COUNT>] <NEW-BRICK> ... [force] 
    # add brick to volume <VOLNAME>
       
    cmd = 'gluster volume add-brick %s %s' % (volume_description,brick_line) 
    syslog.syslog(syslog.LOG_ERR,'add_brick_cmd: '+cmd)
    return cmd
    
def remove_brick_cmd(volume_description,bricks):
    
    brick_line = ''
    for brick in bricks:
        brick_line = brick_line + '%s:%s/glusterfs_storage ' % (brick['peer_ip'],brick['storage_path'])     
    cmd = "(echo 'y';)| gluster volume remove-brick %s %s force" % (volume_description,brick_line) 
    syslog.syslog(syslog.LOG_ERR,'remove_brick_cmd: '+cmd)
    return cmd

def remove_brick_cmd_start(volume_description,bricks):
    
    brick_line = ''
    for brick in bricks:
        brick_line = brick_line + '%s:%s/glusterfs_storage ' % (brick['peer_ip'],brick['storage_path'])     
    cmd = "(echo 'y';)| gluster volume remove-brick %s %s start" % (volume_description,brick_line) 
    syslog.syslog(syslog.LOG_ERR,'remove_brick_cmd: '+cmd)
    return cmd

def remove_brick_cmd_status(volume_description,bricks):
    
    brick_line = ''
    for brick in bricks:
        brick_line = brick_line + '%s:%s/glusterfs_storage ' % (brick['peer_ip'],brick['storage_path'])     
    cmd = "(echo 'y';)| gluster volume remove-brick %s %s status" % (volume_description,brick_line) 
    syslog.syslog(syslog.LOG_ERR,'remove_brick_cmd: '+cmd)
    return cmd

def remove_brick_cmd_commit(volume_description,bricks):
    
    brick_line = ''
    for brick in bricks:
        brick_line = brick_line + '%s:%s/glusterfs_storage ' % (brick['peer_ip'],brick['storage_path'])     
    cmd = "(echo 'y';)| gluster volume remove-brick %s %s commit" % (volume_description,brick_line) 
    syslog.syslog(syslog.LOG_ERR,'remove_brick_cmd: '+cmd)
    return cmd

def replace_brick_start_cmd(volume_description,brick,new_brick):
    
    brick_line = "%s:%s/glusterfs_storage %s:%s/glusterfs_storage" % (brick['peer_ip'],brick['storage_path'],new_brick['peer_ip'],new_brick['storage_path'])
    cmd = 'gluster volume replace-brick %s %s start' % (volume_description,brick_line)
    syslog.syslog(syslog.LOG_ERR,'replace_brick_start_cmd: '+cmd)
    return cmd

def replace_brick_status_cmd(volume_description,brick,new_brick):
    
    brick_line = "%s:%s/glusterfs_storage %s:%s/glusterfs_storage" % ((brick['peer_ip'],brick['storage_path'],new_brick['peer_ip'],new_brick['storage_path']))
    cmd = 'gluster volume replace-brick %s %s status' % (volume_description,brick_line)
    syslog.syslog(syslog.LOG_ERR,'replace_brick_status_cmd: '+cmd)
    return cmd

def replace_brick_commit_cmd(volume_description,brick,new_brick):
    
    brick_line = "%s:%s/glusterfs_storage %s:%s/glusterfs_storage" % ((brick['peer_ip'],brick['storage_path'],new_brick['peer_ip'],new_brick['storage_path']))
    cmd = 'gluster volume replace-brick %s %s commit' % (volume_description,brick_line)
    return cmd

def replace_brick_commit_cmd_force(volume_description,brick,new_brick):
    
    brick_line = "%s:%s/glusterfs_storage %s:%s/glusterfs_storage" % ((brick['peer_ip'],brick['storage_path'],new_brick['peer_ip'],new_brick['storage_path']))
    cmd = 'gluster volume replace-brick %s %s commit force' % (volume_description,brick_line)
    return cmd
