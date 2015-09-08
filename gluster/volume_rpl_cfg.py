# -*- coding: utf-8 -*-

import operation.gluster.volume_clr
import operation.gluster.volume_cfg

import vmd_utils
import system.network.dns_service_op
import support.cmd_exe

import os
import syslog
import traceback
import time


def do_gluster_replace_brick(volume_desc,old_ip,old_storage,new_ip,new_storage):
    
    old_dir = '%s/glusterfs_storage' % (old_storage)
    brick_info = {'peer_ip':old_ip,'storage_path':old_dir}
    
    new_dir = '%s/glusterfs_storage' % (new_storage)
    new_brick = {'peer_ip':new_ip,'storage_path':new_dir}
    
    
    operation.gluster.volume_cfg.save_cfgs(volume_desc,brick_info,new_brick)
    flag,msg = operation.gluster.volume_cfg.modify_cfgs(volume_desc,brick_info,new_brick)
    if not flag:
        operation.gluster.volume_cfg.restore_cfgs(volume_desc,brick_info,new_brick)
        return False,msg
    
    operation.gluster.volume_cfg.clear_bak_cfgs(volume_desc)
    return True,''

def gluster_replace_brick(volume_desc,brick_info,new_brick):
    
    host_ip = system.network.dns_service_op.get_localhost_ip()
    
    operation.gluster.volume_sys.do_stop_gluster()
    
    flag = True
    
    try:
        if host_ip == brick_info['peer_ip']:
            operation.gluster.volume_clr.do_clear_brick(volume_desc,brick_info['peer_ip'],brick_info['storage_path'])
        else:
            #target_ip = brick_info['peer_ip']
            target_ip = brick_info['target_ip']
            
            (flag, psh) = vmd_utils.get_rpcConnection(target_ip)
            if flag:
                psh.do_clear_brick(volume_desc,brick_info['peer_ip'],brick_info['storage_path'])
    except:
        syslog.syslog(syslog.LOG_ERR,'gluster replace brick '+str(traceback.format_exc()))
                
    try:
        do_gluster_replace_brick(volume_desc,brick_info['peer_ip'],brick_info['storage_path'],new_brick['peer_ip'],new_brick['storage_path'])
        
        new_dir = '%s/glusterfs_storage' % (new_brick['storage_path'])
        cmd = "(vol=%s; brick=%s; setfattr -n  trusted.glusterfs.volume-id -v 0x$(grep volume-id \
/var/lib/glusterd/vols/$vol/info | cut -d= -f2 | sed 's/-//g') $brick) " % (volume_desc,new_dir)
        os.system(cmd)
        
        # rb_dst_brick.vol
        operation.gluster.volume_cfg.rewrite_rb_dst_brick(volume_desc,new_brick)
    except:
        syslog.syslog(syslog.LOG_ERR,'gluster replace brick '+str(traceback.format_exc()))
        flag = False
        
    operation.gluster.volume_sys.do_restart_gluster()

    if not flag:
        return False,'gluster replace brick failed,check the messages log'
    return True,''


def brick_replace_status(vol_desc,new_brick):

    cmd = 'find %s/glusterfs_storage/ -noleaf -print0 | xargs --null stat > /dev/null' % (new_brick['storage_path'])
    os.system(cmd)
    
    #判断命令是否执行成功?
    exists = False
    syslog.syslog(syslog.LOG_ERR,'brick_replace_status_start')
    
    cmd = 'gluster volume heal %s info' % (vol_desc)
    output = support.cmd_exe.cmd_exe(cmd)
    if not output[0]:
        return False,'get gluster replace info failed'
    lines = output[1]['stdout']
    
    syslog.syslog(syslog.LOG_ERR,'brick_replace_status'+str(output))
    
    for line in lines:
        x = line.strip()
        if not x:
            continue
        
        if x.find('self-heal-daemon is not running on') != -1:
            return False,''
        
        if x.startswith('Number of entries'):
            exists = True
            if x.find('Number of entries: 0') == -1:
                return False,''
            
    if not exists:
        return False,''
            
    return True,''

def wait_for_replace(vol_desc,new_brick):
    
    while True:
        flag,_ = brick_replace_status(vol_desc,new_brick)
        if flag:
            break
    
        time.sleep(15)
        
    return True
     
def replace_brick_status(volume_description,brick,new_brick):
    
    cmd = operation.gluster.volume_cmd.replace_brick_status_cmd(volume_description,brick,new_brick)
    output = support.cmd_exe.cmd_exe(cmd)
    if not output[0]:
        return False,'get volume replace brick status failed'
    lines = output[1]['stdout']
    syslog.syslog(syslog.LOG_ERR,'replace_brick_status '+str(lines))
    for line in lines:
        if not line.strip():
            continue
        if line.find('Current file') != -1:
            return True,'running' 
        if line.find('Migration complete') != -1:
            return True,'suc'
        if line.find('failed') != -1:
            return True,'failed'
    return True,'failed'
