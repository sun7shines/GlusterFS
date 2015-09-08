# -*- coding: utf-8 -*-




import operation.gluster.volume_db
import operation.gluster.volume_sys
import vmd_utils

import os
import time
import syslog
import traceback

def do_clear_brick(volume_desc,brick_ip,brick_storage):

    brick_dir = '%s/glusterfs_storage' % (brick_storage)
    try:
        pid_file = '/var/lib/glusterd/vols/%s/run/%s%s.pid' % (volume_desc,brick_ip,('-').join(brick_dir.split('/')))
        if not os.path.exists(pid_file):
            return True
        
        f = open(pid_file)
        lines = f.readlines()
        f.close()
        pid = lines[0].strip()
        
        cmd = 'kill -9 %s' % (pid)
        syslog.syslog(syslog.LOG_ERR,'kill brick process: '+ cmd)
        os.system(cmd)
        
        cmd = 'rm -f %s' % (pid_file)
        os.system(cmd)
    except:
        syslog.syslog(syslog.LOG_ERR,'kill brick process: '+str(traceback.format_exc()))
        return False,'kill brick process failed'
                
    return True,''


def clear_peer_cfgs():
    
    os.system('/etc/init.d/glusterd stop')
    os.system('rm -f /var/lib/glusterd/glfs_ip')
    os.system('rm -f /var/lib/glusterd/peers/*')
    os.system('rm -rf /var/lib/glusterd/vols/*')
    os.system('/etc/init.d/glusterd start')
    time.sleep(10) 
    return True,''

def gluster_delete_vols(volume_descs):
    
    _,states = operation.gluster.peer_cmd.get_peer_states()
    _,states = operation.gluster.peer_db.convert_to_target_states(states)
    
    for target_ip,conn_state,peer_state in states:
        if conn_state == 'Connected' and peer_state == 'Peer in Cluster':
            try:
                (flag, psh) = vmd_utils.get_rpcConnection(target_ip)
                if not flag:
                    continue
                psh.do_stop_gluster()
            except:
                syslog.syslog(syslog.LOG_ERR,'glsuter rpc '+str(traceback.format_exc()))
                return False,'rpc stop %s gluster service failed' % (target_ip)
            
    operation.gluster.volume_sys.do_stop_gluster()
    try:
        operation.gluster.volume_clr.do_clear_vol(volume_descs)
    except:
        syslog.syslog(syslog.LOG_ERR,'delete vols: '+str(traceback.format_exc()))
    operation.gluster.volume_sys.do_restart_gluster()
    
    for target_ip,conn_state,peer_state in states:
        if conn_state == 'Connected' and peer_state == 'Peer in Cluster':
            try:
                (flag, psh) = vmd_utils.get_rpcConnection(target_ip)
                if not flag:
                    continue
                psh.do_clear_vol(volume_descs)
                
            except:
                syslog.syslog(syslog.LOG_ERR,'glsuter rpc '+str(traceback.format_exc()))
            finally:
                psh.do_restart_gluster()
    time.sleep(5)            
    return True,''

def delete_volume_by_cmd(volume_desc,time_secs):
    
    # volume delete <VOLNAME> - delete volume specified by <VOLNAME>
    # volume stop <VOLNAME> [force] - stop volume specified by <VOLNAME>

    res1 = 99
    res2 = 99
    stop_cmd = "(echo 'y';)|gluster volume stop %s" % (volume_desc)
    n = 0
    while n<3:
        output = operation.gluster.volume_cmd.cmd_result(stop_cmd)
        res1 = output[0]
        syslog.syslog(syslog.LOG_ERR,'stop volume res1: '+str(res1)+' '+str(output)+' '+str(n) )
        if res1 != 0  :
            n = n+1
            time.sleep(5)
        else:
            break
    
    delete_cmd = "(echo 'y';)|gluster volume delete %s" % (volume_desc)
    if 0 == res1 or res1 == 256:
        n = 0
        while n <3:
            output = operation.gluster.volume_cmd.cmd_result(delete_cmd)
            res2 = output[0]
            syslog.syslog(syslog.LOG_ERR,'delete volume res2: '+str(res2)+' '+str(output)+' '+str(n) )
            if res2 != 0:
                n = n+1
                time.sleep(5)
            else:
                break
    
    if res1==0 and res2==0:
        time.sleep(time_secs)
    
    if res1 or res2 or operation.gluster.volume_ifo.volume_info_exists(volume_desc):
        syslog.syslog(syslog.LOG_ERR,'stop volume '+stop_cmd+' res1: '+str(res1))
        syslog.syslog(syslog.LOG_ERR,'delete volume '+delete_cmd+' res2: '+str(res2))
        return False
    
    return True


def clear_resources():
    
    operation.gluster.volume_sys.do_restart_gluster()
    time.sleep(2)
    
    flag,volume_infos = operation.gluster.volume_ifo.get_volume_infos()
    if not flag:
        #若未获取到volume 信息，则无需清理
        return True,''
    
    flag,vc_vols = operation.gluster.volume_db.get_vc_vols()
    if not flag:
        #若未获取到volume 信息，则无需清理,防止删除信息
        return True,''
    
    delete_descs = []
    
    for volume_desc in [volume_desc for volume_desc in volume_infos]:
        if  volume_desc not in [x['description'] for x in vc_vols]:
            delete_descs.append(volume_desc)   
        
    delete_cfg = False
    
    for volume_desc in delete_descs:
        #命令执行间隔为两秒
        if not delete_volume_by_cmd(volume_desc,2):
            delete_cfg = True
            
    if delete_cfg:
        gluster_delete_vols(delete_descs)
    
    reset_descs = []
    for volume_desc in [volume_desc for volume_desc in volume_infos]:
        if operation.gluster.volume_sys.gluster_has_rbtask(volume_desc):
            reset_descs.append(volume_desc)
    
    if len(reset_descs) != 0:
        gluster_reset_rbstate(reset_descs)
        time.sleep(5)
    
    return True,''

def clear_single_host_resource():

    flag,volume_infos = operation.gluster.volume_ifo.get_volume_infos()
    if not flag:
        #若未获取到volume 信息，则无需清理
        return True,''
    
    flag,vc_vols = operation.gluster.volume_db.get_vc_vols()
    if not flag:
        #若未获取到volume 信息，则无需清理,防止删除信息
        return True,''
    
    delete_descs = []
    
    for volume_desc in [volume_desc for volume_desc in volume_infos]:
        if  volume_desc not in [x['description'] for x in vc_vols]:
            delete_descs.append(volume_desc)   

    reset_descs = []
    for volume_desc in [volume_desc for volume_desc in volume_infos]:
        if operation.gluster.volume_sys.gluster_has_rbtask(volume_desc):
            reset_descs.append(volume_desc)
                    
    #等gluster同步信息结束
    #if len(delete_descs)!= 0:
    #   time.sleep(5)
    #检查是否有其他任务在进行
        
    delete_cfg = False
    for volume_desc in delete_descs:
        #命令执行间隔为两秒
        if not delete_volume_by_cmd(volume_desc,2):
            delete_cfg = True

    if delete_cfg or len(reset_descs) != 0:
        
        operation.gluster.volume_sys.do_stop_gluster()
        if delete_cfg:
            do_clear_vol(delete_descs)
        if len(reset_descs) != 0:
            do_clear_rbstate(reset_descs)
        operation.gluster.volume_sys.do_restart_gluster()
        time.sleep(3)
    
    return True,''

def do_clear_rbstate(volume_descs):
    
    lines = ['rb_status=0\n']
    
    for volume_desc in volume_descs:
        try:
            rbpath = '/var/lib/glusterd/vols/%s/rbstate' % (volume_desc)
            f = open(rbpath,'w')
            f.writelines(lines)
            f.close()
        except:
            continue
        
    return True,''


def do_clear_vol(volume_descs):
    
    for volume_desc in volume_descs:
        vol_dir = '/var/lib/glusterd/vols/%s' % (volume_desc)
        run_dir = '%s/run' % (vol_dir)
        if os.path.exists(run_dir):
            for x in os.listdir(run_dir):
                if not x.endswith('pid'):
                    continue
                try:
                    pid_file = '%s/run/%s' % (vol_dir,x)
                    f = open(pid_file)
                    lines = f.readlines()
                    f.close()
                    pid = lines[0].strip()
                    
                    cmd = 'kill -9 %s' % (pid)
                    syslog.syslog(syslog.LOG_ERR,'kill brick process: '+ cmd)
                    os.system(cmd)
                except:
                    syslog.syslog(syslog.LOG_ERR,'kill brick process: '+str(traceback.format_exc()))
                    continue
    
        cmd = 'rm -rf %s' % (vol_dir)
        syslog.syslog(syslog.LOG_ERR,'do clear vol: '+cmd)
        os.system(cmd)
    return True,''


def delete_brick_attr(bricks,delete=False):
    
    
    for brick in bricks:
        try:
            target_ip = brick['target_ip']
            storage_path = brick['storage_path']
            
            (flag, psh) = vmd_utils.get_rpcConnection(target_ip)
            if not flag:
                continue
            psh.do_clear_path_attr(storage_path,delete)
        except:
            syslog.syslog(syslog.LOG_ERR,'gluster clear path attr failed: '+str(target_ip)+' '+storage_path)
            syslog.syslog(syslog.LOG_ERR,'gluster clear path attr failed: '+str(traceback.format_exc()))
    
    return True,''

def do_clear_path_attr(storage_path,delete=False):
    
    cmd = 'setfattr -x trusted.glusterfs.volume-id %s/glusterfs_storage;setfattr -x trusted.gfid %s/glusterfs_storage' % (storage_path,storage_path)
    syslog.syslog(syslog.LOG_ERR,'clear path attr '+cmd)
    os.system(cmd)
    
    
    cmd = 'rm -rf %s/glusterfs_storage;mkdir %s/glusterfs_storage' % (storage_path,storage_path)
    syslog.syslog(syslog.LOG_ERR,'clear path attr '+cmd)
    os.system(cmd)
    
    '''
    if delete:
        cmd = 'ls -a %s/glusterfs_storage | xargs rm -rf' % (storage_path)
        os.system(cmd)
    '''
     
    return True,''

def clear_used_bricks(volume_infos,new_bricks,host_ip):
    
    deleted_bricks = []
    
    for new_brick in new_bricks:
        for volume_desc in volume_infos:
            used_bricks = volume_infos[volume_desc]['bricks']
            for used_brick in used_bricks:
                if new_brick['storage_path'] in deleted_bricks:
                    continue
                if new_brick['storage_path'] == used_brick['storage_path']:
                    removing_bricks = operation.gluster.volume_db.get_removing_bricks(volume_desc,used_bricks)
                    #如果removing_bricks的信息无法在数据库中获取到，则，应该删除整个的volume？
                    if len(removing_bricks)!=0 and len(removing_bricks) < len(used_bricks):
                        #为bicks其中的一部分，需要缩减
                        cmd = operation.gluster.volume_cmd.remove_brick_cmd(volume_desc, removing_bricks)
                    
                        if 0 != os.system(cmd):
                            return False,host_ip+'移除分布式存储块单元失败'
                    else:
                        #包含了所有的bricks，需要删除整个volume
                        if not delete_volume_by_cmd(volume_desc, 10):
                            #删除成功后是否恢复，间隔为10秒
                            flag,msg = gluster_delete_vols([volume_desc])
                            if not flag:
                                return False,host_ip+msg
    return True,''

def gluster_reset_rbstate(volume_descs):
    
    #是否只重启一个机器的服务就可以？？
    _,states = operation.gluster.peer_cmd.get_peer_states()
    _,states = operation.gluster.peer_db.convert_to_target_states(states)
    
    for target_ip,conn_state,peer_state in states:
        if conn_state == 'Connected' and peer_state == 'Peer in Cluster':
            try:
                (flag, psh) = vmd_utils.get_rpcConnection(target_ip)
                if not flag:
                    continue
                psh.do_stop_gluster()
            except:
                syslog.syslog(syslog.LOG_ERR,'glsuter rpc '+str(traceback.format_exc()))
                return False,'rpc stop %s gluster service failed' % (target_ip)
           
     
    operation.gluster.volume_sys.do_stop_gluster()
    try:
        do_clear_rbstate(volume_descs)
    except:
        syslog.syslog(syslog.LOG_ERR,'cleare rbstate: '+str(traceback.format_exc()))
    operation.gluster.volume_sys.do_restart_gluster()
    
    for target_ip,conn_state,peer_state in states:
        if conn_state == 'Connected' and peer_state == 'Peer in Cluster':
            try:
                (flag, psh) = vmd_utils.get_rpcConnection(target_ip)
                if not flag:
                    continue
                psh.do_clear_rbstate(volume_descs)
            except:
                syslog.syslog(syslog.LOG_ERR,'glsuter rpc '+str(traceback.format_exc()))
                
            finally:
                psh.do_restart_gluster()
                
    return True,''

