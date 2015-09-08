# -*- coding: utf-8 -*-

from operation.gluster.cfg.g3_6_1_1.glfs import get_brick_seq
import operation.gluster.cfg.g3_6_1_1.fuse
import operation.gluster.cfg.g3_6_1_1.trusted_fuse
import operation.gluster.cfg.g3_6_1_1.vol_host_dir
import operation.gluster.cfg.g3_6_1_1.brk_host_dir
import operation.gluster.cfg.g3_6_1_1.shd_srv
import operation.gluster.cfg.g3_6_1_1.nfs_srv
import operation.gluster.cfg.g3_6_1_1.info

def update(old_ip,old_dir,new_ip,new_dir,vol):

    seq = get_brick_seq(old_ip,old_dir,vol)
    if -1 == seq:
        return False,'brick not exist in info'

    flag,msg = operation.gluster.cfg.g3_6_1_1.fuse.edit(old_ip,old_dir,new_ip,new_dir,vol,seq)
    if not flag:
        return False,msg
 
    flag,msg = operation.gluster.cfg.g3_6_1_1.trusted_fuse.edit(old_ip,old_dir,new_ip,new_dir,vol,seq)
    if not flag:
        return False,msg

    flag,msg = operation.gluster.cfg.g3_6_1_1.vol_host_dir.edit(old_ip,old_dir,new_ip,new_dir,vol,seq)
    if not flag:
        return False,msg

    flag,msg = operation.gluster.cfg.g3_6_1_1.brk_host_dir.edit(old_ip,old_dir,new_ip,new_dir,vol,seq)
    if not flag:
        return False,msg
    
    operation.gluster.cfg.g3_6_1_1.shd_srv.edit(old_ip,old_dir,new_ip,new_dir,vol,seq)
    operation.gluster.cfg.g3_6_1_1.nfs_srv.edit(old_ip,old_dir,new_ip,new_dir,vol,seq)

    flag,msg = operation.gluster.cfg.g3_6_1_1.info.edit(old_ip,old_dir,new_ip,new_dir,vol,seq)
    if not flag:
        return False,msg
 
    return True,''

if __name__ == '__main__':

    update('192.168.36.15','/mnt/glfs/dir2','192.168.36.15','/mnt/glfs/dir5','g1')



