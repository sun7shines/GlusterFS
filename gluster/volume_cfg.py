# -*- coding: utf-8 -*-

import operation.gluster.volume_ifo

import os
import syslog
import traceback
import operation.gluster.cfg.g3_6_1_1.update_cfg

def save_cfgs(volume_desc,brick_info,new_brick):
    
    old_ip = brick_info['peer_ip']
    old_dir = brick_info['storage_path']
    
    save_dir = '/var/lib/glusterd/vols/%s/save_cfgs' % (volume_desc)
    cmd = 'rm -rf %s ;mkdir %s; mkdir %s/bricks' % (save_dir,save_dir,save_dir) 
    os.system(cmd)
    
    vol_ip_dir = '/var/lib/glusterd/vols/%s/%s.%s.%s.vol' % (volume_desc,volume_desc,old_ip,('-').join(old_dir.split('/')[1:]))
    vol_fuse = '/var/lib/glusterd/vols/%s/%s-fuse.vol' % (volume_desc,volume_desc)
    vol_info = '/var/lib/glusterd/vols/%s/info' % (volume_desc)
    trusted_vol_fuse = '/var/lib/glusterd/vols/%s/trusted-%s-fuse.vol' % (volume_desc,volume_desc)
    brick_file = '/var/lib/glusterd/vols/%s/bricks/%s:%s.vol' % (volume_desc,old_ip,('-').join(old_dir.split('/')))
    
    vol_ip_dir_bak = '%s/%s.%s.%s.vol' % (save_dir,volume_desc,old_ip,('-').join(old_dir.split('/')[1:]))
    vol_fuse_bak = '%s/%s-fuse.vol' % (save_dir,volume_desc)
    vol_info_bak = '%s/info' % (save_dir)
    trusted_vol_fuse_bak = '%s/trusted-%s-fuse.vol' % (save_dir,volume_desc)
    brick_file_bak = '%s/bricks/%s:%s.vol' % (save_dir,old_ip,('-').join(old_dir.split('/')))
    
    cmd = 'cp %s %s ' % (vol_ip_dir,vol_ip_dir_bak)
    os.system(cmd)
    
    cmd = 'cp %s %s ' % (vol_fuse,vol_fuse_bak)
    os.system(cmd)
    
    cmd = 'cp %s %s ' % (vol_info,vol_info_bak)
    os.system(cmd)
    
    cmd = 'cp %s %s ' % (trusted_vol_fuse,trusted_vol_fuse_bak)
    os.system(cmd)
    
    cmd = 'cp %s %s ' % (brick_file,brick_file_bak)
    os.system(cmd)
    
    return True,''


def clear_bak_cfgs(volume_desc):
    
    save_dir = '/var/lib/glusterd/vols/%s/save_cfgs' % (volume_desc)
    cmd = 'rm -rf %s' % (save_dir) 
    os.system(cmd)
    return True,''

def restore_cfgs(volume_desc,brick_info,new_brick):

    old_ip = brick_info['peer_ip']
    old_dir = brick_info['storage_path']
    
    save_dir = '/var/lib/glusterd/vols/%s/save_cfgs' % (volume_desc)
    
    vol_ip_dir = '/var/lib/glusterd/vols/%s/%s.%s.%s.vol' % (volume_desc,volume_desc,old_ip,('-').join(old_dir.split('/')[1:]))
    vol_fuse = '/var/lib/glusterd/vols/%s/%s-fuse.vol' % (volume_desc,volume_desc)
    vol_info = '/var/lib/glusterd/vols/%s/info' % (volume_desc)
    trusted_vol_fuse = '/var/lib/glusterd/vols/%s/trusted-%s-fuse.vol' % (volume_desc,volume_desc)
    brick_file = '/var/lib/glusterd/vols/%s/bricks/%s:%s.vol' % (volume_desc,old_ip,('-').join(old_dir.split('/')))
    
    vol_ip_dir_bak = '%s/%s.%s.%s.vol' % (save_dir,volume_desc,old_ip,('-').join(old_dir.split('/')[1:]))
    vol_fuse_bak = '%s/%s-fuse.vol' % (save_dir,volume_desc)
    vol_info_bak = '%s/info' % (save_dir)
    trusted_vol_fuse_bak = '%s/trusted-%s-fuse.vol' % (save_dir,volume_desc)
    brick_file_bak = '%s/bricks/%s:%s.vol' % (save_dir,old_ip,('-').join(old_dir.split('/')))
    
    cmd = 'rm -f %s %s %s %s %s'  % (vol_ip_dir,vol_fuse,vol_info,trusted_vol_fuse,brick_file)
    os.system(cmd)
    
    cmd = 'cp %s %s ' % (vol_ip_dir_bak,vol_ip_dir)
    os.system(cmd)
    
    cmd = 'cp %s %s ' % (vol_fuse_bak,vol_fuse)
    os.system(cmd)
    
    cmd = 'cp %s %s ' % (vol_info_bak,vol_info)
    os.system(cmd)
    
    cmd = 'cp %s %s ' % (trusted_vol_fuse_bak,trusted_vol_fuse)
    os.system(cmd)
    
    cmd = 'cp %s %s ' % (brick_file_bak,brick_file)
    os.system(cmd)
        
    return True,''

def modify_cfgs(volume_desc,brick_info,new_brick):
        
    old_ip = brick_info['peer_ip']
    old_dir = brick_info['storage_path']
    new_ip = new_brick['peer_ip']
    new_dir = new_brick['storage_path']
    
    try:
        operation.gluster.cfg.g3_6_1_1.update_cfg.update(old_ip, old_dir, new_ip, new_dir, volume_desc)
    except:
        syslog.syslog(syslog.LOG_ERR,'modify_cfgs '+traceback.format_exc())
      
    return True,''

def rewrite_rb_dst_brick(volume_desc,new_brick):
    
    new_dir = '%s/glusterfs_storage' % (new_brick['storage_path'])
    flag,vol_info = operation.gluster.volume_ifo.get_vol_user_info(volume_desc)
    if not flag:
        return False,''
    vol_id = vol_info['volume_id']
    user_name = vol_info['username']
    password = vol_info['password']

    strs = '''volume src-posix
 type storage/posix
 option directory %s
 option volume-id %s
end-volume
volume %s
 type features/locks
 subvolumes src-posix
end-volume
volume src-server
 type protocol/server
 option auth.login.%s.allow %s
 option auth.login.%s.password %s
 option auth.addr.%s.allow *
 option transport-type tcp
 subvolumes %s
end-volume''' % (new_dir,vol_id,new_dir,new_dir,user_name,user_name,password,new_dir,new_dir)

    rb_dst = '/var/lib/glusterd/vols/%s/rb_dst_brick.vol' % (volume_desc)
    f = open(rb_dst,'w')
    f.writelines([strs])
    f.close()
    return True,''

