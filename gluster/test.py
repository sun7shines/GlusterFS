

import os

if __name__ == "__main__":

    cmd = 'setfattr -x trusted.glusterfs.volume-id /mnt/e7ee79c1a00ad91861ed102b424ec93f/glusterfs_storage;setfattr -x trusted.gfid /mnt/e7ee79c1a00ad91861ed102b424ec93f/glusterfs_storage'
    os.system(cmd)

    cmd = 'setfattr -x trusted.glusterfs.volume-id /mnt/0fbabba996017e4f81e89dde6952c0ed/glusterfs_storage;setfattr -x trusted.gfid /mnt/0fbabba996017e4f81e89dde6952c0ed/glusterfs_storage'
    os.system(cmd)

    cmd = "(echo 'y' ;) | gluster volume create s  replica 2 transport tcp 192.168.36.82:/mnt/e7ee79c1a00ad91861ed102b424ec93f/glusterfs_storage 192.168.36.82:/mnt/0fbabba996017e4f81e89dde6952c0ed/glusterfs_storage"
    os.system(cmd)

