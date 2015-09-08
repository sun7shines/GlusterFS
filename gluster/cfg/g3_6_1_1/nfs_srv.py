# -*- coding: utf-8 -*-

from glfs import std_dir,file_lines,file_update

def edit(old_ip,old_dir,new_ip,new_dir,vol,seq):

    fn = '/var/lib/glusterd/nfs/nfs-server.vol'
    lines = file_lines(fn)

    num1 = seq*10+(7-1)
    num2 = seq*10+(8-1)

    if lines[num1].find(std_dir(old_dir)) == -1 or lines[num2].find(old_ip) == -1:
        return False,'brick seq error in nfs_srv'
    lines[num1] = lines[num1].replace(std_dir(old_dir),std_dir(new_dir))
    lines[num2] = lines[num2].replace(old_ip,new_ip)
    file_update(fn,lines)

    return True,''



