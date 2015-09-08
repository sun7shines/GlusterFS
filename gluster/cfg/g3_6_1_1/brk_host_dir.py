# -*- coding: utf-8 -*-

from glfs import std_dir
import os
def edit(old_ip,old_dir,new_ip,new_dir,vol,seq):

    dstr = '-'.join(std_dir(old_dir).split('/')[1:])
    fn = '/var/lib/glusterd/vols/%s/bricks/%s:-%s' % (vol,old_ip,dstr)
    lstr = file(fn).read()
    lstr = lstr.replace(std_dir(old_dir),std_dir(new_dir))
    lstr = lstr.replace(old_ip,new_ip)

    dstr1 = '-'.join(std_dir(new_dir).split('/')[1:])
    fn1 = '/var/lib/glusterd/vols/%s/bricks/%s:-%s' % (vol,new_ip,dstr1)
    file(fn1,'w').write(lstr)
    
    os.system('rm -f %s' % fn )
    return True,''



