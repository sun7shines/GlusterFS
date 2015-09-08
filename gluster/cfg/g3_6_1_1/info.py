# -*- coding: utf-8 -*-

from glfs import std_dir,file_lines,file_update

def edit(old_ip,old_dir,new_ip,new_dir,vol,seq):

    fn = '/var/lib/glusterd/vols/%s/info' % (vol)
    lines = file_lines(fn)

    num1 = 18+(seq+1-1)
   
    dstr = '-'.join(std_dir(old_dir).split('/')[1:])
    idstr = '%s:-%s' % (old_ip,dstr)
 
    if lines[num1].find(idstr) == -1:
        return False,'brick seq error in info'

    dstr1 = '-'.join(std_dir(new_dir).split('/')[1:])
    idstr1 = '%s:-%s' % (new_ip,dstr1)

    lines[num1] = lines[num1].replace(idstr,idstr1)

    num2 = (9-1)
    ver = lines[num2].strip().split('=')[1]
    lines[num2] = lines[num2].replace(ver,str(int(ver)+2))

    file_update(fn,lines)

    return True,''


