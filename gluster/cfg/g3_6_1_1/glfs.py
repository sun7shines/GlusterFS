# -*- coding: utf-8 -*-

def std_dir(dirx):

    ll = dirx.split('/')
    ll1 = []
    for x in ll:
        if x == '/':
            continue
        if not x:
            continue
        ll1.append(x)
    return '/'+'/'.join(ll1)

def file_lines(fn):
    f = open(fn)
    lines = f.readlines()
    f.close()
    newlines = []
    for x in lines:
        if not x.strip():
            continue
        newlines.append(x)
    return newlines

def file_update(fn,lines):

    f = open(fn,'w')
    f.writelines(lines)
    f.close()

def get_brick_seq(ip,dirx,vol):

    dstr = '-'.join(std_dir(dirx).split('/')[1:])
    idstr = '%s:-%s' % (ip,dstr)

    fn = "/var/lib/glusterd/vols/%s/info" % (vol)
    lines = file_lines(fn)
    for x in lines:
        if x.startswith('brick') and x.find(idstr) != -1:
            seq = x.split('=')[0].split('-')[1]
            return int(seq)

    return -1

