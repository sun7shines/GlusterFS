
import sys
import traceback
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append("/usr/vmd")
import syslog
import time
import os
import os.path
import global_params

global_params.django = True
sys.path.append("/usr/")
import global_params
global_params.django = True
sys.path.append("/usr/")
from django.conf import settings
sys.path.append("/usr/django_object")
try:
    import django_object.settings
    settings.configure(default_settings=django_object.settings)
except:
    pass

import operation.vm.conf_xml

def get_running_imgs(vmuuid,GSTORAGES):

    for storage_path in GSTORAGES:
        vm_op = operation.vm.conf_xml.vm_operation(vmuuid, storage_path)
        (flag, allpara) = vm_op.get_vm_all_para()
        if (not flag) or (not allpara):
            continue
        imgs = []
        for vhd in allpara['hd']:
            img = vhd['runningimg']
            if os.path.exists(storage_path+'/'+img):
                imgs.append('/'+img)
                continue
            elif os.path.exists(storage_path+'/'+vmuuid+'/'+img):
                imgs.append('/'+vmuuid+'/'+img)
                continue
        return imgs

    return []

def fwprint(lstr):
    f = open('/root/glfs.log','a')
    print >>f,lstr+'\n'
    f.close()

if __name__ == '__main__':

    imgs = get_running_imgs('gGKclKsK-L31xjp-x6QO',['/mnt/23c1fb0653ee3e8a8dd8ad76ac36f342']) 
    print imgs
    imgs = get_running_imgs('mQoLwhyx-uVjvaE-m3jO',['/mnt/23c1fb0653ee3e8a8dd8ad76ac36f342'])
    print imgs

