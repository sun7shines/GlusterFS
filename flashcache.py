# -*- coding: utf-8 -*-

import syslog
import traceback
import os
import commands
import time
import sys

import operation.vm.vm_running_script
import operation.vstorage.storage_cmd_op
import operation.vstorage.storage_db_op
import support.cmd_exe
import diskinfo
import operation.vhost.hostinfo
import dbmodule.db_module_interface
import support.uuid_op
import operation.vstorage.storage_interface
import dbmodule.db_op

def creat_cache_dev(cache_mod,cache_size,cache_name,cache_disk,device):

    #cmd = 'flashcache_create -p '+cache_mod+' -s 200g -b 4k '+ cache_name+' '+cache_disk+' '+device
    cmd = 'flashcache_create -p '+cache_mod+' -s '+cache_size+' -b 4k '+ cache_name+' '+cache_disk+' '+device
    print cmd
    result = support.cmd_exe.cmd_exe(cmd)
    return result[0]    

def cache_name_exists(cache_name):

    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name='host_caches')
    module_object.message['field1'] = {'cachename':cache_name}
    flag,msg = module_object.select()
    if not flag:
        syslog.syslog(syslog.LOG_ERR, "SELECT DB FAILED:")
        syslog.syslog(syslog.LOG_ERR, "FILE:%s,FUN:%s,LINE:%s,ERROR_MSG:%s" % (sys._getframe().f_code.co_filename,sys._getframe().f_code.co_name,sys._getframe().f_lineno,msg))
        syslog.syslog(syslog.LOG_ERR,'select name_exists cache failed: '+str(traceback.format_exc()))
        return True 
    return False

def cache_disk_exists(cache_disk):

    module_object = dbmodule.db_module_interface.DbMessageObject(db_name='host_caches')
    module_object.message['field1'] = {'harddisk_name':cache_disk}
    flag,msg = module_object.select()
    if not flag:
        return True     
    return False

def cache_storage_exists(storage_uuid):

    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name='host_caches')
    module_object.message['field1'] = {'storageuuid':storage_uuid}
    flag,msg = module_object.select()
    if not flag:
        syslog.syslog(syslog.LOG_ERR, "SELECT DB FAILED:")
        syslog.syslog(syslog.LOG_ERR, "FILE:%s,FUN:%s,LINE:%s,ERROR_MSG:%s" % (sys._getframe().f_code.co_filename,sys._getframe().f_code.co_name,sys._getframe().f_lineno,msg))
        syslog.syslog(syslog.LOG_ERR,'select storage exists cache failed: '+str(traceback.format_exc()))
        return False    
    return True

def add_cache_info(cache_mod,cache_size,cache_name,cache_disk,storage_uuid):
    
    hostuuid = support.uuid_op.get_vs_uuid()[1]
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name = "host_caches")
    field1 = {"host":{"db_name":"hosts","field":{"uuid":hostuuid}},
              "harddisk_name":cache_disk,"cachename":cache_name,"cachemod":cache_mod,
              "cachesize":cache_size,"storageuuid":storage_uuid}
    module_object.message["field1"] = field1
    flag,msg = module_object.insert_f()
    if not flag:
        syslog.syslog(syslog.LOG_ERR, "INSERT DB FAILED:")
        syslog.syslog(syslog.LOG_ERR, "FILE:%s,FUN:%s,LINE:%s,ERROR_MSG:%s" % (sys._getframe().f_code.co_filename,sys._getframe().f_code.co_name,sys._getframe().f_lineno,msg))
        syslog.syslog(syslog.LOG_ERR,'add cache dev info failed: '+str(traceback.format_exc()))
        return False
    is_vcuuid,vcuuid,vc_ip = support.uuid_op.get_vc_uuid()
    if is_vcuuid and vcuuid!="127.0.0.1":
        module_object.ip_d = vc_ip
        flag,msg = module_object.insert_f()
        if not flag:
            syslog.syslog(syslog.LOG_ERR, "INSERT DB FAILED:")
            syslog.syslog(syslog.LOG_ERR, "FILE:%s,FUN:%s,LINE:%s,ERROR_MSG:%s" % (sys._getframe().f_code.co_filename,sys._getframe().f_code.co_name,sys._getframe().f_lineno,msg))
            syslog.syslog(syslog.LOG_ERR,'add cache dev info failed: '+str(traceback.format_exc()))
            return False
    return True



def clear_flashcache(cache_name,cache_disk):

    cmd = 'dmsetup remove '+cache_name
    os.system(cmd)

    cmd = 'flashcache_destroy '+cache_disk + ' -f'
    os.system(cmd)

def get_memdef_size():

    _,meminfo = operation.vhost.hostinfo.get_mem_fileinfo()
    size = int(meminfo['memtotal'])/1024/4*150/600
    return size

def get_disk_size(cache_disk):

    harddisklst=diskinfo.get_diskinfo()
    for x in harddisklst:
        if x['devicename'] == cache_disk:
            size = int(float(x['size']))/1024*1000/1024*1000/1024
            return True,size
    return False,0

def open_flashcache_op(event):

    param = event.param
    cache_disk = param.get('harddisk_name')
    cache_mod = param.get('cache_mod')
    #cache_size = param.get('cache_size')
    cache_name = param.get('cache_name')
    storage_uuid = param.get('storage_uuid')

    storage = operation.vstorage.storage_db_op.get_storage(storage_uuid)
    if not storage:
        return False,'get storage by uuid failed'

    flag,cache_size = get_disk_size(cache_disk)
    if not flag:
        return False,'no such a disk: '+cache_disk
    mcache_size = get_memdef_size()

    if mcache_size > cache_size:
        cache_size = cache_size
    else:
        cache_size = mcache_size
    
    storage_size = int(storage['total_size'])/1024
    if storage_size < cache_size:
        cache_size = storage_size
        
    cache_size = str(cache_size)+'g'
    
    vm_infos = operation.vm.vm_running_script.get_all_running_vm_by_vmprocess()
    if len(vm_infos) != 0:
        return False,'running vm exists'

    if storage['mount_type'].find('cluster') != -1:
        cache_mod = 'thru'
        
    if not cache_name_exists(cache_name):
        return False,'cache name alread in use'

    if not cache_disk_exists(cache_disk):
        return False,'the disk alread in use'

    if cache_storage_exists(storage['uuid']):
        return False,'storage alread exists cache dev'

    if not operation.vstorage.storage_cmd_op.umount_path(storage['mount_path']):
        return False,'umount storage path failed'

    clear_flashcache(cache_name,cache_disk)

    if not operation.vstorage.storage_cmd_op.umount_path(storage['mount_path']):
        return False,'umount storage path failed'

    #需要测试创建成功和创建失败两种情况
    if not creat_cache_dev(cache_mod,cache_size,cache_name,cache_disk,storage['par']['device']):
        return False,'create flashcache dev failed'

    if not add_cache_info(cache_mod,cache_size,cache_name,cache_disk,storage['uuid']):
        operation.vstorage.storage_cmd_op.umount_path(storage['mount_path'])
        clear_flashcache(cache_name,cache_disk)
        return False,'add cache info failed'

    operation.vstorage.storage_interface.auto_recoverallstorage_option()
    
    time.sleep(5)
    
    #增加错误处理
    return True,'' 

def get_cacheobj(cache_name,cache_disk):
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name='host_caches')
    module_object.message['field1'] = {'cachename':cache_name,'harddisk_name':cache_disk}
    flag,msg = module_object.select()
    
    if not flag:
        syslog.syslog(syslog.LOG_ERR, "SELECT DB FAILED:")
        syslog.syslog(syslog.LOG_ERR, "FILE:%s,FUN:%s,LINE:%s,ERROR_MSG:%s" % (sys._getframe().f_code.co_filename,sys._getframe().f_code.co_name,sys._getframe().f_lineno,msg))
        syslog.syslog(syslog.LOG_ERR,'select storage exists cache failed: '+str(traceback.format_exc()))
        return None
    
    return msg

def delete_cache_info(cache_name,cache_disk):
    
    hostuuid = support.uuid_op.get_vs_uuid()[1]
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name = "host_caches")
    field1 = {"host":{"db_name":"hosts","field":{"uuid":hostuuid}},
              "cachename":cache_name,"harddisk_name":cache_disk}
    module_object.message["field1"] = field1
    flag,msg = module_object.delete_f()
    if not flag:
        syslog.syslog(syslog.LOG_ERR, "DELETE DB FAILED:")
        syslog.syslog(syslog.LOG_ERR, "FILE:%s,FUN:%s,LINE:%s,ERROR_MSG:%s" % (sys._getframe().f_code.co_filename,sys._getframe().f_code.co_name,sys._getframe().f_lineno,msg))
        syslog.syslog(syslog.LOG_ERR,'delete cache info failed')
        return False
    dbmodule.db_op.db_delete('host_harddisk', {'harddisk_type':'flashcache'})
    
    is_vcuuid,vcuuid,vc_ip = support.uuid_op.get_vc_uuid()
    if is_vcuuid and vcuuid!="127.0.0.1":
        module_object.ip_d = vc_ip
        flag,msg = module_object.delete_f()
        
        if not flag:
            syslog.syslog(syslog.LOG_ERR, "INSERT DB FAILED:")
            syslog.syslog(syslog.LOG_ERR, "FILE:%s,FUN:%s,LINE:%s,ERROR_MSG:%s" % (sys._getframe().f_code.co_filename,sys._getframe().f_code.co_name,sys._getframe().f_lineno,msg))
            syslog.syslog(syslog.LOG_ERR,'delete cache info failed')
            return False
        dbmodule.db_op.db_delete_vc('host_harddisk', {'harddisk_type':'flashcache'},vc_ip)
    return True


def close_flashcache_op(event):

    param = event.param
    cache_name = param.get('cache_name')
    cache_disk = param.get('harddisk_name')

    caches = get_cacheobj(cache_name,cache_disk)
    if not caches:
        return False,'flashcache device does not exists'

    cacheobj = caches[0]

    storage = operation.vstorage.storage_db_op.get_storage(cacheobj['storageuuid'])
    if not storage:
        return False,'get storage by uuid failed'

    vm_infos = operation.vm.vm_running_script.get_all_running_vm_by_vmprocess()
    if len(vm_infos) != 0:
        return False,'running vm exists'

    if not operation.vstorage.storage_cmd_op.umount_path(storage['mount_path']):
        return False,'umount storage path failed'

    clear_flashcache(cache_name,cache_disk)
    
    if storage['par'].get('filesystem') == 'xfs':
        local_device = storage['par'].get('device')
        cmd = 'xfs_repair -L %s' % (local_device)
        os.system(cmd)
    
    cmd = 'dd if=/dev/zero of=%s bs=512 count=1' % (cache_disk)
    os.system(cmd)
    cmd = 'parted -s %s mklabel gpt' % (cache_disk)
    os.system(cmd)
    
    if not delete_cache_info(cache_name,cache_disk):
        return False,'delete cache info failed'
    
    operation.vstorage.storage_interface.auto_recoverallstorage_option()
    
    time.sleep(5)
    
    return True,''

def load_flashcache():
    
    
    module_object = dbmodule.db_module_interface.DbMessageObject(db_name='host_caches')
    module_object.message["field1"] = {}
    module_object.message["foreignkey_list"] = []
    flag,msg = module_object.select_all()
    
    if not flag or not msg:
        return  
    cache_name = msg[0]['cachename']
    cache_disk = msg[0]['harddisk_name']

    fn = '/dev/mapper'+'/'+cache_name
    if os.path.exists(fn):
        return 

    caches = get_cacheobj(cache_name,cache_disk)
    if not caches:
        return False,'flashcache device does not exists'

    cacheobj = caches[0]

    storage = operation.vstorage.storage_db_op.get_storage(cacheobj['storageuuid'])
    if not storage:
        return False,'get storage by uuid failed'

    try:
        operation.vstorage.storage_cmd_op.umount_path(storage['mount_path'])
    except:
        pass
    
    cmd = 'flashcache_load '+cache_disk
    flag,output = commands.getstatusoutput(cmd)
    if flag:
        syslog.syslog(syslog.LOG_ERR,'flashcache_load: '+str(output))
    time.sleep(5)
    return True,''
