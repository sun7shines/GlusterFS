# -*- coding: utf-8 -*-
""""""
import syslog
import traceback

import global_params
import support.lock_option

import operation.cancel.singlevm_completeclone_cancel
import operation.cancel.create_complete_backup_cancel
import operation.cancel.create_diff_backup_cancel
import operation.cancel.delete_template_cancle

def cancel_event(event_object):
    
    evuuid = event_object.param['cancel_evuuid']
    #action = event.param['cancel_action']
    for event in global_params.event_working_queue:
        if evuuid == event.uuid:
            event.cancel_stat = 'yes' 
            break
            
    #取消事件会不发送给任务进程，否则会更改任务的进度
    #直接从事件队列中删除对象
    support.lock_option.lock_acquire("optevent_lock")
    try:
        try:
            if event_object in global_params.event_working_queue:
                global_params.event_working_queue.remove(event_object)
        except:
            print "delete_finished_event_object error"
        syslog.syslog("Event remove:%s:%s:%s" % (str(event_object.uuid), str(event_object.mission_id),str(event_object.action)))
    finally:
        support.lock_option.lock_release("optevent_lock")
        
    return (True, "suc")

optevent_cancel = {

41601:operation.cancel.create_complete_backup_cancel.rollback,
41602:operation.cancel.create_diff_backup_cancel.rollback,
#41701:'',
#41702:'',
#50301:'',
#50302:'',
50401:operation.cancel.singlevm_completeclone_cancel.rollback,
51101:operation.cancel.delete_template_cancle.rollback,
}

def rollback(event,pos,tgfiles):
    #因为删除快照等接口中，会用到event.cacel_stat，避免对删除快照造成影响。
    saved = event.cancel_stat
    event.cancel_stat = 'no' 
    try:    
        optevent_cancel[event.action](event,pos,tgfiles)
    except:
        syslog.syslog(syslog.LOG_ERR, "optevent_cancel error:"+str(event.action)+":"+str(traceback.format_exc()))
    event.cancel_stat = saved
    return True,''

