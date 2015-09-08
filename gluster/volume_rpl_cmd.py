# -*- coding: utf-8 -*-

import operation.gluster.volume_cmd
import os

def replace_brick_commit(volume_desc,brick_info,new_brick):
    
    cmd = operation.gluster.volume_cmd.replace_brick_commit_cmd_force(volume_desc, brick_info, new_brick)
    if 0!= os.system(cmd):
        return False,'gluster replace brick failed,check the messages log'
    return True,''

