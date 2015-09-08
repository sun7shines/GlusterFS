# -*- coding: utf-8 -*-


from operation.vobjects.vhd import VHDIMG
from operation.vobjects.vhdphy import VHDHD,VHDPT
from operation.vobjects.vhddev import VHDLV,VHDFORMAT_FULL_HD,VHDFORMAT_FREE_HD,VHDVG_FREE_SPACE


def getvhdobj(vhd):
    
    if not vhd.get('use_pattern'):
        vhdclass = VHDIMG
        
    
    elif vhd.get('use_pattern') and vhd.get('use_pattern') in ['hd']:
        vhdclass = VHDHD
    
    elif vhd.get('use_pattern') and vhd.get('use_pattern') in ['pt']:
        vhdclass = VHDPT
    
    elif vhd.get('use_pattern') and vhd.get('use_pattern') in ['lv']:
        vhdclass = VHDLV
    
    elif vhd.get('use_pattern') and vhd.get('use_pattern') in ['format_full_hd']:
        vhdclass = VHDFORMAT_FULL_HD
    
    elif vhd.get('use_pattern') and vhd.get('use_pattern') in ['format_free_hd']:
        vhdclass = VHDFORMAT_FREE_HD 
    
    elif vhd.get('use_pattern') and vhd.get('use_pattern') in ['vg_free_space']:
        vhdclass = VHDVG_FREE_SPACE
    
    else:
        vhdclass = None
        
    if vhdclass:
        return vhdclass(vhd=vhd)
    
    return None

def hd_create_objects(allpara):

    for vhd in allpara["hd"]:
        vmuuid = allpara['vmuuid']
        storage_path = allpara['storage_path']
        vhdobj = getvhdobj(vhd)
        vhd['vhdobj'] = vhdobj
        vhdobj.storage_path = storage_path
        vhdobj.vmuuid =vmuuid
            
    return allpara
