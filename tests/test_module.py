
import sys
import os
import thingamajig.aws.s3


def test_module_load():

    import thingamajig.aws.s3
    
    print(f'Module loaded')

def test_s3_list():
    from thingamajig.aws.s3 import get_client, list

    client = get_client()

    result = list(client, 'pw-media-main', '')
    
    print(f'result: {len(result)} objects')

def test_class_s3_list():
    from thingamajig.aws.s3 import s3
    
    result = s3.list(client, 'pw-media-main', '')
    
    print(f'result: {len(result)} objects')


